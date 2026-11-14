"""
compute_gender.py
------------------
One-time batch precomputation of author-gender inference for the FAIR
longitudinal study, decoupled from notebooks/06_gender_analysis.ipynb.

The notebook used to call names_dataset + gender_guesser live, inside the
Jupyter kernel, once per distinct `author.given_name` value (~2.1M of
them). That pass, combined with the ~2-4GB names_dataset package held in
memory alongside the notebook's own DataFrames, was the direct cause of
past OOM kernel deaths. This script performs the identical inference
offline and writes one row per distinct given_name to a new
`given_name_gender` table, so the notebook only needs to LEFT JOIN against
it -- no model loading, no per-row inference, inside Jupyter at all.

Inference logic (threshold, strategy order, fallback table) mirrors what
the notebook used to run live:
  Strategy 1: names_dataset (global probabilistic first-name DB)
  Strategy 2: gender_guesser fallback
  Bare initials (one letter, or two letters with no lower-case component,
  e.g. "A", "A.", "AB", "A.B.") -> unknown
Only the FIRST whitespace-separated token of given_name is used (e.g.
"Abdur Rehman" -> keyed on "Abdur"), matching the notebook's behaviour --
but each row is still stored under its own full given_name string so a
plain SQL join on `author.given_name` reproduces the same per-row
classification. An in-process cache on that token means the expensive
model calls still only run once per unique first name, not once per
distinct full given_name string.

IMPORTANT METHODOLOGICAL CAVEAT (see review/chatgpt_review_gender.md):
this is NAME-BASED GENDER INFERENCE, not author gender identity. It
estimates the gender association of a name in global reference
populations; it cannot detect non-binary authors, does not use country/
affiliation context, and has not been validated against any ground
truth for this corpus. Treat its output as an exploratory secondary
signal only -- see the paper's limitations section before citing any
numbers derived from it. `source` records which strategy produced each
row's result, so downstream analysis can report how much of the corpus
relies on the (less reliable) gender_guesser fallback.

Resume-safe: given_name values already present in given_name_gender are
skipped on restart.

Usage
-----
  python pipeline/compute_gender.py --db data/processed/pmc_fair.db
  python pipeline/compute_gender.py --db pmc_fair.db --reset
"""

import argparse
import logging
import sqlite3
import sys
import time
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path

LOG_FMT  = "%(asctime)s  %(levelname)-7s  %(message)s"
LOG_DATE = "%H:%M:%S"


def setup_logging(log_path: Path) -> logging.Logger:
    logger = logging.getLogger("compute_gender")
    logger.setLevel(logging.DEBUG)
    logger.handlers.clear()
    fmt = logging.Formatter(LOG_FMT, datefmt=LOG_DATE)
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO); ch.setFormatter(fmt)
    logger.addHandler(ch)
    fh = logging.FileHandler(log_path, encoding="utf-8", mode="a")
    fh.setLevel(logging.DEBUG); fh.setFormatter(fmt)
    logger.addHandler(fh)
    return logger


SCHEMA = """
CREATE TABLE IF NOT EXISTS given_name_gender (
    given_name       TEXT PRIMARY KEY,
    gender           TEXT NOT NULL,   -- female | male | ambiguous | unknown
    inference_score  REAL NOT NULL,   -- name-distribution share or fallback heuristic score;
                                       -- NOT a calibrated probability of actual gender identity
    source           TEXT NOT NULL    -- 'names_dataset' | 'gender_guesser' | 'unknown' (no match / bare initials)
);
"""

RESET_TABLES = ["given_name_gender"]

# Token-level cache: first-name token (capitalized) -> (label, score, source).
# Mirrors the notebook's old module-level _CACHE dict.
_CACHE: dict = {}
_gg = None
_nd = None
_nd_failures = 0


def _package_version(dist_name: str) -> str:
    try:
        return version(dist_name)
    except PackageNotFoundError:
        return "unknown"


def _init_models(logger: logging.Logger) -> None:
    global _gg, _nd
    logger.info("  Loading names_dataset + gender_guesser (one-time, ~2-4GB RAM)...")
    import gender_guesser.detector as _gg_lib
    from names_dataset import NameDataset as _ND
    _gg = _gg_lib.Detector(case_sensitive=False)
    _nd = _ND()
    logger.info(
        f"  Models loaded. (names-dataset=={_package_version('names-dataset')}, "
        f"gender-guesser=={_package_version('gender-guesser')})"
    )


def _is_bare_initials(token: str) -> bool:
    """True for things like 'A', 'A.', 'AB', 'A.B.' -- not a real given name.

    Strip periods first (so 'A.B.' is judged on 'AB', not on its raw
    4-character length -- the original implementation compared
    `len(first) <= 1 or (len(first) == 2 and first.endswith("."))`
    directly against the undotted form, which let bare initials written
    without a trailing period (e.g. 'AB') or with two periods (e.g.
    'A.B.') slip through as if they were real names). What's left is an
    initial if it's at most two letters AND has no lower-case component --
    that excludes genuine short names like 'Jo' or 'Al'.
    """
    letters = token.replace(".", "")
    if len(letters) == 0:
        return True
    return len(letters) <= 2 and letters.isupper()


def infer_gender(given_name, threshold: float = 0.80):
    """Name-based gender inference -- NOT author gender identity.
    See the module docstring's methodological caveat before using this
    output in any paper claim."""
    global _nd_failures
    if not given_name or not str(given_name).strip():
        return "unknown", 0.0, "unknown"
    first = str(given_name).strip().split()[0]
    if _is_bare_initials(first):
        return "unknown", 0.0, "unknown"
    key = first.capitalize()
    if key in _CACHE:
        return _CACHE[key]

    result = ("unknown", 0.0, "unknown")

    try:
        r = _nd.search(key)
        if r and r.get("first_name") and r["first_name"].get("gender"):
            gd = r["first_name"]["gender"]
            fp = gd.get("Female", 0.0)
            mp = gd.get("Male", 0.0)
            total = fp + mp
            if total > 0.01:
                fr = fp / total
                if fr >= threshold:
                    result = ("female", round(fr, 3), "names_dataset")
                elif fr <= 1 - threshold:
                    result = ("male", round(1 - fr, 3), "names_dataset")
                else:
                    result = ("ambiguous", round(max(fr, 1 - fr), 3), "names_dataset")
    except Exception:
        _nd_failures += 1

    if result[0] == "unknown":
        GG = {
            "male": ("male", 0.92), "mostly_male": ("male", 0.75),
            "female": ("female", 0.92), "mostly_female": ("female", 0.75),
            "andy": ("ambiguous", 0.50), "unknown": ("unknown", 0.0),
        }
        label, score = GG.get(_gg.get_gender(key), ("unknown", 0.0))
        result = (label, score, "gender_guesser" if label != "unknown" else "unknown")

    _CACHE[key] = result
    return result


def ensure_schema(con: sqlite3.Connection) -> None:
    con.executescript(SCHEMA)
    con.commit()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Precompute given_name -> inferred gender for every distinct author given name.",
        formatter_class=argparse.RawDescriptionHelpFormatter, epilog=__doc__,
    )
    parser.add_argument("--db", default="pmc_fair.db")
    parser.add_argument("--reset", action="store_true")
    parser.add_argument("--batch-size", type=int, default=20_000)
    args = parser.parse_args()

    if args.batch_size <= 0:
        sys.exit(f"--batch-size must be positive, got {args.batch_size}")

    db_path = Path(args.db)
    log_path = db_path.with_suffix(".gender.log")
    if not db_path.exists():
        sys.exit(f"Database not found: {db_path}")

    logger = setup_logging(log_path)
    t0 = time.perf_counter()

    logger.info("=" * 62)
    logger.info("Gender Inference Precomputation  —  FAIR Study")
    logger.info("=" * 62)
    logger.info(f"  Database : {db_path}")
    logger.info("")

    con = sqlite3.connect(db_path)
    con.execute("PRAGMA journal_mode = WAL")
    con.execute("PRAGMA synchronous  = NORMAL")

    if args.reset:
        for tbl in RESET_TABLES:
            con.execute(f"DROP TABLE IF EXISTS {tbl}")
        con.commit()

    ensure_schema(con)

    already = {r[0] for r in con.execute("SELECT given_name FROM given_name_gender").fetchall()}
    logger.info(f"  Already classified : {len(already):,} given names")

    all_names = [r[0] for r in con.execute(
        "SELECT DISTINCT given_name FROM author WHERE given_name IS NOT NULL AND given_name != ''"
    ).fetchall()]
    todo = [n for n in all_names if n not in already]
    logger.info(f"  Distinct given names in DB : {len(all_names):,}")
    logger.info(f"  To classify                : {len(todo):,}")
    logger.info("")

    if not todo:
        logger.info("Nothing to do -- all given names already classified.")
        con.close()
        return

    _init_models(logger)

    buf = []
    n_done = 0
    for name in todo:
        label, score, source = infer_gender(name)
        buf.append((name, label, score, source))
        n_done += 1
        if len(buf) >= args.batch_size:
            con.executemany(
                "INSERT OR IGNORE INTO given_name_gender "
                "(given_name, gender, inference_score, source) VALUES (?,?,?,?)",
                buf,
            )
            con.commit()
            buf = []
            if n_done % (args.batch_size * 5) == 0:
                elapsed = time.perf_counter() - t0
                rate = n_done / elapsed if elapsed > 0 else 0
                remaining = (len(todo) - n_done) / rate if rate > 0 else 0
                logger.info(
                    f"  {n_done:,}/{len(todo):,} classified "
                    f"({rate:.0f}/s, ~{remaining/60:.1f} min remaining, "
                    f"unique tokens cached: {len(_CACHE):,}, "
                    f"names_dataset failures: {_nd_failures:,})"
                )
    if buf:
        con.executemany(
            "INSERT OR IGNORE INTO given_name_gender "
            "(given_name, gender, inference_score, source) VALUES (?,?,?,?)",
            buf,
        )
        con.commit()

    total = con.execute("SELECT COUNT(*) FROM given_name_gender").fetchone()[0]
    dist = con.execute(
        "SELECT gender, COUNT(*) FROM given_name_gender GROUP BY gender ORDER BY COUNT(*) DESC"
    ).fetchall()
    src_dist = con.execute(
        "SELECT source, COUNT(*) FROM given_name_gender GROUP BY source ORDER BY COUNT(*) DESC"
    ).fetchall()
    logger.info("")
    logger.info("=" * 62)
    logger.info("PRECOMPUTATION COMPLETE")
    logger.info(f"  Total given_name rows    : {total:,}")
    for label, cnt in dist:
        logger.info(f"    {label:10s} : {cnt:,}")
    logger.info("  By source:")
    for src, cnt in src_dist:
        logger.info(f"    {src:15s} : {cnt:,}")
    logger.info(f"  Unique first-token cache : {len(_CACHE):,}")
    logger.info(f"  names_dataset failures   : {_nd_failures:,}")
    logger.info(f"  Wall-clock time          : {(time.perf_counter()-t0)/60:.1f} min")
    con.close()


if __name__ == "__main__":
    main()
