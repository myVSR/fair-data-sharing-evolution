#!/bin/bash
#SBATCH --tasks=1
#SBATCH --partition=standard
#SBATCH --job-name=dataset_creation
#SBATCH --output=dataset_creation.out.%j
#SBATCH --error=dataset_creation.err.%j

# 1. Point to the pyenv directory
export PYENV_ROOT="$HOME/.pyenv"
export PATH="$PYENV_ROOT/bin:$PATH"

export LD_LIBRARY_PATH="$HOME/sqlite/lib:$LD_LIBRARY_PATH"
export LIBRARY_PATH="$HOME/sqlite/lib:$LIBRARY_PATH"
export CPATH="$HOME/sqlite/include:$CPATH"

# 2. Initialize pyenv
eval "$(pyenv init -)"

# 3. Select the version you installed
pyenv shell 3.11.0

# 4. Verify (this should now show 3.11.0)
python --version

pip install aiohttp

# --- FUJI setup ---------------------------------------------------------
# FUJI pins jsonschema<4.18, but requirements.txt below (via jupyterlab)
# pulls jsonschema>=4.18 into the same pyenv. That's a real version
# conflict even though it wasn't the cause of the crash we saw (that was
# a stray blank line in fuji_server/data/doi_prefixes.tsv, fixed
# separately). Isolating FUJI in its own venv avoids this whole class of
# dependency drift between FUJI and the analysis/notebook stack.
if [ ! -d fuji ]; then
    git clone https://github.com/pangaea-data-publisher/fuji
fi
cd fuji

if [ ! -d .venv ]; then
    python -m venv .venv
fi
.venv/bin/python -m pip install --upgrade pip
.venv/bin/python -m pip install .

FUJI_HOST=$(sed -n 's/^service_host *= *//p' fuji_server/config/server.ini | tr -d '[:space:]')
FUJI_PORT=$(sed -n 's/^service_port *= *//p' fuji_server/config/server.ini | tr -d '[:space:]')

.venv/bin/python -m fuji_server -c fuji_server/config/server.ini &
FUJI_PID=$!
trap 'kill "$FUJI_PID" 2>/dev/null' EXIT

# Wait for FUJI to actually accept connections before the pipeline
# starts hammering it (previously the pipeline started immediately and
# raced the server's startup).
echo "Waiting for FUJI on ${FUJI_HOST}:${FUJI_PORT} ..."
for i in $(seq 1 60); do
    if ! kill -0 "$FUJI_PID" 2>/dev/null; then
        echo "FUJI server process died during startup" >&2
        exit 1
    fi
    if (exec 3<>"/dev/tcp/${FUJI_HOST}/${FUJI_PORT}") 2>/dev/null; then
        exec 3>&-
        echo "FUJI is up after ${i}s"
        break
    fi
    sleep 1
    if [ "$i" -eq 60 ]; then
        echo "FUJI did not come up within 60s" >&2
        exit 1
    fi
done
# --- end FUJI setup ------------------------------------------------------

cd ..
pip install requests
pip install -r requirements.txt
#pip install aiohttp
bash pipeline/run_pipeline.sh




