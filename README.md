
#Multi-container app with Docker Compose.

###Services running:

(*) db → PostgreSQL 16, with a volume (db_data) for persistent storage.

(*) api → Python app (FastAPI) served by uvicorn, connecting to the Postgres DB via service name db.

(*) pgadmin → pgAdmin web UI for browsing and managing the Postgres database.


### Install project
python3 -m venv .venv
source .venv/bin/activate
pip list
pip -m pip install --upgrade pip
pip install -r requirements.txt

### Steps
create requirements.txt in api and streamlit
create docker files for api and streamlit
create docker compose file


docker compose up -d --build

 regtechdemo-api                     Built 
 regtechdemo-streamlit               Built
 Network regtechdemo_default         Created
 Volume "regtechdemo_db_data"        Created
 Volume "regtechdemo_pgadmin_data"   Created
 Container regtechdemo-db-1          Healthy
 Container regtechdemo-pgadmin-1     Started
 Container regtechdemo-api-1         Started
 Container regtechdemo-streamlit-1   Started

## Testing

docker compose down --volumes --remove-orphans
docker compose up -d --build
docker compose logs -f db
docker compose ps
docker compose exec api sh -lc 'ls -lh data' 

docker system prune

docker compose down api
docker compose build api
docker compose up -d --build
docker compose logs -f api

docker compose down -v && docker compose up -d --build


docker compose down streamlit & docker compose up streamlit -d --build


## Create Parquet file for trades
curl -X POST http://localhost:8000/make_trades \
  -H 'content-type: application/json' \
  -d '{"rows": 10000000, "chunk_size": 500000}'

## Poll status
curl http://localhost:8000/runs/<run_id>
When status = succeeded: ls -lh data/runs/<run_id>/trades.parquet

curl -X POST http://localhost:8000/make_trades \
  -H 'content-type: application/json' \
  -d '{"rows": 10000000, "chunk_size": 500000}'
Internal Server Error% 
docker compose logs -f api

## Load trades in the database
Trigger a load by run_id (written earlier by /make_trades)
curl -s "http://localhost:8000/load_trades?run_id=<your_run_id>" | jq

Load from an explicit path
curl -s "http://localhost:8000/load_trades?parquet_path=/app/data/runs/<run_id>/trades.parquet" | jq

## Poll status
curl -s http://localhost:8000/loads/<load_id> | jq

## Kernel to use virtual env
python -m ipykernel install --user --name .venv --display-name "Python (.venv)"


pytest -v --maxfail=1 --disable-warnings
pytest --cov=orchestrator --cov-report=term-missing