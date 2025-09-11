
python3 -m venv .venv
source .venv/bin/activate
pip list
pip -m pip install --upgrade pip
pip install -r requirements.txt

create requirements.txt in api and streamlit
create docker files for api and streamlit
create docker compose file

Multi-container app with Docker Compose.
Services running:

db → PostgreSQL 16, with a volume (db_data) for persistent storage.

api → Python app (FastAPI) served by uvicorn, connecting to the Postgres DB via service name db.

pgadmin → pgAdmin web UI for browsing and managing the Postgres database.


docker compose up -d

[+] Running 9/9
 regtechdemo-api                     Built 
 regtechdemo-streamlit               Built
 Network regtechdemo_default         Created
 Volume "regtechdemo_db_data"        Created
 Volume "regtechdemo_pgadmin_data"   Created
 Container regtechdemo-db-1          Healthy
 Container regtechdemo-pgadmin-1     Started
 Container regtechdemo-api-1         Started
 Container regtechdemo-streamlit-1   Started


RegTechDemo on  main [?] via 🐳 desktop-linux via 🐍 v3.13.1 (.venv) on ☁️  (eu-west-2) took 1m11s 
❯ docker compose ps -a
NAME                      IMAGE                   COMMAND                  SERVICE     CREATED         STATUS                          PORTS
regtechdemo-api-1         regtechdemo-api         "uvicorn /bin/sh -c …"   api         3 minutes ago   Restarting (2) 42 seconds ago   
regtechdemo-db-1          postgres:16             "docker-entrypoint.s…"   db          3 minutes ago   Up 3 minutes (healthy)          0.0.0.0:5432->5432/tcp, [::]:5432->5432/tcp
regtechdemo-pgadmin-1     dpage/pgadmin4:latest   "/entrypoint.sh"         pgadmin     3 minutes ago   Up 3 minutes                    0.0.0.0:5050->80/tcp, [::]:5050->80/tcp
regtechdemo-streamlit-1   regtechdemo-streamlit   "streamlit run app.p…"   streamlit   3 minutes ago   Up 3 minutes                    0.0.0.0:8501->8501/tcp, [::]:8501->8501/tcp


docker compose down --volumes --remove-orphans
docker compose up -d --build
docker compose logs -f db
docker compose ps

docker compose exec api sh -lc 'ls -lh /app' 

Kick off a 10M-row build (adjust chunk_size to your RAM/CPU)
curl -X POST http://localhost:8000/make_trades \
  -H 'content-type: application/json' \
  -d '{"rows": 10000000, "chunk_size": 500000}'

Poll status
curl http://localhost:8000/runs/<run_id>
When status = succeeded:
ls -lh data/runs/<run_id>/trades.parquet


docker system prune


to debug the service api:

code change
docker compose down api
docker compose build api
docker compose up -d --build
docker compose logs -f api
curl -X POST http://localhost:8000/make_trades \
  -H 'content-type: application/json' \
  -d '{"rows": 10000000, "chunk_size": 500000}'
Internal Server Error% 
docker compose logs -f api



# 1) Trigger a load by run_id (written earlier by /make_trades)
curl -s "http://localhost:8000/load_trades?run_id=<your_run_id>" | jq

# 2) Or load from an explicit path
curl -s "http://localhost:8000/load_trades?parquet_path=/app/data/runs/<run_id>/trades.parquet" | jq

# 3) Poll status
curl -s http://localhost:8000/loads/<load_id> | jq
