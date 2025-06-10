# api.py
# ──────────────────────────────────────────────────────────────────────────
# FastAPI server for:
#   • /schema                → expose tables & columns
#   • /configure             → save user selections + dependency plan
#   • /schedule_daily_2am    → register nightly extract via APScheduler
#
# Requires:
#   dependency_graph.py
#   extractor.py         (defines run_sync)
#   scheduler.py         (defines schedule_daily_2am)
#   config_store.py      (global in-memory config registry)
# ──────────────────────────────────────────────────────────────────────────
from typing import Dict, List

from fastapi import FastAPI, HTTPException , BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from sqlalchemy import create_engine, inspect
from sqlalchemy.exc import SQLAlchemyError

import config_store                       # ← NEW: shared store
from depend import DependencyGraph
from extract import run_sync
from scheduler import schedule_daily_2am

# ─── Database connection & helpers ────────────────────────────────────────
DATABASE_URL = (
    "postgresql+psycopg2://postgres:admin@localhost:5432/postgres"
)
engine     = create_engine(DATABASE_URL, pool_pre_ping=True)
inspector  = inspect(engine)
graph      = DependencyGraph(engine)

# ─── FastAPI instance & CORS ──────────────────────────────────────────────
app = FastAPI(title="ETL Configure / Plan API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],       # adjust for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ──────────────────────────────────────────────────────────────────────────
# 1.  Schema discovery
# ──────────────────────────────────────────────────────────────────────────
@app.get("/schema")
def get_schema(): #-> Dict[str, List[str]]
    """Return {table: [column, …]} for front-end picker."""
    tables = inspector.get_table_names(schema = 'company')
    table_dict = {}
    for table in tables:
        
        columns = inspector.get_columns(table , schema = 'company')
        table_columns = [col['name'] for col in columns]
        table_dict[table] = table_columns
    return table_dict

    # shorten version
    # return {
    #     t: [c["name"] for c in inspector.get_columns(t)]
    #     for t in inspector.get_table_names()
     
    # }

# ──────────────────────────────────────────────────────────────────────────
# 2.  Configure  (save selections + plan)
# ──────────────────────────────────────────────────────────────────────────
class FieldSelection(BaseModel):
    selections: Dict[str, List[str]]

@app.post("/configure")
def configure(body: FieldSelection):
    """
    • Compute minimal FK-safe load order
    • Store selections + plan in config_store
    • Return config_id & order
    """
    try:
        tables     = list(body.selections.keys())
        load_order = graph.sorted_tables(tables)

        cfg_id = config_store.next_id
        config_store.next_id += 1
        config_store.store[cfg_id] = {
            "selections": body.selections,
            "load_order": load_order,
        }

        return {"config_id": cfg_id, "load_order": load_order}

    except (ValueError, SQLAlchemyError) as exc:
        raise HTTPException(500, f"Configure failed: {exc}")

# # ──────────────────────────────────────────────────────────────────────────
# # 3.  Schedule nightly extract at 02:00 (Asia/Bangkok)
# # ──────────────────────────────────────────────────────────────────────────
class IDBody(BaseModel):
    config_id: int

@app.post("/schedule_daily_2am")
def schedule_daily(body: IDBody):
    """
    Register (or replace) a nightly 02:00 Asia/Bangkok sync
    for the given config_id.
    """
    cfg = config_store.store.get(body.config_id)
    if cfg is None:
        raise HTTPException(404, f"Config {body.config_id} not found")

    # APScheduler job id = "cfg-<id>" — unique per config
    schedule_daily_2am(run_sync, body.config_id)
    return {
        "status": "scheduled",
        "config_id": body.config_id,
        "cron": "0 2 * * *",
        "timezone": "Asia/Bangkok",
    }

class ExtractBody(BaseModel):
    config_id: int

@app.post("/extract_now")
def extract_now(body: ExtractBody, bg: BackgroundTasks):
    """
    Fire-and-forget extract in a FastAPI background thread.
    Returns immediately so UI stays responsive.
    """
    if body.config_id not in config_store.store:
        raise HTTPException(404, "Config not found")

    bg.add_task(run_sync, body.config_id)   # run_sync is in extractor.py
    return {"status": "started", "config_id": body.config_id}
