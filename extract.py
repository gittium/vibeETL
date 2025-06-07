# extractor.py  ────────────────────────────────────────────────────────────
"""
Stage → Merge extractor for MySQL.

CLI:
    python extractor.py <config_id>
"""
import os, sys
from typing import List, Dict
from collections import defaultdict

from sqlalchemy import (create_engine, MetaData, Table, Column, select, text, func , 
                        Integer, String ,BigInteger)
from sqlalchemy.schema import Sequence
import config_store

# ─── connection strings ───────────────────────────────────────────────────
SRC_URL  = os.getenv("SRC_URL",
    "postgresql+psycopg2://postgres:admin@localhost:5432/postgres")
DEST_URL = os.getenv("DEST_URL",
    "mysql+pymysql://mysql:pass@127.0.0.1:3307/mysql")

src_engine  = create_engine(SRC_URL,  pool_pre_ping=True)
dest_engine = create_engine(DEST_URL, pool_pre_ping=True)

CHUNK = 10_000                       # rows per batch
sync_state: Dict[int, Dict[str,int]] = defaultdict(dict)
#  sync_state[config_id][table] = last_pk

# ──────────────────────────────────────────────────────────────────────────
def required_cols(tbl: Table) -> List[str]:
    """NOT NULL & no default columns that must always be copied."""
    out = []
    for c in tbl.columns:
        if (not c.nullable) and c.default is None and c.server_default is None:
            out.append(c.name)
    return out

# ──────────────────────────────────────────────────────────────────────────
def ensure_tables(src_tbl: Table, dest_meta: MetaData):
    """
    Guarantee both destination & staging tables exist.
    Returns (dest_tbl, stg_tbl)
    """
    name      = src_tbl.name
    stg_name  = f"_stg_{name}"

    # --- destination table ---
    if name not in dest_meta.tables:
        dest_tbl = src_tbl.to_metadata(dest_meta)
        # wipe PG defaults / sequences
        for col in dest_tbl.columns:
            if isinstance(col.server_default, Sequence) or (
                col.server_default and "nextval" in str(col.server_default.arg)
            ):
                col.server_default = None
            col.default = None
        dest_tbl.create(bind=dest_engine)
        print(f"[+] Created dest table {name}")
    else:
        dest_tbl = dest_meta.tables[name]

    # --- staging table (always recreated empty) ---
    # --- staging table (always recreated empty) -----------------------------
    if stg_name in dest_meta.tables:
        dest_engine.execute(text(f"DROP TABLE `{stg_name}`"))
        dest_meta.remove(dest_meta.tables[stg_name])

    stg_cols = []                                   # fresh column list
    for c in src_tbl.columns:                       # iterate every source column
        clone = c.copy()                            # deep-copy Column -> new table

        # --- strip PostgreSQL-specific defaults / sequences ---
        if isinstance(clone.server_default, Sequence) or (
            clone.server_default is not None
            and "nextval" in str(clone.server_default.arg)
        ):
            clone.server_default = None             # remove PG sequence default
        clone.default = None                        # remove Python-side default

        stg_cols.append(clone)                      # add cleaned column to list

    # build the staging table with cleaned columns
    stg_tbl = Table(stg_name, dest_meta, *stg_cols)
    stg_tbl.create(bind=dest_engine)                # CREATE TABLE _stg_xxx (...)
    dest_meta.reflect(bind=dest_engine, only=[name, stg_name])

    return dest_meta.tables[name], dest_meta.tables[stg_name]

def is_numeric(column):
    return isinstance(column.type, (Integer, BigInteger))
# ──────────────────────────────────────────────────────────────────────────
def copy_to_staging(src_tbl: Table, cols: List[str], stg_tbl: Table,
                    last_pk):
    pk_col = list(src_tbl.primary_key.columns)[0]

    if is_numeric(pk_col):
        # ------------- numeric PK fast path -------------
        cursor = last_pk if last_pk is not None else -1
        while True:
            q = (
                select([src_tbl.c[c] for c in cols])
                .where(pk_col > cursor)
                .order_by(pk_col)
                .limit(CHUNK)
            )
            rows_raw = src_engine.execute(q).fetchall()
            if not rows_raw:
                break
            cursor = rows_raw[-1][pk_col.name]
            dest_engine.execute(stg_tbl.insert(), [dict(r) for r in rows_raw])
            print(f"   staged {len(rows_raw)} rows (pk up to {cursor})")
    else:
        # ------------- fallback OFFSET pagination -------------
        offset = 0
        while True:
            q = (
                select([src_tbl.c[c] for c in cols])
                .order_by(pk_col)
                .limit(CHUNK)
                .offset(offset)
            )
            rows = [dict(r) for r in src_engine.execute(q)]
            if not rows:
                break
            dest_engine.execute(stg_tbl.insert(), rows)
            offset += CHUNK
            print(f"   staged {len(rows)} rows (offset {offset})")
def copy_to_staging(src_tbl: Table, cols: List[str], stg_tbl: Table,
                    last_pk):
    pk_col = list(src_tbl.primary_key.columns)[0]

    if is_numeric(pk_col):
        # ------------- numeric PK fast path -------------
        cursor = last_pk if last_pk is not None else -1
        while True:
            q = (
                select([src_tbl.c[c] for c in cols])
                .where(pk_col > cursor)
                .order_by(pk_col)
                .limit(CHUNK)
            )
            rows_raw = src_engine.execute(q).fetchall()
            if not rows_raw:
                break
            cursor = rows_raw[-1][pk_col.name]
            dest_engine.execute(stg_tbl.insert(), [dict(r) for r in rows_raw])
            print(f"   staged {len(rows_raw)} rows (pk up to {cursor})")
    else:
        # ------------- fallback OFFSET pagination -------------
        offset = 0
        while True:
            q = (
                select([src_tbl.c[c] for c in cols])
                .order_by(pk_col)
                .limit(CHUNK)
                .offset(offset)
            )
            rows = [dict(r) for r in src_engine.execute(q)]
            if not rows:
                break
            dest_engine.execute(stg_tbl.insert(), rows)
            offset += CHUNK
            print(f"   staged {len(rows)} rows (offset {offset})")
                  


# ──────────────────────────────────────────────────────────────────────────
def merge_into_target(dest_tbl: Table, stg_tbl: Table, cols: List[str]):
    """
    REPLACE strategy: overwrite conflicting PK rows, insert new rows.
    """
    col_list = ", ".join(f"`{c}`" for c in cols)
    sql = text(f"""
        REPLACE INTO `{dest_tbl.name}` ({col_list})
        SELECT {col_list}
        FROM `{stg_tbl.name}`;
    """)
    dest_engine.execute(sql)
    dest_engine.execute(text(f"TRUNCATE `{stg_tbl.name}`"))

# ──────────────────────────────────────────────────────────────────────────
def run_sync(cfg_id: int, full_refresh: bool = False):
    cfg = config_store.store.get(cfg_id)
    if not cfg:
        print(f"Config {cfg_id} not found")
        return

    sel:   Dict[str, List[str]] = cfg["selections"]
    order: List[str]            = cfg["load_order"]

    src_meta  = MetaData(); src_meta.reflect(bind=src_engine, only=order)
    dest_meta = MetaData(); dest_meta.reflect(bind=dest_engine)

    dest_engine.execute(text("SET foreign_key_checks = 0"))  # disable FKs

    for tbl_name in order:
        src_tbl = src_meta.tables[tbl_name]

        # ── 1. Column list (user + required) ──────────────────────────────
        user_cols = sel.get(tbl_name, ["*"])
        cols = ([c.name for c in src_tbl.columns]        # user asked for "*"
                if user_cols == ["*"] else list(user_cols))
        cols = list(set(cols) | set(required_cols(src_tbl)))

        pk_col = list(src_tbl.primary_key.columns)[0]
        if pk_col.name not in cols:          # always include PK for cursor
            cols.append(pk_col.name)

        # ── 2. Ensure destination + staging tables ───────────────────────
        dest_tbl, stg_tbl = ensure_tables(src_tbl, dest_meta)
        print(f"[=] Processing {tbl_name}")

        # ── 3. Determine incremental cursor ──────────────────────────────
        last_pk = None if full_refresh else sync_state[cfg_id].get(tbl_name)

        # ── 4. Copy rows into staging ────────────────────────────────────
        copy_to_staging(src_tbl, cols, stg_tbl, last_pk)

        # ── 5. Merge staging → destination ───────────────────────────────
        merge_into_target(dest_tbl, stg_tbl, cols)

        # ── 6. Update cursor state ───────────────────────────────────────
        if is_numeric(pk_col):
            new_max = src_engine.execute(
                select(func.max(pk_col))
            ).scalar()
            sync_state[cfg_id][tbl_name] = new_max
        else:
            sync_state[cfg_id][tbl_name] = None  # non-numeric PK → full load next run

    dest_engine.execute(text("SET foreign_key_checks = 1"))  # re-enable FKs
    print("✅ Sync complete")


# ──────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python extractor.py <config_id>"); sys.exit(1)
    run_sync(int(sys.argv[1]))
