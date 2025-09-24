# New code

import sqlite3
import random
from datetime import datetime

DB = "C:/Users/sunbeam/OneDrive/Desktop/Projects/Dataware_house_project/logs.db"

# ----------------------------
# Helpers
# ----------------------------

def get_log_tables(conn):
    cur = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'logs_%';")
    return [r[0] for r in cur.fetchall()]

def table_has_column(conn, table, column):
    cur = conn.execute(f"PRAGMA table_info({table});")
    cols = [r[1] for r in cur.fetchall()]
    return column in cols

# ----------------------------
# Assignment Functions
# ----------------------------

def assign_from_dim_rowwise(conn, table, dim_table, dim_col, target_col):
    """Assign random values from a dimension table column into the target_col of the log table."""
    cur = conn.cursor()
    cur.execute(f"SELECT {dim_col} FROM {dim_table};")
    values = [r[0] for r in cur.fetchall()]

    if not values:
        print(f"⚠️ No values in {dim_table}, skipping {target_col} for {table}")
        return

    cur.execute(f"SELECT rowid FROM {table};")
    rowids = [r[0] for r in cur.fetchall()]

    for rid in rowids:
        val = random.choice(values)
        cur.execute(f"UPDATE {table} SET {target_col}=? WHERE rowid=?", (val, rid))

    conn.commit()
    print(f"✔ {target_col} assigned for {table} ({len(rowids)} rows)")

def assign_test_condition_random(conn, table):
    """Assign random test conditions to each row from dim_test_condition."""
    cur = conn.cursor()
    cur.execute("SELECT test_id FROM dim_test_condition;")
    test_conditions = [r[0] for r in cur.fetchall()]

    if not test_conditions:
        print("⚠️ No test conditions in dim_test_condition, skipping.")
        return

    cur.execute(f"SELECT rowid FROM {table};")
    rowids = [r[0] for r in cur.fetchall()]

    for rid in rowids:
        tc = random.choice(test_conditions)
        cur.execute(f"UPDATE {table} SET test_condition=? WHERE rowid=?", (tc, rid))

    conn.commit()
    print(f"✔ test_condition assigned for {table} ({len(rowids)} rows)")

def assign_enrichment_metadata(conn, table):
    """Mark enrichment metadata."""
    cur = conn.cursor()
    now = datetime.utcnow().isoformat()
    cur.execute(f"""
        UPDATE {table}
        SET enrichment_status='complete',
            metadata_assigned_at=?,
            metadata_assigned_by='metadata_enrich.py',
            metadata_version='v1.0'
    """, (now,))
    conn.commit()

# ----------------------------
# Validation
# ----------------------------

def validate_table(conn, table):
    cur = conn.cursor()
    cur.execute(f"SELECT COUNT(*) FROM {table} WHERE enrichment_status='complete';")
    complete = cur.fetchone()[0]

    cur.execute(f"SELECT test_condition, COUNT(*) FROM {table} GROUP BY test_condition;")
    tc = dict(cur.fetchall())

    cur.execute(f"SELECT impact_score, COUNT(*) FROM {table} GROUP BY impact_score;")
    impacts = dict(cur.fetchall())

    return {"complete": complete, "test_conditions": tc, "impact_scores": impacts}

# ----------------------------
# Main
# ----------------------------

def main():
    conn = sqlite3.connect(DB)
    conn.execute("PRAGMA journal_mode=WAL;")

    log_tables = get_log_tables(conn)
    print("Enriching metadata for:", log_tables)

    for table in log_tables:
        # assign basic dimensions
        assign_from_dim_rowwise(conn, table, "dim_chip", "chip_id", "chip_id")
        assign_from_dim_rowwise(conn, table, "dim_team", "team", "team")
        assign_from_dim_rowwise(conn, table, "dim_design_block", "design_block", "design_block")
        assign_from_dim_rowwise(conn, table, "dim_simulation", "simulation_id", "simulation_id")
        assign_from_dim_rowwise(conn, table, "dim_business", "impact_score", "impact_score")

        # assign test_condition separately
        assign_test_condition_random(conn, table)

        # mark enrichment done
        assign_enrichment_metadata(conn, table)

        print(f" - {table} enriched.")

    # validation summary
    print("Validation Summary")
    for table in log_tables:
        summary = validate_table(conn, table)
        print(f"Table {table}: {summary}")

    conn.close()
    print("Metadata enrichment complete. Tables ready for analytics.")

if __name__ == "__main__":
    main()
