import sqlite3
import random
from datetime import datetime

db = "C:/Users/sunbeam/OneDrive/Desktop/Projects/Dataware_house_project/logs.db"

def get_log_tables(conn):
    cur = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'logs_%';")
    return [r[0] for r in cur.fetchall()]

# dimension table population

def ensure_dim_tables(conn):
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS dim_chip (
                chip_id TEXT PRIMARY KEY, 
                origin TEXT,
                launch_date TEXT,
                expected_yield REAL
                );""")
    cur.execute("CREATE TABLE IF NOT EXISTS dim_design_block (design_block TEXT PRIMARY KEY);")
    cur.execute("CREATE TABLE IF NOT EXISTS dim_team (team TEXT PRIMARY KEY);")
    cur.execute("CREATE TABLE IF NOT EXISTS dim_test_condition (test_id TEXT PRIMARY KEY, condition_txt TEXT);")
    cur.execute("CREATE TABLE IF NOT EXISTS dim_simulation (simulation_id TEXT PRIMARY KEY, start_time TEXT);")
    cur.execute("CREATE TABLE IF NOT EXISTS dim_business (impact_score TEXT PRIMARY KEY);")

    cur.execute("""CREATE TABLE IF NOT EXISTS metadata_audit(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                table_name TEXT,
                row_rowid INTEGER,
                column_name TEXT,
                old_value TEXT,
                new_value TEXT,
                changed_at TEXT,
                changed_by TEXT,
                comment TEXT
                );""")
    conn.commit()

def populate_dim_tables_if_empty(conn):
    cur = conn.cursor()
    
    # populating dim_chips
    cur.execute("SELECT COUNT(*) FROM dim_chip;")
    if cur.fetchone()[0] == 0:
        prefixes = ['Chip_A', 'Chip_B', 'Chip_C', 'Chip_D']
        rows = []
        for i in range(12):
            pid = prefixes[i % len(prefixes)] + str(i+1)
            origin = random.choice(['Fab_Taiwan', 'Fab_USA', 'Fab_Korea', 'Fab_China'])
            launch_date = None if random.random() < 0.1 else (datetime(2025,10,1).date().isoformat())
            expected_yield = None if random.random() < 0.1 else round(random.uniform(0.80,0.99),3)
            rows.append((pid, origin, launch_date, expected_yield))
        cur.executemany("INSERT INTO dim_chip (chip_id, origin, launch_date, expected_yield) VALUES (?,?,?,?);", rows)

        # populating dim_design_block
        cur.execute("SELECT COUNT(*) FROM dim_design_block;")
        if cur.fetchone()[0] == 0:
            blocks = [("Cache",), ("ALU",), ("I/O",), ("MemoryCtrl",), ("Interconnect",), ("PHY",), ("PowerMgmt",)]
            cur.executemany("INSERT INTO dim_design_block (design_block) VALUES(?);", blocks)

        # populating dim_team
        cur.execute("SELECT COUNT(*) FROM dim_team;")
        if cur.fetchone()[0] == 0:
            teams = [("RTL_Team",), ("Verification_Team",), ("DFT_Team",), ("Validation_Team",), ("Software_Team",)]
            cur.executemany("INSERT INTO dim_team (team) VALUES (?);", teams)

        # populating dim_test_condition
        cur.execute("SELECT COUNT(*) FROM dim_test_condition;")
        if cur.fetchone()[0] == 0:
            volts = ["0.9V", "1.0V", "1.1V", "1.2V"]
            temps = ["0C", "25C", "85C", "125C"]
            tc = [(f"TC{i+1:03d}", f"Voltage={random.choice(volts)}; Temp={random.choice(temps)};") for i in range(12)]
            cur.executemany("INSERT INTO dim_test_condition (test_id, condition_txt) VALUES (?,?);", tc)

        # populating dim_simulation
        cur.execute("SELECT COUNT(*) FROM dim_simulation;")
        if cur.fetchone()[0] == 0:
            sims = [(f"SIM{i+1:04d}", datetime(2025,1,1).isoformat()) for i in range(60)]
            cur.executemany("INSERT INTO dim_simulation (simulation_id, start_time) VALUES (?,?);", sims)

        # populating dim_business
        cur.execute("SELECT COUNT(*) FROM dim_business;")
        if cur.fetchone()[0] == 0:
            cur.executemany("INSERT INTO dim_business (impact_score) VALUES(?);", [("Low",), ("Medium",), ("High",)])
        conn.commit()

# helper functions
def column_exists(conn, table, column):
    cur = conn.execute(f"PRAGMA table_info({table});")
    cols = [r[1] for r in cur.fetchall()]
    return column in cols

def add_metadata_columns(conn, table):

    cols = [
        ("chip_id", "TEXT"),
        ("design_block", "TEXT"),
        ("team", "TEXT"),
        ("simulation_id", "TEXT"),
        ("test_condition", "TEXT"),
        ("impact_score", "TEXT"),
        ("metadata_source", "TEXT"),
        ("metadata_version", "TEXT"),
        ("metadata_assigned_at", "TEXT"),
        ("metadata_assigned_by", "TEXT"),
        ("enrichment_status", "TEXT")
    ]
    cur = conn.cursor()

    for name, typ in cols:
        if not column_exists(conn, table, name):
            cur.execute(f"ALTER TABLE {table} ADD COLUMN {name} {typ};")
    conn.commit()


# main orchestration
def main():
    conn = sqlite3.connect(db)
    conn.execute("PRAGMA journal_mode=WAL;") # better concurrency/performance
    print("Discovering log tables...")
    logs = get_log_tables(conn)
    print("Found:", logs)

    print("Ensuring dimension tables..")
    ensure_dim_tables(conn)
    populate_dim_tables_if_empty(conn)

    print("Adding metadata columns (if missing)...")
    for t in logs:
        add_metadata_columns(conn, t)

    conn.close()
    print("metadata_init completed. Database updated and ready for enrichment.")

if __name__ == "__main__":
    main()
