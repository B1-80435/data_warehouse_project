import os
import re
import requests
import tarfile
import zipfile
import pandas as pd
import sqlite3

# ---------- Config ----------
DATA = {
    "bgl": "https://zenodo.org/records/8196385/files/BGL.zip?download=1",
    "openstack": "https://zenodo.org/records/8196385/files/OpenStack.tar.gz?download=1",
    "mac": "https://zenodo.org/records/8196385/files/Mac.tar.gz?download=1",
    "android_v1": "https://zenodo.org/records/8196385/files/Android_v1.zip?download=1",
    "openssh": "https://zenodo.org/records/8196385/files/SSH.tar.gz?download=1"
}

RAW_DIR = "raw_datasets"
DB_FILE = "logs.db"

# ---------- Regex parsers ----------

def parse_mac_log(file_path):
    """Parse Mac logs."""
    pattern = re.compile(r"^(\w+)\s+(\d+)\s+(\d+:\d+:\d+)\s+(\S+)\s+([^[]+)\[(\d+)\]:\s+(.*)$")
    rows = []
    with open(file_path, "r", errors="ignore") as f:
        for line in f:
            m = pattern.match(line.strip())
            if m:
                rows.append(m.groups())
    df = pd.DataFrame(rows, columns=["Month", "Date", "Time", "User", "Component", "PID", "Content"])
    return df

def parse_android_log(file_path):
    """Parse Android logs."""
    pattern = re.compile(r"^(\d{2}-\d{2})\s+(\d{2}:\d{2}:\d{2}\.\d+)\s+(\d+)\s+(\d+)\s+([A-Z])\s+(\S+):\s+(.*)$")
    rows = []
    with open(file_path, "r", errors="ignore") as f:
        for line in f:
            m = pattern.match(line.strip())
            if m:
                rows.append(m.groups())
    df = pd.DataFrame(rows, columns=["Date", "Time", "Pid", "Tid", "Level", "Component", "Content"])
    return df

# openstackr parser
def load_openstack_logs(file_paths):
    """Parse OpenStack log files into DF."""
    rows = []
    log_pattern = re.compile(
        r'^(?P<Logrecord>\S+)\s+'         # file log id
        r'(?P<Date>\d{4}-\d{2}-\d{2})\s+' # date
        r'(?P<Time>\d{2}:\d{2}:\d{2}\.\d+)\s+' # time
        r'(?P<Pid>\d+)\s+'                # pid
        r'(?P<Level>\w+)\s+'              # level
        r'(?P<Component>\S+)\s+'          # component
        r'(?P<ADDR>\[.*?\])\s+'           # request id block
        r'(?P<Content>.*)$'               # rest of line
    )

    for path in file_paths:
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            for line in f:
                match = log_pattern.match(line.strip())
                if match:
                    rows.append(match.groupdict())

    df = pd.DataFrame(rows)
    df.insert(0, "LineId", range(1, len(df)+1))
    return df

# ---------- Helpers ----------

def download_and_extract(name, url):
    """Download archive if not exists, then extract into dataset_dir."""
    os.makedirs(RAW_DIR, exist_ok=True)
    dataset_dir = os.path.join(RAW_DIR, name)
    os.makedirs(dataset_dir, exist_ok=True)

    ext = os.path.splitext(url.split("?")[0])[1]  # .zip or .gz
    archive_path = os.path.join(RAW_DIR, f"{name}{ext}")

    if not os.path.exists(archive_path):
        print(f"⬇️ Downloading {url}...")
        r = requests.get(url, stream=True)
        with open(archive_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
    else:
        print(f"Already downloaded: {archive_path}")

    # Extract
    if archive_path.endswith(".zip"):
        with zipfile.ZipFile(archive_path, "r") as zf:
            zf.extractall(dataset_dir)
    elif archive_path.endswith((".tar.gz", ".tgz", ".gz")):
        with tarfile.open(archive_path, "r:gz") as tf:
            tf.extractall(dataset_dir)

    return dataset_dir


def find_file(dataset_dir, pattern):
    """Find first file matching regex pattern inside dataset_dir."""
    for root, _, files in os.walk(dataset_dir):
        for f in files:
            if re.search(pattern, f, re.IGNORECASE):
                return os.path.join(root, f)
    raise FileNotFoundError(f"No file matching {pattern} in {dataset_dir}")

# ---------- Standardize Schema ----------

def standardize(df, content_col="Content", source="unknown"):
    """Map logs into a unified schema."""
    if content_col not in df.columns:
        # fallback: join all cols as content
        df["Content"] = df.astype(str).agg(" ".join, axis=1)
    else:
        df = df.copy()

    df = df[[content_col]].rename(columns={content_col: "Content"})
    df.insert(0, "LineId", range(1, len(df) + 1))
    df["EventId"] = None
    df["EventTemplate"] = None
    df["Source"] = source
    return df

# ---------- Load into SQLite ----------

def load_dataset_to_sqlite(conn, name, dataset_dir):
    table_name = f"logs_{name}"
    try:
        if name == "bgl":
            file_path = find_file(dataset_dir, r"BGL\.log$")

            # Step 1: Load only the first 9 structured columns
            df = pd.read_csv(
                file_path,
                sep=r"\s+",
                names=["Label", "Timestamp", "Date", "Node", "Time",
                    "NodeRepeat", "Type", "Component", "Level"],
                engine="python",
                usecols=range(9),
                on_bad_lines="skip"
            )

            # Step 2: Extract the 10th "Content" column from raw lines
            with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
                contents = [line.strip().split(maxsplit=9)[-1] for line in f]
            df["Content"] = contents

            # Step 3: Add LineId
            df.insert(0, "LineId", range(1, len(df) + 1))


        elif name == "openssh":
            file_path = find_file(dataset_dir, r"SSH.*\.log$")
            rows = []
            with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
                for line in f:
                    parts = line.strip().split(maxsplit=5)
                    if len(parts) == 6:
                        rows.append(parts)
            df = pd.DataFrame(rows, columns=["Date", "Day", "Time", "Component", "Pid", "Content"])
            df.insert(0, "LineId", range(1, len(df) + 1))


        elif name == "openstack":
            log_files = [
                os.path.join(dataset_dir, "openstack_normal1.log"),
                os.path.join(dataset_dir, "openstack_normal2.log"),]
            # file_path = find_file(dataset_dir, r"anomaly_labels")
            df = load_openstack_logs(log_files)            # adjust if anomaly_labels file already has some columns
            table_name = f"logs_{name}"
            df.to_sql(table_name, conn, if_exists="replace", index=False)
            print(f"Loaded {len(df)} rows into {table_name}")
            
        elif name == "mac":
            file_path = find_file(dataset_dir, r"Mac.*\.log$")
            df = parse_mac_log(file_path)
            # Mac regex parser already extracts some cols
            # df["Address"] = None  # placeholder
            df.insert(0, "LineId", range(1, len(df) + 1))
            # df["EventId"] = None
            # df["EventTemplate"] = None
            df = df[["LineId", "Month", "Date", "Time", "User", "Component",
                     "PID", "Content"]]

        elif name == "android_v1":
            file_path = find_file(dataset_dir, r"Android.*\.log$")
            df = parse_android_log(file_path)
            df.insert(0, "LineId", range(1, len(df) + 1))
            # df["EventId"] = None
            # df["EventTemplate"] = None
            df = df[["LineId", "Date", "Time", "Pid", "Tid", "Level",
                     "Component", "Content"]]

        else:
            print(f"No parser for {name}")
            return

        # Load into SQLite
        df.to_sql(table_name, conn, if_exists="replace", index=False)
        print(f"✅ Loaded {len(df)} rows into {table_name}")

    except Exception as e:
        print(f"❌ Failed to load {name}: {e}")


# ---------- Main ----------

def main():
    conn = sqlite3.connect(DB_FILE)
    for name, url in DATA.items():
        dataset_dir = download_and_extract(name, url)
        # Debug: show extracted files
        files = []
        for root, _, fs in os.walk(dataset_dir):
            for f in fs:
                files.append(os.path.relpath(os.path.join(root, f), dataset_dir))
        print(f"\n📂 Files in {dataset_dir}:")
        for f in files:
            print("   ", f)

        load_dataset_to_sqlite(conn, name, dataset_dir)

    conn.close()
    print("All datasets standardized and loaded into SQLite successfully.")

if __name__ == "__main__":
    main()
