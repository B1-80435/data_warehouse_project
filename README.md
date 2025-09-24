# 📊 Data Warehousing – Log Analytics with Metadata Enrichment

This project builds a **log analytics data warehouse** with metadata enrichment and an **interactive dashboard** for exploration. It enables engineers to filter and analyze log data by chip, block, team, impact score, and test conditions.

---

## 📂 Project Structure

```
.
├── logs.py              # Step 1 – Download & ingest raw logs into SQLite
├── metadata_init.py     # Step 2 – Initialize metadata tables
├── metadata_enrich.py   # Step 3 – Enrich log data with metadata attributes
├── dash.py              # Step 4 – Streamlit dashboard for analysis
├── logs.db              # Auto-generated SQLite database
└── README.md            # Project documentation
```

---

## ⚙️ Setup Instructions

1. **Clone the repository**
   ```bash
   git clone <your_repo_url>
   cd <your_repo_name>
   ```

2. **Create a virtual environment (recommended)**
   ```bash
   python -m venv venv
   source venv/bin/activate     # For Linux/Mac
   venv\Scripts\activate        # For Windows
   ```

3. **Install dependencies**
   ```bash
   pip install streamlit plotly
   ```

---

## 🚀 How to Run the Pipeline

The project follows a **four-step execution order**:

### Step 1 – Ingest Logs
```bash
python logs.py
```
- Downloads and extracts raw logs (dataset source: [LogPai GitHub](https://github.com/logpai/loghub))
- Creates `logs_*` tables in SQLite (`logs.db`)
- Populates tables with raw simulation logs

### Step 2 – Initialize Metadata
```bash
python metadata_init.py
```
- Creates metadata dimension tables (chip family, design block, team, test condition, etc.)
- Prepares mapping rules for enrichment

### Step 3 – Enrich Logs
```bash
python metadata_enrich.py
```
- Reads from both log tables and metadata tables
- Produces enriched log tables with searchable attributes:
  - Chip family  
  - Design block  
  - Team  
  - Test condition  
  - Impact score  

### Step 4 – Launch Dashboard
```bash
python -m streamlit run dash.py
```
- Opens an **interactive Streamlit dashboard** in the browser  
- Key Features:
  - **Filters:** chip, block, team, impact score, test condition  
  - **KPIs:** total logs, unique chips, impacted blocks, impacted teams  
  - **Visuals:**  
    - Error distribution (by block, team, score)  
    - Error trends over time  
    - Heatmap (team vs block)  
    - Categorization trends (failure modes)  
    - Anomaly detection with rolling averages  
    - Raw log viewer + CSV export  

---

## 🔑 Key Outcomes

- Centralized **SQLite log warehouse** with metadata enrichment  
- Engineers can quickly filter logs by multiple attributes  
- Dashboard provides insights such as:  
  - Error trends & anomalies  
  - Workload distribution across teams  
  - Pareto & correlation analyses  
  - Heatmaps for **problem localization**  

---

## 📌 Dataset

- The log dataset is sourced from the (dataset source: [LogPai GitHub](https://github.com/logpai/loghub)).  
- Used as raw input for `logs.py` during ingestion.  

---

## 📝 Notes

- Ensure **all four scripts** are run in order:  
  `1. logs.py → 2. metadata_init.py → 3. metadata_enrich.py → 4. dash.py`  
- Use a fresh database (`logs.db`) if you re-run the pipeline.  
- Streamlit dashboard runs locally at: **http://localhost:8501**  

---

## 🤝 Contribution

Feel free to fork, raise issues, or submit pull requests to enhance the functionality (e.g., adding more visualizations, connecting to external DBs, or scaling with cloud services).
