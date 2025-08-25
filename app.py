import os
import io
import re
import string
import hashlib
import streamlit as st
import pandas as pd
from datetime import datetime
from openai import OpenAI
from openpyxl import load_workbook
from openpyxl.utils import get_column_letter
from openpyxl.styles import Alignment
import sqlalchemy as sa
from sqlalchemy import text
from dotenv import load_dotenv
# =========================
# Config (Render environment)
# =========================
# Read from environment for safety. In Render, set these in the dashboard.

load_dotenv()

try:
    # Try Streamlit secrets first (when deployed to Streamlit Cloud)
    OPENAI_API_KEY = st.secrets["OPENAI_API_KEY"]
    DATABASE = st.secrets["DATABASE"]
except:
    # Fallback to environment variables (for local development or other deployments)
    OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
    DATABASE = os.getenv("DATABASE")
    
DATABASE_URL = DATABASE.strip()

MASTER_XLSX = "genai_job_impact_master.xlsx"  # optional: one-time import + downloadable snapshot
ALL_JOBS_SHEET = "All Jobs"
SYNTHESIS_SHEET = "Synthesis"

st.set_page_config(page_title="GenAI Job Impact Analyst — Postgres on Render", layout="wide")
st.title("💼 GenAI Job Impact Analyst — Postgres (Render)")

# Fail fast if env not set
if not OPENAI_API_KEY:
    st.error("OPENAI_API_KEY env var is not set.")
    st.stop()
if not DATABASE_URL:
    st.error("DATABASE_URL env var is not set. Use the External Database URL with ?sslmode=require.")
    st.stop()

client = OpenAI(api_key=OPENAI_API_KEY)

# =========================
# Helpers (parsing, normalize, etc.)
# =========================
def extract_roles_from_text(raw_text: str) -> list[str]:
    blocks = re.split(r'\n\s*---\s*\n', raw_text.strip(), flags=re.MULTILINE)
    return [b.strip() for b in blocks if b.strip()]

def role_name_from_jobdesc(jd: str, index: int) -> str:
    m = re.search(r'(?i)^ *Job Title:\s*(.+)$', jd, flags=re.MULTILINE)
    if m:
        name = m.group(1).strip()
    else:
        first = next((ln.strip() for ln in jd.splitlines() if ln.strip()), f"Job_{index+1}")
        name = first.split("|")[0].split(" - ")[0].strip()
    name = re.sub(r'[\[\]\*\?/\\:]', "", name)[:120]
    return name or f"Job_{index+1}"

def parse_markdown_table(md_text: str) -> pd.DataFrame:
    lines = [ln.rstrip() for ln in md_text.splitlines()]
    table_lines = [ln.strip() for ln in lines if "|" in ln]
    sep_pat = r'^\s*\|?\s*[-:]+(?:\s*\|\s*[-:]+)*\s*\|?\s*$'
    table_lines = [ln for ln in table_lines if not re.match(sep_pat, ln)]
    if not table_lines:
        return pd.DataFrame()
    rows = []
    for ln in table_lines:
        parts = [cell.strip() for cell in ln.strip("|").split("|")]
        rows.append(parts)
    header = rows[0]
    data = rows[1:] if len(rows) > 1 else []
    while header and header[-1] == "":
        header = header[:-1]
        data = [r[:-1] for r in data]
    df = pd.DataFrame(data, columns=header)
    df = df.loc[:, ~(df.columns.str.strip() == "")]
    return df

def split_table_and_synthesis(text: str) -> tuple[str, str]:
    parts = text.split("Synthesis:")
    if len(parts) == 2:
        return parts[0], parts[1].strip()
    parts = re.split(r'(?i)Synthèse\s*:', text)
    if len(parts) == 2:
        return parts[0], parts[1].strip()
    return text, ""

def normalize_task(task: str) -> str:
    if not isinstance(task, str):
        task = "" if pd.isna(task) else str(task)
    t = task.lower().strip()
    t = re.sub(r'\s+', ' ', t)
    table = str.maketrans('', '', string.punctuation)
    t = t.translate(table)
    return t

def add_provenance(df: pd.DataFrame, role_name: str, jd_text: str) -> pd.DataFrame:
    run_id = datetime.now().isoformat(timespec="seconds")
    jd_hash = hashlib.sha256(jd_text.strip().encode("utf-8")).hexdigest()[:12]
    if "Job Title" not in df.columns:
        df.insert(0, "Job Title", role_name)
    else:
        df["Job Title"] = df["Job Title"].replace("", role_name).fillna(role_name)
    df["Run ID"] = run_id
    df["JD Hash"] = jd_hash
    return df

# =========================
# Excel (optional snapshot + one-time import)
# =========================
def load_master_excel() -> tuple[pd.DataFrame, pd.DataFrame]:
    if not os.path.exists(MASTER_XLSX):
        return pd.DataFrame(), pd.DataFrame(columns=["Job Title", "Synthesis", "Run ID", "JD Hash"])
    try:
        xl = pd.ExcelFile(MASTER_XLSX)
        all_jobs = pd.read_excel(xl, sheet_name=ALL_JOBS_SHEET)
        syn = pd.read_excel(xl, sheet_name=SYNTHESIS_SHEET)
        return all_jobs, syn
    except Exception:
        return pd.DataFrame(), pd.DataFrame(columns=["Job Title", "Synthesis", "Run ID", "JD Hash"])

def write_master_excel(all_jobs: pd.DataFrame, synthesis: pd.DataFrame) -> io.BytesIO:
    cols = all_jobs.columns.tolist()
    if "Job Title" in cols:
        cols = ["Job Title"] + [c for c in cols if c != "Job Title"]
        all_jobs = all_jobs[cols]
    with pd.ExcelWriter(MASTER_XLSX, engine="openpyxl") as writer:
        all_jobs.to_excel(writer, sheet_name=ALL_JOBS_SHEET, index=False)
        synthesis.to_excel(writer, sheet_name=SYNTHESIS_SHEET, index=False)
    wb = load_workbook(MASTER_XLSX)
    if ALL_JOBS_SHEET in wb.sheetnames:
        ws = wb[ALL_JOBS_SHEET]
        for col in ws.columns:
            max_len = 0
            col_letter = get_column_letter(col[0].column)
            for c in col:
                c.alignment = Alignment(wrap_text=True, vertical="top")
                if c.value:
                    max_len = max(max_len, len(str(c.value)))
            ws.column_dimensions[col_letter].width = min(max_len + 2, 60)
    wb.save(MASTER_XLSX)
    buf = io.BytesIO()
    wb.save(buf)
    buf.seek(0)
    return buf

# =========================
# Postgres (Render) via SQLAlchemy
# =========================
def get_engine() -> sa.Engine:
    return sa.create_engine(DATABASE_URL, pool_pre_ping=True, future=True)

def ensure_sql_schema(engine: sa.Engine):
    with engine.begin() as conn:
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS all_jobs (
            id SERIAL PRIMARY KEY,
            job_title TEXT,
            task TEXT,
            time_allocation TEXT,
            ai_impact_score TEXT,
            impact_explanation TEXT,
            task_transformation TEXT,
            tooling_nature TEXT,
            run_id TEXT,
            jd_hash TEXT,
            task_norm TEXT,
            CONSTRAINT uq_job_task UNIQUE (job_title, task_norm)
        );
        """))
        
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS synthesis (
            id SERIAL PRIMARY KEY,
            job_title TEXT,
            synthesis TEXT,
            run_id TEXT,
            jd_hash TEXT,
            CONSTRAINT uq_job_synthesis UNIQUE (job_title, jd_hash)
        );
        """))


def upsert_all_jobs_sql(engine: sa.Engine, df: pd.DataFrame):
    if df.empty:
        return

    out = pd.DataFrame({
        "job_title": df.get("Job Title", ""),
        "task": df.get("Task", ""),
        "time_allocation": df.get("Time allocation %", ""),
        "ai_impact_score": df.get("AI Impact Score (0–100)", ""),   # 👈 suspect line
        "impact_explanation": df.get("Impact Explanation", ""),
        "task_transformation": df.get("Task Transformation %", ""),
        "tooling_nature": df.get("Tooling nature % generic vs specific", ""),
        "run_id": df.get("Run ID", ""),
        "jd_hash": df.get("JD Hash", ""),
        "task_norm": df["Task"].apply(normalize_task) if "Task" in df.columns else ""
    }).replace({pd.NA: "", None: ""})

    # Deduplicate before insert
    out = out.drop_duplicates(subset=["job_title", "task_norm"], keep="last")

    rows = out.to_dict(orient="records")

    # ✅ ADD THIS DEBUG PRINT HERE
    if rows:
        print("DEBUG first row:", rows[0].get("ai_impact_score"))

    with engine.begin() as conn:
        stmt = text("""
            INSERT INTO all_jobs
            (job_title, task, time_allocation, ai_impact_score, impact_explanation,
             task_transformation, tooling_nature, run_id, jd_hash, task_norm)
            VALUES (:job_title, :task, :time_allocation, :ai_impact_score, :impact_explanation,
                    :task_transformation, :tooling_nature, :run_id, :jd_hash, :task_norm)
            ON CONFLICT (job_title, task_norm) DO UPDATE SET
                time_allocation = EXCLUDED.time_allocation,
                ai_impact_score = EXCLUDED.ai_impact_score,
                impact_explanation = EXCLUDED.impact_explanation,
                task_transformation = EXCLUDED.task_transformation,
                tooling_nature = EXCLUDED.tooling_nature,
                run_id = EXCLUDED.run_id,
                jd_hash = EXCLUDED.jd_hash;
        """)
        conn.execute(stmt, rows)

def append_synthesis_sql(engine: sa.Engine, syn_rows: list[dict]):
    if not syn_rows:
        return
    with engine.begin() as conn:
        conn.execute(
            text("""
                INSERT INTO synthesis (job_title, synthesis, run_id, jd_hash)
                VALUES (:job_title, :synthesis, :run_id, :jd_hash)
                ON CONFLICT (job_title, jd_hash) DO NOTHING;
            """),
            [ { 
                "job_title": r.get("job_title",""),
                "synthesis": r.get("synthesis",""),
                "run_id": r.get("run_id",""),
                "jd_hash": r.get("jd_hash","")
              } for r in syn_rows ]
        )

# One-time Excel -> Postgres import (idempotent)
def migrate_excel_to_postgres(engine: sa.Engine, excel_path: str = MASTER_XLSX):
    with engine.begin() as conn:
        conn.exec_driver_sql("""
            CREATE TABLE IF NOT EXISTS app_migrations (
                key TEXT PRIMARY KEY,
                applied_at TIMESTAMP DEFAULT NOW()
            );
        """)
        if conn.execute(text("SELECT 1 FROM app_migrations WHERE key='excel_to_pg'")).first():
            return

    if not os.path.exists(excel_path):
        # still mark as done to skip re-checking each run
        with engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO app_migrations(key) VALUES ('excel_to_pg')
                ON CONFLICT (key) DO NOTHING;
            """))
        return

    try:
        xl = pd.ExcelFile(excel_path)
        jobs = pd.read_excel(xl, sheet_name=ALL_JOBS_SHEET)
        syn  = pd.read_excel(xl, sheet_name=SYNTHESIS_SHEET)
    except Exception:
        jobs, syn = pd.DataFrame(), pd.DataFrame()

    if not jobs.empty and "Task" in jobs.columns:
        jobs["task_norm"] = jobs["Task"].apply(normalize_task)
        jobs = jobs.drop_duplicates(subset=["Job Title", "task_norm"], keep="last")

    upsert_all_jobs_sql(engine, jobs)

    if not syn.empty:
        syn_rows = [{
            "job_title": r.get("Job Title",""),
            "synthesis": r.get("Synthesis",""),
            "run_id":    r.get("Run ID",""),
            "jd_hash":   r.get("JD Hash",""),
        } for r in syn.to_dict(orient="records")]
        append_synthesis_sql(engine, syn_rows)

    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO app_migrations(key) VALUES ('excel_to_pg')
            ON CONFLICT (key) DO NOTHING;
        """))

# =========================
# UI State
# =========================
if "new_reports" not in st.session_state:
    st.session_state["new_reports"] = {}   # role -> df
if "new_synthesis" not in st.session_state:
    st.session_state["new_synthesis"] = {} # role -> text
if "new_jd_text" not in st.session_state:
    st.session_state["new_jd_text"] = {}   # role -> raw JD

# =========================
# Sidebar & Main Controls
# =========================
st.sidebar.header("Upload or Write Job Description(s)")
uploaded_file = st.sidebar.file_uploader("Upload job descriptions (.txt or .csv)", type=["txt", "csv"])
job_text = st.sidebar.text_area("Or paste a single job description here")
st.sidebar.caption(
    "💡 Multiple roles? In .txt, separate with a line containing only `---`. "
    "In .csv, provide one job description per row under a column named 'JobDescription'."
)

# Prominent Generate button
generate_clicked_sidebar = st.sidebar.button("🚀 Generate Report", type="primary")
col1, col2 = st.columns([1, 1])

with col1:
    generate_clicked_main = st.button("🚀 Generate Report", type="primary")

with col2:
    st.markdown(
        """
        <a href="https://app.powerbi.com/view?r=eyJrIjoiMDFhMGVlOGItOTY5MC00ZTRhLWI5ZTEtNmMwNDQxNTUzNTNmIiwidCI6IjA3NmEzOTkyLTA0ZjgtNDcwMC05ODQ0LTA4YzM3NDc3NzdlZiJ9" 
           target="_blank">
            <button style="background-color:#0078D4; color:white; padding:0.6em 1.2em; border:none; border-radius:8px; cursor:pointer;">
                📊 Dashboard
            </button>
        </a>
        """,
        unsafe_allow_html=True
    )


# =========================
# Preflight DB (connect, ensure schema, migrate once)
# =========================
engine = get_engine()
try:
    with engine.connect() as c:
        c.execute(sa.text("SELECT 1"))
    st.success("✅ Connected to Render Postgres")
except Exception as e:
    st.error(f"❌ Postgres connection failed: {e}")
    st.stop()

ensure_sql_schema(engine)
migrate_excel_to_postgres(engine)

# =========================
# Generate
# =========================
if generate_clicked_sidebar or generate_clicked_main:
    job_descriptions = []
    if uploaded_file is not None:
        if uploaded_file.name.endswith(".txt"):
            raw_text = uploaded_file.read().decode("utf-8")
            job_descriptions = extract_roles_from_text(raw_text)
        elif uploaded_file.name.endswith(".csv"):
            df_csv = pd.read_csv(uploaded_file)
            # Expecting a column named 'JobDescription'
            if "JobDescription" in df_csv.columns:
                job_descriptions = df_csv["JobDescription"].dropna().astype(str).tolist()
            else:
                st.error("CSV must contain a column named 'JobDescription'")
                st.stop()
    elif job_text.strip():
        job_descriptions = [job_text.strip()]
    else:
        st.error("Please upload or paste at least one job description.")
        st.stop()

    system_prompt = """
You are GenAI-Job-Impact-Analyst, an expert designed to evaluate how generative AI can transform work at Club Med. 
Your mission
Input: You will receive the full text of a Club Med job description.
Output: Produce a table – one line per task – with the following six columns: 
| Task | Time allocation % | AI Impact Score (0–100) | Impact Explanation | Task Transformation % | Tooling nature % generic vs specific |

Task – concise verb-phrase copied or paraphrased from the job description. 
Time allocation % – your best estimate of the share of the job’s total time this task takes (sum ≈ 100%). 
AI Impact Score – how strongly Gen-AI could affect the task (0 = no impact, 100 = fully automatable/augmented). 
Impact Explanation – 2–3 sentences justifying the chosen score. 
Task Transformation % – proportion of the task likely to change for the employee (e.g., 70% up-skilling vs 30% pure automation). 
Tooling nature – split the AI tooling you foresee into generic (ChatGPT-like, “80” default) vs domain-specific (custom models or vertical SaaS, “20” default). Express as two numbers that sum to 100. 

Procedure
A. Scan the description and list every distinct, non-trivial activity. 
B. Estimate Time allocation % first – it anchors later scores. 
C. For each activity, ask yourself: Could Gen-AI draft, summarize, translate, ideate, classify, predict or converse here? How big a quality- or speed-gain would that bring? 
D. Assign the numeric scores and craft clear rationales in French. 
E. Deliver the table, then add a one-paragraph synthesis highlighting the top three automation opportunities and any human-core tasks that should stay manual. 

Formatting rules
Use Markdown. Keep the table width manageable (wrap long explanations after 80 chars). 
Round percentages to the nearest 5%. Do not invent tasks that are absent from the description.
"""

    with st.spinner("Analyzing job description(s) with GPT-4o..."):
        for idx, jd in enumerate(job_descriptions):
            role_name = role_name_from_jobdesc(jd, idx)
            user_prompt = f"Here is the job description:\n\n{jd}"

            resp = client.chat.completions.create(
                model="gpt-4o",
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                ]
            )
            output_text = resp.choices[0].message.content

            st.markdown(f"### 📊 Generated Report — **{role_name}**")
            st.markdown(output_text)

            table_text, synthesis_text = split_table_and_synthesis(output_text)
            df = parse_markdown_table(table_text)
            if df.empty:
                df = pd.DataFrame({"Job Title": [role_name], "Report": [output_text],
                                   "Run ID": [datetime.now().isoformat(timespec="seconds")], "JD Hash": [""]})
            else:
                df = add_provenance(df, role_name, jd)
                # session-level dedup for same run (do not alter UI)
                if "Task" in df.columns:
                    _tmp = df.copy()
                    _tmp["task_norm"] = _tmp["Task"].apply(normalize_task)
                    _tmp = _tmp.drop_duplicates(subset=["Job Title", "task_norm"], keep="last")
                    df = _tmp.drop(columns=["task_norm"], errors="ignore")

            st.session_state["new_reports"][role_name] = df
            st.session_state["new_synthesis"][role_name] = synthesis_text
            st.session_state["new_jd_text"][role_name] = jd

# =========================
# Persist (Excel snapshot + Postgres write)
# =========================
if st.session_state["new_reports"]:
    st.divider()
    st.subheader("⬇️ Update Excel (optional) + Postgres (Render)")
    if st.button("Update Master"):
        # Build new tasks
        new_tasks = pd.concat(st.session_state["new_reports"].values(), ignore_index=True)

        # Excel snapshot (optional for download)
        existing_tasks, existing_syn = load_master_excel()
        if "Task" in new_tasks.columns:
            new_tasks["Task_norm"] = new_tasks["Task"].apply(normalize_task)
            # Dedup within new set
            new_tasks = new_tasks.drop_duplicates(subset=["Job Title", "Task_norm"], keep="last")
        if not existing_tasks.empty and "Task" in existing_tasks.columns:
            existing_tasks["Task_norm"] = existing_tasks["Task"].apply(normalize_task)
        if existing_tasks.empty:
            all_tasks = new_tasks.copy()
        else:
            if "Task" in new_tasks.columns and "Task" in existing_tasks.columns:
                key_cols = ["Job Title", "Task_norm"]
                merged = new_tasks.merge(
                    existing_tasks[key_cols].drop_duplicates(),
                    on=key_cols,
                    how="left",
                    indicator=True
                )
                to_add = merged[merged["_merge"] == "left_only"].drop(columns=["_merge"])
                all_tasks = pd.concat([existing_tasks, to_add], ignore_index=True)
            else:
                all_tasks = pd.concat([existing_tasks, new_tasks], ignore_index=True)
        if "Task_norm" in all_tasks.columns:
            all_tasks.drop(columns=["Task_norm"], inplace=True, errors="ignore")

        new_syn_rows = []
        for role, syn in st.session_state["new_synthesis"].items():
            jd_text = st.session_state["new_jd_text"].get(role, "")
            run_id = datetime.now().isoformat(timespec="seconds")
            jd_hash = hashlib.sha256(jd_text.strip().encode("utf-8")).hexdigest()[:12]
            new_syn_rows.append({"Job Title": role, "Synthesis": syn, "Run ID": run_id, "JD Hash": jd_hash})
        new_syn_df = pd.DataFrame(new_syn_rows, columns=["Job Title", "Synthesis", "Run ID", "JD Hash"])
        all_syn = pd.concat([existing_syn, new_syn_df], ignore_index=True) if not existing_syn.empty else new_syn_df
        # Dedup synthesis Excel snapshot
        if not all_syn.empty and "JD Hash" in all_syn.columns:
            all_syn = all_syn.drop_duplicates(subset=["Job Title", "JD Hash"], keep="last")

        # Excel snapshot: write + download
        buf = write_master_excel(all_tasks, all_syn)
        st.success(f"Master Excel updated: {MASTER_XLSX}")
        st.download_button(
            label="📥 Download Current Master Excel",
            data=buf,
            file_name=MASTER_XLSX,
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

        # Postgres: upsert tasks + append synthesis
        upsert_all_jobs_sql(engine, new_tasks if "Task" in new_tasks.columns else all_tasks)
        syn_rows = []
        for role, syn in st.session_state["new_synthesis"].items():
            jd_text = st.session_state["new_jd_text"].get(role, "")
            syn_rows.append({
                "job_title": role,
                "synthesis": syn,
                "run_id": datetime.now().isoformat(timespec="seconds"),
                "jd_hash": hashlib.sha256(jd_text.strip().encode("utf-8")).hexdigest()[:12]
            })
        append_synthesis_sql(engine, syn_rows)
        st.success("✅ Postgres updated")

        # Clear buffers
        st.session_state["new_reports"].clear()
        st.session_state["new_synthesis"].clear()
        st.session_state["new_jd_text"].clear()
