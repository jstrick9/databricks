import os
import time
from pathlib import Path
from datetime import datetime, timedelta, date
import pytz
import yaml
import streamlit as st
import pandas as pd
from databricks import sql
from databricks.sdk import WorkspaceClient

# -------------------------------
# YAML + Paths
# -------------------------------
def read_yaml(file_path: str):
    with open(file_path, "r") as f:
        return yaml.safe_load(f)

def _app_root() -> Path:
    return Path(__file__).resolve().parent.parent

def _get_env(default: str = "dev") -> str:
    return os.getenv("ENVIRONMENT", default)

def _get_configs(dbx_env: str):
    cfg_path = _app_root() / "config" / dbx_env / "wait-times-conf.yaml"
    if not cfg_path.exists():
        raise FileNotFoundError(f"Config not found for env {dbx_env}: {cfg_path}")
    cfg = read_yaml(str(cfg_path))
    if "schema" not in cfg or "trgt_catalog" not in cfg["schema"]:
        raise KeyError("Invalid config")
    return cfg

def _normalize_host(host: str | None) -> str | None:
    if not host:
        return None
    return host.replace("https://","").replace("http://","").rstrip("/")

def sql_escape(value) -> str:
    if value is None:
        return ""
    return str(value).replace("\\","\\\\").replace("'","''")

def show_temp_message(message_type, message, seconds=3):
    placeholder = st.empty()
    getattr(placeholder, message_type, placeholder.info)(message)
    time.sleep(seconds)
    placeholder.empty()

# -------------------------------
# DB Connection
# -------------------------------
def _resolve_http_path_by_name(w: WorkspaceClient, name: str) -> str:
    matches = [wh for wh in w.warehouses.list() if (wh.name or "").strip() == name.strip()]
    if not matches:
        raise ValueError(f"No Warehouse named '{name}'")
    if len(matches)>1:
        ids = ", ".join(getattr(m,"id","unknown") for m in matches)
        raise ValueError(f"Multiple warehouses match '{name}': {ids}")
    wh = matches[0]
    http_path = getattr(wh.odbc_params, "http_path", None) or getattr(wh.odbc_params, "path", None)
    if not http_path:
        raise ValueError(f"Warehouse '{name}' has no http_path")
    return http_path

def _create_fresh_connection():
    w = WorkspaceClient()
    host = _normalize_host(w.config.host)
    if not host:
        raise ValueError("No host")
    wname = os.getenv("DATABRICKS_WAREHOUSE_NAME")
    if not wname:
        raise ValueError("DATABRICKS_WAREHOUSE_NAME not set")
    http_path = _resolve_http_path_by_name(w, wname)
    headers = w.config.authenticate()
    token = headers.get("Authorization","").split(" ",1)[-1]
    if not token:
        raise ValueError("No OAuth token")
    return sql.connect(server_hostname=host, http_path=http_path, access_token=token)

def _is_connection_alive(conn) -> bool:
    if conn is None:
        return False
    try:
        cur = conn.cursor()
        cur.execute("SELECT 1")
        cur.fetchone()
        cur.close()
        return True
    except Exception:
        return False

def _close_connection_safely(conn):
    if conn is not None:
        try:
            conn.close()
        except:
            pass

def get_connection(max_retries=3, retry_delay_seconds=2):
    cached = st.session_state.get("_db_connection")
    if cached is not None and _is_connection_alive(cached):
        return cached, cached.cursor()
    if cached is not None:
        _close_connection_safely(cached)
        st.session_state.pop("_db_connection", None)
    placeholder = st.empty()
    last_error = None
    for attempt in range(1, max_retries+1):
        try:
            placeholder.info(f"🔄 Connecting to SQL Warehouse... (attempt {attempt}/{max_retries})")
            conn = _create_fresh_connection()
            if _is_connection_alive(conn):
                st.session_state["_db_connection"] = conn
                placeholder.empty()
                return conn, conn.cursor()
            else:
                raise ValueError("Health check failed")
        except Exception as e:
            last_error = e
            if attempt < max_retries:
                time.sleep(retry_delay_seconds*attempt)
    placeholder.empty()
    st.error("❌ Failed to connect to SQL Warehouse")
    with st.expander("🔍 Details"):
        st.code(str(last_error))
    st.stop()

def clear_connection_cache():
    cached = st.session_state.get("_db_connection")
    if cached:
        _close_connection_safely(cached)
        st.session_state.pop("_db_connection", None)

# -------------------------------
# Wait Times Specific DB Functions
# -------------------------------
def get_silver_summary(cursor, configs):
    catalog = configs["schema"]["trgt_catalog"]
    silver_schema = configs["schema"].get("silver_schema","silver")
    table = f"{catalog}.{silver_schema}.case_milestones"
    try:
        cursor.execute(f"SELECT COUNT(*) as total, COUNT_IF(_is_current=true) as current FROM {table}")
        row = cursor.fetchone()
        total, current = row[0], row[1] if len(row)>1 else row[0]
        cursor.execute(f"SELECT MAX(_updated_ts) FROM {table} WHERE _is_current=true")
        max_ts = cursor.fetchone()[0]
        return {"total": total, "current": current, "max_ts": max_ts, "table": table}
    except Exception as e:
        return {"error": str(e), "table": table}

def get_latest_gold_snapshot(cursor, configs):
    catalog = configs["schema"]["trgt_catalog"]
    gold_schema = configs["schema"].get("gold_schema","gold")
    table = f"{catalog}.{gold_schema}.processing_wait_times"
    try:
        cursor.execute(f"SELECT MAX(snapshot_date) FROM {table}")
        max_date = cursor.fetchone()[0]
        return max_date
    except Exception as e:
        return None

def get_processing_wait_times(cursor, configs, snapshot_date=None):
    catalog = configs["schema"]["trgt_catalog"]
    gold_schema = configs["schema"].get("gold_schema","gold")
    table = f"{catalog}.{gold_schema}.processing_wait_times"
    try:
        if snapshot_date:
            cursor.execute(f"SELECT * FROM {table} WHERE snapshot_date = '{sql_escape(snapshot_date)}' ORDER BY metric_key")
        else:
            cursor.execute(f"SELECT * FROM {table} WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM {table}) ORDER BY metric_key")
        cols = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        df = pd.DataFrame(rows, columns=cols)
        return df
    except Exception as e:
        st.error(f"Failed to fetch gold: {e}")
        return pd.DataFrame()

def get_metric_targets(cursor, configs):
    catalog = configs["schema"]["trgt_catalog"]
    gold_schema = configs["schema"].get("gold_schema","gold")
    table = f"{catalog}.{gold_schema}.metric_targets"
    try:
        cursor.execute(f"SELECT * FROM {table} ORDER BY sort_order")
        cols = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        df = pd.DataFrame(rows, columns=cols)
        return df
    except Exception as e:
        # If table doesn't exist, return defaults from config
        defaults = configs.get("default_targets", {})
        data = []
        for k,v in defaults.items():
            data.append({"metric_key": k, "target_value": v})
        return pd.DataFrame(data)

def update_metric_target(cursor, configs, metric_key, target_value, metric_name=None, section=None, unit=None, sort_order=None):
    """Legacy wrapper – updates target_value + optionally unit, metric_name, section, sort_order"""
    return update_metric_target_full(cursor, configs, metric_key, target_value=target_value, metric_name=metric_name, section=section, unit=unit, sort_order=sort_order)

def update_metric_target_full(cursor, configs, metric_key, target_value=None, metric_name=None, section=None, unit=None, sort_order=None):
    """Full update – allows editing metric_name, section, unit, target_value, sort_order"""
    catalog = configs["schema"]["trgt_catalog"]
    gold_schema = configs["schema"].get("gold_schema","gold")
    table = f"{catalog}.{gold_schema}.metric_targets"
    try:
        cursor.execute(f"SELECT COUNT(*) FROM {table} WHERE metric_key = '{sql_escape(metric_key)}'")
        exists = cursor.fetchone()[0] > 0
        if exists:
            sets = []
            if target_value is not None:
                sets.append(f"target_value = {float(target_value)}")
            if unit is not None:
                # Validate unit
                u = sql_escape(unit.lower())
                if u not in ["days","months"]:
                    # allow but coerce
                    u = u
                sets.append(f"unit = '{sql_escape(unit)}'")
            if metric_name is not None:
                sets.append(f"metric_name = '{sql_escape(metric_name)}'")
            if section is not None:
                sets.append(f"section = '{sql_escape(section)}'")
            if sort_order is not None:
                sets.append(f"sort_order = {int(sort_order)}")
            if not sets:
                return False, "No fields to update"
            set_clause = ", ".join(sets)
            cursor.execute(f"UPDATE {table} SET {set_clause} WHERE metric_key = '{sql_escape(metric_key)}'")
            return True, f"Updated {metric_key}: {set_clause}"
        else:
            m_name = sql_escape(metric_name or metric_key)
            sec = sql_escape(section or "")
            u = sql_escape(unit or "days")
            so = int(sort_order or 0)
            tv = float(target_value) if target_value is not None else 0.0
            cursor.execute(f"INSERT INTO {table} (metric_key, metric_name, section, unit, target_value, sort_order) VALUES ('{sql_escape(metric_key)}','{m_name}','{sec}','{u}',{tv},{so})")
            return True, f"Inserted {metric_key} target {tv} unit {u}"
    except Exception as e:
        return False, str(e)

def calculate_preview_via_sql(cursor, configs, lookback_months=18, filing_basis_filter=None):
    """
    Hybrid quick preview: Compute wait times directly via SQL on silver, v14.4 logic (filing-only, mean ITU, median postreg*0.71)
    """
    catalog = configs["schema"]["trgt_catalog"]
    silver_schema = configs["schema"].get("silver_schema","silver")
    table = f"{catalog}.{silver_schema}.case_milestones"

    # v14.4 metrics SQL with per-metric lookback and filing_basis
    metrics_sql = {
        # summary months
        "first_action": ("filing_date", "first_oa_date", None, 18, False),
        "registration_or_abandonment": ("filing_date", "disposal_date", None, 18, False),
        # pre-exam: TEAS 9mo mean, MADRID filing->ib 18mo mean (best possible 74.7 vs live10)
        "pre_exam_teas": ("filing_date", "first_oa_date", "(filing_basis ILIKE '%TEAS%' OR filing_basis ILIKE '%USE%' OR filing_basis ILIKE '%ITU%' OR filing_basis='' OR filing_basis ILIKE '%NO BASIS%' OR filing_basis ILIKE '%BASE%')", 9, False),
        "pre_exam_madrid": ("filing_date", "ib_notification_date", "filing_basis ILIKE '%MADRID%' OR filing_basis ILIKE '%66A%' OR filing_basis ILIKE '%IR%'", 18, False),
        # ITU + ESU + LOP – mean, filing-only, per-metric lookback
        "esu_responses": ("esu_response_date", "esu_processed_date", None, 18, False),
        "itu_extension": ("extension_request_date", "extension_processed_date", None, 24, False),
        "itu_sou": ("sou_filing_date", "sou_processed_date", None, 18, False),
        "itu_divisional": ("divisional_request_date", "divisional_processed_date", None, 60, False),
        "petitions_lop": ("lop_filing_date", "lop_processed_date", None, 18, False),
        # postreg – median *0.71 business days
        "postreg_affidavit": ("affidavit_filing_date", "affidavit_processed_date", None, 18, True),
        "postreg_renewal": ("renewal_filing_date", "renewal_processed_date", None, 18, True),
        "postreg_amendment": ("amendment_filing_date", "amendment_processed_date", None, 18, True),
    }

    results = []
    for key, (filing, processed, extra, lb, use_median) in metrics_sql.items():
        try:
            lb = lb or lookback_months
            where_extra = f"AND ({extra})" if extra else ""
            # trim for postreg/ITU
            trim_filter = "AND DATEDIFF({processed}, {filing}) BETWEEN 0 AND 365".format(processed=processed, filing=filing)
            if use_median:
                agg_expr = f"percentile_approx(DATEDIFF({processed}, {filing}), 0.5) as avg_days"
                # business days factor applied later
            else:
                agg_expr = f"AVG(DATEDIFF({processed}, {filing})) as avg_days"
            sql_q = f"""
                SELECT 
                    {agg_expr},
                    COUNT(*) as n,
                    MIN({filing}) as min_filing,
                    MAX({filing}) as max_filing
                FROM {table}
                WHERE _is_current = true
                  AND {filing} IS NOT NULL AND {processed} IS NOT NULL
                  AND {filing} >= DATEADD(month, -{lb}, CURRENT_DATE())
                  {where_extra}
                  {trim_filter}
            """
            cursor.execute(sql_q)
            row = cursor.fetchone()
            avg_days, n, min_f, max_f = row if row else (None,0,None,None)
            # business days factor for postreg
            if avg_days and key.startswith("postreg_"):
                avg_days = avg_days * 0.71
            results.append({"metric_key": key, "avg_days": avg_days, "n": n, "min_filing": min_f, "max_filing": max_f, "lookback": lb, "median": use_median})
        except Exception as e:
            results.append({"metric_key": key, "error": str(e), "n":0})
    return pd.DataFrame(results)

def get_exam_queue_window(cursor, configs, lookback_months=18):
    catalog = configs["schema"]["trgt_catalog"]
    silver_schema = configs["schema"].get("silver_schema","silver")
    table = f"{catalog}.{silver_schema}.case_milestones"
    try:
        cursor.execute(f"""
            SELECT 
                percentile_approx(filing_date, 0.25) as q_start,
                percentile_approx(filing_date, 0.75) as q_end
            FROM {table}
            WHERE _is_current = true AND first_oa_date IS NULL AND filing_date IS NOT NULL
              AND filing_date >= DATEADD(month, -{lookback_months}, CURRENT_DATE())
        """)
        row = cursor.fetchone()
        return row[0], row[1]
    except Exception as e:
        return None, None

def write_json_to_publish_bucket(publish_bucket, snapshot_date, payload: dict, configs=None):
    """
    Write JSON to S3 or UC Volume. 
    In Databricks Apps, boto3 S3 will fail with "Unable to locate credentials" because Apps don't have job cluster instance profile.
    FIX: On credentials error, fallback to UC Volume /Volumes/{catalog}/gold/wait_times via WorkspaceClient.files.upload.
    publish_bucket must come from config/{env}/wait-times-conf.yaml per env – dev=lab-tmdc, prod=prod-tmdc.
    configs is optional – used to get volume alternative or catalog for fallback.
    """
    import json
    import tempfile
    json_str = json.dumps(payload, indent=2, default=str)
    if not publish_bucket:
        return False, "publish_bucket is empty – check config"
    # Helper to write to Volume
    def _write_to_volume(volume_path, snapshot_date, json_str):
        try:
            w = WorkspaceClient()
            versioned_path = f"{volume_path.rstrip('/')}/wait_times_{snapshot_date}.json"
            latest_path = f"{volume_path.rstrip('/')}/wait_times_latest.json"
            with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as tf:
                tf.write(json_str)
                temp_path = tf.name
            try:
                with open(temp_path, 'rb') as f:
                    w.files.upload(versioned_path, f, overwrite=True)
                with open(temp_path, 'rb') as f:
                    w.files.upload(latest_path, f, overwrite=True)
                return True, f"Wrote {versioned_path} and {latest_path} via WorkspaceClient.files.upload (S3 fallback due to no credentials in Apps)"
            finally:
                try:
                    os.remove(temp_path)
                except:
                    pass
        except Exception as ve:
            return False, f"Volume write failed: {ve}"

    try:
        if publish_bucket.startswith("s3://"):
            try:
                import boto3
                from botocore.exceptions import NoCredentialsError, PartialCredentialsError
            except ImportError:
                NoCredentialsError = Exception
                PartialCredentialsError = Exception
            try:
                path = publish_bucket.replace("s3://","")
                if "/" in path:
                    bucket, prefix = path.split("/",1)
                else:
                    bucket, prefix = path, ""
                s3 = boto3.client("s3")
                versioned_key = f"{prefix.rstrip('/')}/wait_times_{snapshot_date}.json".lstrip("/")
                latest_key = f"{prefix.rstrip('/')}/wait_times_latest.json".lstrip("/")
                s3.put_object(Bucket=bucket, Key=versioned_key, Body=json_str.encode('utf-8'), ContentType='application/json')
                s3.put_object(Bucket=bucket, Key=latest_key, Body=json_str.encode('utf-8'), ContentType='application/json')
                return True, f"Wrote s3://{bucket}/{versioned_key} and s3://{bucket}/{latest_key} (env bucket from config)"
            except (NoCredentialsError, PartialCredentialsError) as cred_e:
                # Apps don't have instance profile – fallback to UC Volume
                # Try to get volume path from configs or infer from catalog
                volume_path = None
                if configs:
                    volume_path = configs.get("publish",{}).get("volume_path") or configs.get("publish",{}).get("uc_volume")
                    if not volume_path:
                        # Infer from catalog
                        catalog = configs.get("schema",{}).get("trgt_catalog", "trm_tmngpdb_dev")
                        gold_schema = configs.get("schema",{}).get("gold_schema","gold")
                        volume_path = f"/Volumes/{catalog}/{gold_schema}/wait_times"
                if not volume_path:
                    # Last resort: use payload env to guess
                    env = payload.get("dbx_env","dev")
                    if env == "prod":
                        volume_path = "/Volumes/trm_tmngpdb/gold/wait_times"
                    else:
                        volume_path = "/Volumes/trm_tmngpdb_dev/gold/wait_times"
                st.warning(f"⚠️ S3 write failed due to no AWS credentials in Apps (expected) – falling back to UC Volume `{volume_path}`. For prod S3, use scheduled job publish_wait_times task which has instance_profile.")
                return _write_to_volume(volume_path, snapshot_date, json_str)
            except Exception as e:
                # If error is "Unable to locate credentials" substring, also fallback
                err_str = str(e)
                if "Unable to locate credentials" in err_str or "NoCredentialsError" in err_str or "credentials" in err_str.lower():
                    volume_path = None
                    if configs:
                        volume_path = configs.get("publish",{}).get("volume_path")
                        if not volume_path:
                            catalog = configs.get("schema",{}).get("trgt_catalog", "trm_tmngpdb_dev")
                            gold_schema = configs.get("schema",{}).get("gold_schema","gold")
                            volume_path = f"/Volumes/{catalog}/{gold_schema}/wait_times"
                    if not volume_path:
                        env = payload.get("dbx_env","dev")
                        volume_path = "/Volumes/trm_tmngpdb/gold/wait_times" if env=="prod" else "/Volumes/trm_tmngpdb_dev/gold/wait_times"
                    st.warning(f"⚠️ S3 credentials not found in Apps – falling back to Volume `{volume_path}`")
                    return _write_to_volume(volume_path, snapshot_date, json_str)
                else:
                    return False, f"S3 write failed: {e}"
        elif publish_bucket.startswith("/Volumes/"):
            return _write_to_volume(publish_bucket, snapshot_date, json_str)
        else:
            return False, f"Unsupported publish_bucket scheme: {publish_bucket} – must be s3:// or /Volumes/ from config"
    except Exception as e:
        return False, str(e)

def insert_audit_log(cursor, configs, job_name, task_name, status, records, message):
    catalog = configs["schema"]["trgt_catalog"]
    gold_schema = configs["schema"].get("gold_schema","gold")
    table = f"{catalog}.{gold_schema}.etl_audit_log"
    try:
        cursor.execute(f"""
            INSERT INTO {table} (run_id, job_name, task_name, status, records_processed, message, run_ts)
            VALUES (uuid(), '{sql_escape(job_name)}', '{sql_escape(task_name)}', '{sql_escape(status)}', {int(records)}, '{sql_escape(message)}', current_timestamp())
        """)
        return True
    except Exception as e:
        return False

def trigger_wait_times_job(configs, lookback_months=18, snapshot_date=None, exam_start=None, exam_end=None, sou_queue=None, renewal_queue=None, data_updated=None):
    """
    Trigger the existing wf_trademark_processing_wait_times job via Jobs API
    All bucket/env values come from config, no hardcoded S3.
    """
    try:
        w = WorkspaceClient()
        job_name = configs.get("jobs",{}).get("wait_times_job_name","wf_trademark_processing_wait_times")
        jobs = list(w.jobs.list())
        matched = [j for j in jobs if job_name in (j.settings.name or "")]
        if not matched:
            return False, f"Job not found with name containing {job_name}"
        job_id = matched[0].job_id
        params = {
            "dbx_env": os.getenv("ENVIRONMENT","dev"),
            "lookback_months": str(lookback_months),
        }
        if snapshot_date:
            params["snapshot_date"] = str(snapshot_date)
        if exam_start:
            params["exam_queue_start_date"] = str(exam_start)
        if exam_end:
            params["exam_queue_end_date"] = str(exam_end)
        if sou_queue:
            params["sou_queue_date"] = str(sou_queue)
        if renewal_queue:
            params["renewal_queue_date"] = str(renewal_queue)
        if data_updated:
            params["data_updated_date"] = str(data_updated)
        run = w.jobs.run_now(job_id=job_id, job_parameters=params)
        return True, f"Triggered job {job_name} (id {job_id}) run {run.run_id} with params {params}"
    except Exception as e:
        return False, str(e)
