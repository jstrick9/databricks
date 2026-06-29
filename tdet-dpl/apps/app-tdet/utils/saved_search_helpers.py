# utils/saved_search_helpers.py

import uuid
import json
import pandas as pd
import streamlit as st
from datetime import datetime
from utils.db_helpers import get_connection, sql_escape


def save_new_preset(
    name: str,
    search_type: str,
    config: dict,
    user_email: str,
    tdet_catalog: str,
    tdet_schema: str,
):
    """Save a new search configuration preset."""
    if not name or not config:
        return False, "Name and configuration are required."

    conn, cursor = get_connection()
    if not cursor:
        return False, "DB Connection failed"

    try:
        email_clean = sql_escape((user_email or "").strip().lower())
        name_clean = sql_escape(name)

        # Check for Duplicate Name
        check_sql = f"""
        SELECT 1 FROM {tdet_catalog}.{tdet_schema}.tdet_app_saved_searches 
        WHERE user_email = '{email_clean}' AND lower(search_name) = lower('{name_clean}')
        """
        cursor.execute(check_sql)
        if cursor.fetchone():
            return (
                False,
                f"A saved search with the name '{name}' already exists. "
                f"Please choose a different name.",
            )

        # Insert New
        new_id = str(uuid.uuid4())
        ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        config_str = json.dumps(config).replace("'", "''")

        sql_query = f"""
        INSERT INTO {tdet_catalog}.{tdet_schema}.tdet_app_saved_searches
        (id, user_email, search_name, search_type_code, config_json, _created_timestamp)
        VALUES ('{new_id}', '{email_clean}', '{name_clean}', '{sql_escape(search_type)}', '{config_str}', '{ts}')
        """
        cursor.execute(sql_query)
        return True, "Search saved successfully!"
    except Exception as e:
        return False, str(e)
    finally:
        # FIXED: only close cursor, NOT connection (it's cached/shared)
        try:
            cursor.close()
        except Exception:
            pass


def get_user_presets(
    user_email: str,
    tdet_catalog: str,
    tdet_schema: str,
    filters: dict = None,
    limit: int = 5,
) -> pd.DataFrame:
    """Get saved searches with optional filtering."""
    conn, cursor = get_connection()
    if not cursor:
        return pd.DataFrame()

    try:
        email_clean = sql_escape((user_email or "").strip().lower())

        base_query = f"""
        SELECT id, search_name, search_type_code, config_json, _created_timestamp
        FROM {tdet_catalog}.{tdet_schema}.tdet_app_saved_searches
        WHERE user_email = '{email_clean}'
        """

        conditions = []
        if filters:
            if filters.get("search_name"):
                val = sql_escape(filters["search_name"].strip())
                conditions.append(f"search_name ILIKE '%{val}%'")
            if filters.get("search_type"):
                val = sql_escape(filters["search_type"].strip())
                conditions.append(f"search_type_code = '{val}'")

        if conditions:
            base_query += " AND " + " AND ".join(conditions)

        base_query += f" ORDER BY _created_timestamp DESC LIMIT {limit}"

        cursor.execute(base_query)

        if hasattr(cursor, "fetchall_arrow"):
            arrow_table = cursor.fetchall_arrow()
            if arrow_table and arrow_table.num_rows > 0:
                df = arrow_table.to_pandas()
            else:
                df = pd.DataFrame(
                    columns=[
                        "id",
                        "search_name",
                        "search_type_code",
                        "config_json",
                        "_created_timestamp",
                    ]
                )
        else:
            data = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            df = pd.DataFrame(data, columns=columns)

        return df

    except Exception as e:
        return pd.DataFrame()
    finally:
        # FIXED: only close cursor, NOT connection
        try:
            cursor.close()
        except Exception:
            pass


def delete_preset(preset_id: str, tdet_catalog: str, tdet_schema: str):
    """Delete a saved search."""
    conn, cursor = get_connection()
    if not cursor:
        return
    try:
        pid = sql_escape(preset_id)
        cursor.execute(
            f"DELETE FROM {tdet_catalog}.{tdet_schema}.tdet_app_saved_searches WHERE id = '{pid}'"
        )
    except Exception:
        pass
    finally:
        # FIXED: only close cursor, NOT connection
        try:
            cursor.close()
        except Exception:
            pass