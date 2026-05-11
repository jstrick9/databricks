"""
AI Remediation Engine.
Uses Databricks Foundation Models to suggest corrections for quarantined data.
"""
import json
from pyspark.sql import functions as F
from mlflow.deployments import get_deploy_client

# Constants
MODEL_ENDPOINT = "databricks-meta-llama-3-1-70b-instruct"

def get_ai_suggestions(failed_records_list):
    """
    Takes a list of dictionaries (failed rows from error_log) 
    and returns them with 'suggested_fix' and 'confidence' populated.
    
    Optimized to batch requests to the LLM.
    """
    client = get_deploy_client("databricks")
    
    # Construct a prompt that asks for multiple fixes at once to save tokens/time
    context_data = []
    for rec in failed_records_list:
        context_data.append({
            "id": rec["error_log_id"],
            "column": rec["column_name"],
            "failed_value": rec["failed_value"],
            "error": rec["error_message"]
        })

    prompt = f"""
    You are a Data Remediation Expert. For each item in the following JSON list, 
    provide the most likely correct value.
    
    Rules:
    1. If the error is a typo in a country, provide the 2-letter ISO code.
    2. If the error is a date format, provide YYYY-MM-DD.
    3. Return a JSON list of objects with "id", "suggested_fix", and "confidence" (0.0 to 1.0).
    4. If you are not sure, return "MANUAL_REVIEW" for suggested_fix.
    
    DATA:
    {json.dumps(context_data)}
    """

    try:
        response = client.predict(
            endpoint=MODEL_ENDPOINT,
            inputs={"messages": [{"role": "user", "content": prompt}], "temperature": 0.1}
        )
        # Parse the AI response
        ai_output = response["choices"][0]["message"]["content"].strip()
        # Clean up markdown if AI included it
        if "```json" in ai_output:
            ai_output = ai_output.split("```json")[1].split("```")[0].strip()
            
        suggestions = json.loads(ai_output)
        return {s["id"]: (s["suggested_fix"], s["confidence"]) for s in suggestions}
    except Exception as e:
        print(f"AI Fixer failed: {e}")
        return {}

def apply_fix_to_source(catalog, schema, table, column, new_value, natural_key_hash):
    """
    Physically updates the source table in Unity Catalog.
    Targets the exact row using the cryptographic _natural_key_hash.
    """
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()
    
    # Deterministic update using our internal CDC hash
    update_sql = f"""
        UPDATE {catalog}.{schema}.{table}
        SET {column} = '{new_value}'
        WHERE _natural_key_hash = '{natural_key_hash}'
    """
    
    try:
        spark.sql(update_sql)
        return True
    except Exception as e:
        print(f"Remediation Update Failed: {e}")
        return False