# Databricks notebook source
# MAGIC %md
# MAGIC # CMS Publisher – POST wait_times JSON to USPTO API Gateway → Drupal
# MAGIC Databricks Secret Scope `uspto-api` holds client_id / client_secret

# COMMAND ----------
dbutils.widgets.text("publish_bucket", "s3://bdr-trm-publish/wait_times")
dbutils.widgets.text("api_gateway_url", "https://api.uspto.gov/trademarks/v1/wait-times")
publish_bucket = dbutils.widgets.get("publish_bucket")
api_url = dbutils.widgets.get("api_gateway_url")

import json, time, requests

# Read latest JSON from S3
latest_path = f"{publish_bucket}/wait_times_latest.json"
json_text = spark.read.text(latest_path).collect()[0]["value"]
# dbutils.fs.head is simpler in Databricks
try:
    json_text = dbutils.fs.head(latest_path)
except:
    pass

payload = json.loads(json_text)

# OAuth2 Client Credentials – secrets in Databricks Secret Scope
try:
    client_id = dbutils.secrets.get(scope="uspto-api", key="client_id")
    client_secret = dbutils.secrets.get(scope="uspto-api", key="client_secret")
    token_url = dbutils.secrets.get(scope="uspto-api", key="token_url")
except Exception as e:
    print(f"WARNING: Secret scope not configured in dev – skipping live POST: {e}")
    client_id = None

if client_id:
    # Get token
    tok = requests.post(token_url, data={
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
        "scope": "trademarks:write"
    }, timeout=15)
    tok.raise_for_status()
    access_token = tok.json()["access_token"]

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
        "X-Source-System": "trm-dpl",
        "X-Data-Updated": payload["data_updated"]
    }

    # POST with retries
    for attempt in range(1, 4):
        r = requests.post(api_url, json=payload, headers=headers, timeout=30)
        if r.status_code in (200, 201, 202):
            print(f"CMS publish SUCCESS – attempt {attempt}: {r.status_code} {r.text[:200]}")
            break
        print(f"Attempt {attempt} failed {r.status_code}: {r.text[:300]}")
        if attempt == 3:
            # DLQ
            dlq_path = f"{publish_bucket}/dlq/wait_times_{payload['snapshot_date']}.json"
            dbutils.fs.put(dlq_path, json.dumps(payload), overwrite=True)
            raise Exception(f"CMS publish failed after 3 attempts: {r.status_code} {r.text}")
        time.sleep(2 ** attempt)
else:
    print("DEV MODE – would POST to:", api_url)
    print(json.dumps(payload, indent=2)[:1200])
    print("\n--- CMS publish skipped (no secrets) – set up uspto-api secret scope in Prod ---")

# COMMAND ----------
print("CMS publisher complete. Drupal node /trademarks/application-timeline will be updated to 'Needs Review', then auto-published by OCIO Web Team if QA passed.")
