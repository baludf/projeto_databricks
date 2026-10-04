#!/usr/bin/env python3
"""
scripts/create-dashboard.py
Cria/atualiza o dashboard Comercial · Rotaperfume via Lakeview API REST.
Usa o DATABRICKS_HOST e DATABRICKS_TOKEN do ambiente.
"""
import os, sys, json, urllib.request, urllib.error

HOST = "https://dbc-ad7f18c2-8e19.cloud.databricks.com"
TOKEN = os.environ.get("DATABRICKS_TOKEN", "")

def api(method, path, body=None):
    url = HOST + path
    data = json.dumps(body).encode() if body else None
    headers = {
        "Authorization": f"Bearer {TOKEN}",
        "Content-Type": "application/json"
    }
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            return json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        return {"error": e.read().decode(), "code": e.code}

# Le o JSON do dashboard
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(os.path.dirname(SCRIPT_DIR))
json_path = os.path.join(PROJECT_ROOT, "resources", "dashboard-comercial.lvdash.json")

with open(json_path, encoding="utf-8") as f:
    dashboard_spec = json.load(f)

serialized = json.dumps(dashboard_spec, ensure_ascii=False)

# Cria o dashboard
result = api("POST", "/api/2.0/lakeview/dashboards", {
    "display_name": dashboard_spec["displayName"],
    "warehouse_id": "dd1ddc0694a86a8e",
    "serialized_dashboard": serialized
})

if "error" in result:
    print(f"ERRO: {result['error']}")
    sys.exit(1)

dashboard_id = result["dashboard_id"]
print(f"Dashboard criado: {dashboard_id}")
print(f"URL: {HOST}/dashboardsv3/{dashboard_id}/published")
