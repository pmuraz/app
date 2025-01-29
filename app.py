from fastapi import FastAPI
import requests

app = FastAPI()

DATABRICKS_URL = "https://dbc-1e51cb59-cc0e.cloud.databricks.com"
DATABRICKS_TOKEN = "dapi6cd66111aa25416a2d5dca8676e688d8"
WAREHOUSE_ID = "a7459af7d821a884"

@app.get("/odata/test_value")
def get_data():
    headers = {"Authorization": f"Bearer {DATABRICKS_TOKEN}", "Content-Type": "application/json"}
    payload = {
        "statement": "SELECT 1 AS test_value",
        "warehouse_id": WAREHOUSE_ID
    }
    
    # Execute Databricks Query
    response = requests.post(f"{DATABRICKS_URL}/api/2.0/sql/statements", json=payload, headers=headers)
    result = response.json()
    
    # Extract query result
    if "result" in result:
        data = result["result"]["data_array"]
        odata_response = {
            "@odata.context": "https://your-api-url/odata/$metadata#test_value",
            "value": [{"test_value": row[0]} for row in data]
        }
        return odata_response
    else:
        return {"error": "Failed to fetch data"}

