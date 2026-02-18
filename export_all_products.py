import os
import csv
import requests
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.environ["SHIPSTATION_API_KEY"]
API_SECRET = os.environ["SHIPSTATION_API_SECRET"]

URL = "https://ssapi.shipstation.com/products"

page = 1
page_size = 500

rows = []

while True:
    params = {
        "page": page,
        "pageSize": page_size
    }

    print(f"Fetching products page {page}")

    r = requests.get(URL, params=params, auth=(API_KEY, API_SECRET))
    r.raise_for_status()

    data = r.json()
    products = data.get("products", [])

    if not products:
        break

    for p in products:
        rows.append({
            "productId": p.get("productId"),
            "sku_master": p.get("sku"),
            "fulfillmentSku": p.get("fulfillmentSku"),
            "warehouseLocation": p.get("warehouseLocation"),
            "name": p.get("name"),
            "active": p.get("active"),
        })

    if len(products) < page_size:
        break

    page += 1

# guardar csv
with open("shipstation_all_products.csv", "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=rows[0].keys())
    writer.writeheader()
    writer.writerows(rows)

print(f"Exportados {len(rows)} productos")
