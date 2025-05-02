import os
import requests
import certifi

SHOPIFY_ACCESS_TOKEN = os.environ.get("SHOPIFY_ACCESS_TOKEN")
SHOPIFY_GRAPHQL_URL = os.environ.get("SHOPIFY_GRAPHQL_URL")

def fetch_all_products():
    query = """
    {
      products(first: 100000) {
        edges {
          node {
            id
            title
            variants(first: 50) {
              edges {
                node {
                  id
                  sku
                  inventoryQuantity
                }
              }
            }
          }
        }
      }
    }
    """

    headers = {
        "X-Shopify-Access-Token": SHOPIFY_ACCESS_TOKEN,
        "Content-Type": "application/json"
    }

    # ⛔ verify=False solo in sviluppo!
    res = requests.post(
        SHOPIFY_GRAPHQL_URL,
        json={"query": query},
        headers=headers,
        verify=False  # oppure: verify=certifi.where()
    )

    if res.status_code != 200:
        raise Exception(f"Errore Shopify: {res.status_code} - {res.text}")
  
    print("📦 Risposta Shopify:", res.text)  # 👈 AGGIUNGI QUESTA RIGA


    try:
        edges = res.json()["data"]["products"]["edges"]
    except Exception as e:
        raise Exception("Errore nel parsing JSON di Shopify: " + str(e))

    products = []

    for p in edges:
        node = p.get("node")
        if not node:
            continue

        variants = []
        for v in node.get("variants", {}).get("edges", []):
            if not v or not v.get("node"):
                continue
            variant_node = v["node"]
            variants.append({
                "id": variant_node["id"],
                "sku": variant_node.get("sku"),
                "inventory_quantity": variant_node.get("inventoryQuantity", 0)
            })

        products.append({
            "id": node["id"],
            "title": node["title"],
            "variants": variants
        })

    return products
