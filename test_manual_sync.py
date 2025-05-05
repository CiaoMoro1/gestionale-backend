import requests

# 🔁 URL locale del tuo backend Flask
url = "http://localhost:5000/shopify/manual-sync"

# 🛡️ Token JWT (user_id: a9465064-6f12-492a-8faf-2743219b1094)
token = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJhOTQ2NTA2NC02ZjEyLTQ5MmEtOGZhZi0yNzQzMjE5YjEwOTQiLCJyb2xlIjoidXNlciIsImlhdCI6MTcxNTAwMzUwMCwiZXhwIjoyMDYwMDAwMDAwfQ.P0fQTLKNDVOtxz7hUKcUmtl5wrZn_rMbEvAN9bPMU3g"

headers = {
    "Authorization": f"Bearer {token}",
    "Content-Type": "application/json"
}

# ❗ Body vuoto: se la route lo accetta
response = requests.post(url, json={}, headers=headers)

# 🧾 Output risultato
print("Status:", response.status_code)
print("Response:", response.text)
