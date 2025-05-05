import jwt
import time

JWT_SECRET = "Ky/ILW1ry9iXCJf7hWsuioBuKlk9rnqMI7/7WMOZnxvATpYGSoIV1F3m+AAyMQn5sZy0vsAOsLZIyMBgk7WpEQ=="
user_id = "a9465064-6f12-492a-8faf-2743219b1094"

payload = {
    "sub": user_id,
    "role": "user",
    "iat": int(time.time()),
    "exp": int(time.time()) + 60 * 60 * 24 * 365  # valido 1 anno
}

token = jwt.encode(payload, JWT_SECRET, algorithm="HS256")
print("🔐 JWT generato:\n", token)
