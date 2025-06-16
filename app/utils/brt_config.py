from dotenv import load_dotenv
load_dotenv()


import os

def get_brt_config():
    return {
        "user_id": os.getenv("BRT_USER_ID"),
        "password": os.getenv("BRT_PASSWORD"),
        "depot": os.getenv("BRT_DEPARTURE_DEPOT"),
        "codice_cliente": os.getenv("BRT_CODICE_CLIENTE"),
        "label_format": os.getenv("BRT_LABEL_FORMAT", "PDF"),
        "api_url": os.getenv("BRT_API_URL"),
    }

if __name__ == "__main__":
    print(get_brt_config())
