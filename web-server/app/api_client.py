import httpx
from .config import API_SERVER_URL


def get_all_bought_items(user_id: str):
    url = f"{API_SERVER_URL}/purchases"
    with httpx.Client(timeout=10.0) as client:
        r = client.get(url, params={"userId": user_id})
        r.raise_for_status()
        return r.json()
