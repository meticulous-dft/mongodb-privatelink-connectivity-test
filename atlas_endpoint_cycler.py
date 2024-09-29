import requests
from requests.auth import HTTPDigestAuth
import time
import os
from dotenv import load_dotenv

load_dotenv()

# Atlas API Configuration
BASE_URL = os.getenv("ATLAS_BASE_URL")
PROJECT_ID = os.getenv("ATLAS_PROJECT_ID")
CLUSTER_NAME = os.getenv("ATLAS_CLUSTER_NAME")
PUBLIC_KEY = os.getenv("ATLAS_PUBLIC_KEY")
PRIVATE_KEY = os.getenv("ATLAS_PRIVATE_KEY")
VPCE_IDS = os.getenv("VPCE_IDS", "").split(",")

# Create a session with Digest Authentication
session = requests.Session()
session.auth = HTTPDigestAuth(PUBLIC_KEY, PRIVATE_KEY)
session.headers.update({"Accept": "application/vnd.atlas.2024-08-05+json"})


def get_endpoint_service_id():
    url = f"{BASE_URL}/api/atlas/v2/groups/{PROJECT_ID}/privateEndpoint/AWS/endpointService"
    response = session.get(url)
    response.raise_for_status()
    return response.json()[0]["id"]


def get_endpoint(endpoint_service_id, vpce_id):
    url = f"{BASE_URL}/api/atlas/v2/groups/{PROJECT_ID}/privateEndpoint/AWS/endpointService/{endpoint_service_id}/endpoint/{vpce_id}"

    try:
        response = session.get(url)
        return response.json()
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 404:
            return None
        else:
            raise e


def delete_endpoint(endpoint_service_id, vpce_id):
    url = f"{BASE_URL}/api/atlas/v2/groups/{PROJECT_ID}/privateEndpoint/AWS/endpointService/{endpoint_service_id}/endpoint/{vpce_id}"
    response = session.delete(url)
    response.raise_for_status()
    print(f"Deleted endpoint {vpce_id}")


def create_private_endpoint(endpoint_service_id, vpce_id):
    url = f"{BASE_URL}/api/atlas/v2/groups/{PROJECT_ID}/privateEndpoint/AWS/endpointService/{endpoint_service_id}/endpoint"
    payload = {
        "id": vpce_id,
    }
    response = session.post(url, json=payload)
    response.raise_for_status()
    print(f"Created private endpoint {vpce_id}")
    return response.json()


def cycle_private_endpoints():
    endpoint_service_id = get_endpoint_service_id()
    print(f"Using Endpoint Service ID: {endpoint_service_id}")

    while True:
        for vpce_id in VPCE_IDS:
            endpoint = get_endpoint(endpoint_service_id, vpce_id)
            if endpoint:
                print(f"Deleting Private Endpoint {vpce_id}...")
                delete_endpoint(endpoint_service_id, vpce_id)
                while True:
                    endpoint = get_endpoint(endpoint_service_id, vpce_id)
                    if endpoint:
                        time.sleep(60)

            print(f"Recreating Private Endpoint {vpce_id}...")
            create_private_endpoint(endpoint_service_id, vpce_id)
            while True:
                endpoint = get_endpoint(endpoint_service_id, vpce_id)
                if endpoint and endpoint["status"] == "AVAILABLE":
                    break
                time.sleep(60)

            # Wait before next cycle
            print("Waiting for 5 minutes before next cycle...")
            time.sleep(300)  # 5 minutes


if __name__ == "__main__":
    cycle_private_endpoints()
