import requests
from requests.auth import HTTPDigestAuth
import time
import os
import logging
from dotenv import load_dotenv

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("atlas_scaler.log"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger()

load_dotenv()

# Atlas API Configuration
BASE_URL = os.getenv("ATLAS_BASE_URL")
PROJECT_ID = os.getenv("ATLAS_PROJECT_ID")
CLUSTER_NAME = os.getenv("ATLAS_CLUSTER_NAME")
PUBLIC_KEY = os.getenv("ATLAS_PUBLIC_KEY")
PRIVATE_KEY = os.getenv("ATLAS_PRIVATE_KEY")
SCALE_FROM = os.getenv("SCALE_FROM", "M10")
SCALE_TO = os.getenv("SCALE_TO", "M20")
SLEEP_INTERVAL = int(os.getenv("SLEEP_INTERVAL", 300))

# Wait file configuration
WAIT_FILE_PATH = os.getenv("WAIT_FILE_PATH")

# Create a session with Digest Authentication
session = requests.Session()
session.auth = HTTPDigestAuth(PUBLIC_KEY, PRIVATE_KEY)
session.headers.update({"Accept": "application/vnd.atlas.2024-08-05+json"})


def wait_for_load_completion():
    if WAIT_FILE_PATH:
        logger.info(f"Waiting for load completion file: {WAIT_FILE_PATH}")
        while not os.path.exists(WAIT_FILE_PATH):
            logger.info("Load completion file not found. Waiting...")
            time.sleep(60)  # Check every minute
        logger.info("Load completion file found. Proceeding with scaling.")
    else:
        logger.info("No wait file specified. Proceeding immediately.")


def get_current_cluster_config():
    url = f"{BASE_URL}/api/atlas/v2/groups/{PROJECT_ID}/clusters/{CLUSTER_NAME}"
    response = session.get(url)
    response.raise_for_status()
    return response.json()


def update_cluster_size(new_size):
    url = f"{BASE_URL}/api/atlas/v2/groups/{PROJECT_ID}/clusters/{CLUSTER_NAME}"

    # Get the current configuration
    current_config = get_current_cluster_config()

    # Prepare the update payload
    payload = {}

    # Update the instance size in all regionConfigs
    for spec in current_config.get("replicationSpecs", []):
        for region in spec.get("regionConfigs", []):
            if "electableSpecs" in region:
                region["electableSpecs"]["instanceSize"] = new_size
            if "readOnlySpecs" in region:
                region["readOnlySpecs"]["instanceSize"] = new_size
            if "analyticsSpecs" in region:
                region["analyticsSpecs"]["instanceSize"] = new_size

    payload["replicationSpecs"] = current_config["replicationSpecs"]

    response = session.patch(url, json=payload)
    response.raise_for_status()
    logger.info(f"Cluster size update initiated: {new_size}")


def get_instance_size(config):
    # Navigate through the config to find the instance size
    for spec in config.get("replicationSpecs", []):
        for config in spec.get("regionConfigs", []):
            if "electableSpecs" in config:
                return config["electableSpecs"]["instanceSize"]
    return None


def wait_for_cluster_update():
    while True:
        config = get_current_cluster_config()
        if config["stateName"] == "IDLE":
            print("Cluster update completed")
            break
        logger.info("Waiting for cluster update to complete...")
        time.sleep(60)


def main():
    logger.info("Starting cluster scaling process")

    # Wait for load completion
    wait_for_load_completion()

    while True:
        try:
            current_config = get_current_cluster_config()
            current_size = get_instance_size(current_config)

            if current_size is None:
                print("Unable to determine current instance size")
                time.sleep(SLEEP_INTERVAL)
                continue

            # Toggle between M10 and M20
            new_size = SCALE_TO if current_size == SCALE_FROM else SCALE_FROM

            logger.info(f"Current size: {current_size}")
            logger.info(f"Scaling to: {new_size}")

            update_cluster_size(new_size)
            wait_for_cluster_update()

            logger.info(
                f"Waiting for {SLEEP_INTERVAL / 60} minutes before next scaling operation..."
            )
            time.sleep(SLEEP_INTERVAL)
        except Exception as e:
            logger.info(f"An error occurred: {e}")
            logger.info(f"Waiting for {SLEEP_INTERVAL / 60} minutes before retrying...")
            time.sleep(SLEEP_INTERVAL)


if __name__ == "__main__":
    main()
