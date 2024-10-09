import requests
from requests.auth import HTTPDigestAuth
import time
import boto3
import os
import logging
from dotenv import load_dotenv

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("atlas_endpoint_cycler.log"),
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
# AWS Configuration
VPC_IDS = os.getenv("VPC_IDS", "").split(",")
SUBNET_IDS = os.getenv("SUBNET_IDS", "").split(",")
SECURITY_GROUP_IDS = os.getenv("SECURITY_GROUP_IDS", "").split(",")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")

# Wait file configuration
WAIT_FILE_PATH = os.getenv("WAIT_FILE_PATH")

# Create a session with Digest Authentication
session = requests.Session()
session.auth = HTTPDigestAuth(PUBLIC_KEY, PRIVATE_KEY)
session.headers.update({"Accept": "application/vnd.atlas.2024-08-05+json"})

# Initialize AWS client
ec2_client = boto3.client(
    "ec2",
    region_name=AWS_REGION,
)


def wait_for_load_completion():
    if WAIT_FILE_PATH:
        logger.info(f"Waiting for load completion file: {WAIT_FILE_PATH}")
        while not os.path.exists(WAIT_FILE_PATH):
            logger.info("Load completion file not found. Waiting...")
            time.sleep(60)  # Check every minute
        logger.info("Load completion file found. Proceeding with scaling.")
    else:
        logger.info("No wait file specified. Proceeding immediately.")


def get_endpoint_service_id():
    url = f"{BASE_URL}/api/atlas/v2/groups/{PROJECT_ID}/privateEndpoint/AWS/endpointService"
    response = session.get(url)
    response.raise_for_status()
    return response.json()[0]["id"]


def get_endpoint_service_name(endpoint_service_id):
    url = f"{BASE_URL}/api/atlas/v2/groups/{PROJECT_ID}/privateEndpoint/AWS/endpointService/{endpoint_service_id}"
    response = session.get(url)
    response.raise_for_status()
    return response.json()["endpointServiceName"]


def get_endpoint(endpoint_service_id, vpce_id):
    url = f"{BASE_URL}/api/atlas/v2/groups/{PROJECT_ID}/privateEndpoint/AWS/endpointService/{endpoint_service_id}/endpoint/{vpce_id}"
    try:
        response = session.get(url)
        response.raise_for_status()
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
    logger.info(f"Deleted private endpoint {vpce_id}")


def create_private_endpoint(endpoint_service_id, vpce_id):
    url = f"{BASE_URL}/api/atlas/v2/groups/{PROJECT_ID}/privateEndpoint/AWS/endpointService/{endpoint_service_id}/endpoint"
    payload = {
        "id": vpce_id,
    }
    response = session.post(url, json=payload)
    response.raise_for_status()
    logger.info(f"Created private endpoint {vpce_id}")
    return response.json()


def get_vpc_endpoint_id(vpc_id, service_name):
    response = ec2_client.describe_vpc_endpoints(
        Filters=[
            {"Name": "vpc-id", "Values": [vpc_id]},
            {"Name": "service-name", "Values": [service_name]},
        ]
    )
    endpoints = response["VpcEndpoints"]
    if endpoints:
        return endpoints[0]["VpcEndpointId"]
    return None


def delete_aws_vpc_endpoint(vpc_endpoint_id):
    ec2_client.delete_vpc_endpoints(VpcEndpointIds=[vpc_endpoint_id])
    logger.info(f"Deleted AWS VPC endpoint {vpc_endpoint_id}")


def create_aws_vpc_endpoint(
    vpc_id, subnet_id, security_group_id, atlas_endpoint_service_name
):
    response = ec2_client.create_vpc_endpoint(
        VpcEndpointType="Interface",
        VpcId=vpc_id,
        ServiceName=atlas_endpoint_service_name,
        SubnetIds=[subnet_id],
        SecurityGroupIds=[security_group_id],
    )
    vpc_endpoint_id = response["VpcEndpoint"]["VpcEndpointId"]
    logger.info(f"Created AWS VPC endpoint {vpc_endpoint_id}")
    return vpc_endpoint_id


def cycle_private_endpoints():
    logger.info("Starting private endpoint cycling process")

    # Wait for load completion if WAIT_FILE_PATH is set
    wait_for_load_completion()

    endpoint_service_id = get_endpoint_service_id()
    print(f"Using Endpoint Service ID: {endpoint_service_id}")
    endpoint_service_name = get_endpoint_service_name(endpoint_service_id)

    while True:
        for vpc_id in VPC_IDS:
            vpce_id = get_vpc_endpoint_id(vpc_id, endpoint_service_name)
            if vpce_id:
                logger.info(f"Deleting Private Endpoint {vpce_id}...")
                delete_endpoint(endpoint_service_id, vpce_id)
                while True:
                    endpoint = get_endpoint(endpoint_service_id, vpce_id)
                    if not endpoint:
                        break
                    time.sleep(30)
                delete_aws_vpc_endpoint(vpce_id)

        time.sleep(120)
        for i, vpc_id in enumerate(VPC_IDS):
            logger.info(f"Recreating Private Endpoint in VPC {vpc_id}...")
            vpce_id = create_aws_vpc_endpoint(
                vpc_id, SUBNET_IDS[i], SECURITY_GROUP_IDS[i], endpoint_service_name
            )
            create_private_endpoint(endpoint_service_id, vpce_id)
            while True:
                endpoint = get_endpoint(endpoint_service_id, vpce_id)
                if endpoint and endpoint["connectionStatus"] == "AVAILABLE":
                    break
                time.sleep(30)

        # Wait before next cycle
        logger.info("Waiting for 5 minutes before next cycle...")
        time.sleep(300)  # 5 minutes


if __name__ == "__main__":
    cycle_private_endpoints()
