import requests
from requests.auth import HTTPDigestAuth
import time
import boto3
import os
from dotenv import load_dotenv

load_dotenv()

# Atlas API Configuration
BASE_URL = os.getenv("ATLAS_BASE_URL")
PROJECT_ID = os.getenv("ATLAS_PROJECT_ID")
CLUSTER_NAME = os.getenv("ATLAS_CLUSTER_NAME")
PUBLIC_KEY = os.getenv("ATLAS_PUBLIC_KEY")
PRIVATE_KEY = os.getenv("ATLAS_PRIVATE_KEY")
VPC_IDS = os.getenv("VPC_IDS", "").split(",")
SUBNET_IDS = os.getenv("SUBNET_IDS", "").split(",")
SECURITY_GROUP_IDS = os.getenv("SECURITY_GROUP_IDS", "").split(",")
# AWS Configuration
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")

# Create a session with Digest Authentication
session = requests.Session()
session.auth = HTTPDigestAuth(PUBLIC_KEY, PRIVATE_KEY)
session.headers.update({"Accept": "application/vnd.atlas.2024-08-05+json"})

# Initialize AWS client
ec2_client = boto3.client(
    "ec2",
    region_name=AWS_REGION,
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
)


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
    print(f"Deleted AWS VPC endpoint {vpc_endpoint_id}")


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
    print(f"Created AWS VPC endpoint {vpc_endpoint_id}")
    return vpc_endpoint_id


def cycle_private_endpoints():
    endpoint_service_id = get_endpoint_service_id()
    print(f"Using Endpoint Service ID: {endpoint_service_id}")
    endpoint_service_name = get_endpoint_service_name(endpoint_service_id)

    while True:
        for vpc_id in VPC_IDS:
            vpce_id = get_vpc_endpoint_id(vpc_id, endpoint_service_name)
            if vpce_id:
                endpoint = get_endpoint(endpoint_service_id, vpce_id)
                print(f"Deleting Private Endpoint {vpce_id}...")
                delete_endpoint(endpoint_service_id, vpce_id)
                while True:
                    endpoint = get_endpoint(endpoint_service_id, vpce_id)
                    if not endpoint:
                        break
                    time.sleep(30)
                delete_aws_vpc_endpoint(vpce_id)

        time.sleep(60)
        for i, vpc_id in enumerate(VPC_IDS):
            print(f"Recreating Private Endpoint in VPC {vpc_id}...")
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
        print("Waiting for 5 minutes before next cycle...")
        time.sleep(300)  # 5 minutes


if __name__ == "__main__":
    cycle_private_endpoints()
