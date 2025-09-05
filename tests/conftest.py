import os
import sys

import boto3
from moto import mock_aws
from moto.server import ThreadedMotoServer
import pytest


TEST_DIRECTORY = os.path.dirname(__file__)

SRC_DIRECTORY = os.path.join(os.path.dirname(TEST_DIRECTORY), "ostrich_egg")
ROOT_DIR = os.path.dirname(SRC_DIRECTORY)

sys.path.insert(0, ROOT_DIR)
sys.path.insert(1, SRC_DIRECTORY)
sys.path.insert(2, TEST_DIRECTORY)


DATA_INPUTS_DIRECTORY = os.path.join(TEST_DIRECTORY, "data_inputs")
DATA_OUTPUTS_DIRECTORY = os.path.join(TEST_DIRECTORY, "data_outputs")

MOCK_ENDPOINT = "test"
TEST_S3_PARAMS = {
    "use_ssl": False,
    "url_style": "path",
    "use_credential_chain": True,
}


os.environ["MOTO_S3_CUSTOM_ENDPOINTS"] = f"http://{MOCK_ENDPOINT}"
os.environ["S3_IGNORE_SUBDOMAIN_BUCKETNAME"] = "true"


@pytest.fixture()
def mocked_s3_res(moto_server, monkeypatch):
    monkeypatch.delenv("AWS_ENDPOINT_URL", raising=False)
    monkeypatch.delenv("AIRFLOW_CONN_AWS_DEFAULT", raising=False)
    with mock_aws():
        yield boto3.resource("s3", endpoint_url=moto_server)


@pytest.fixture()
def mocked_s3_client(moto_server):
    with mock_aws():
        yield boto3.client("s3", endpoint_url=moto_server)


@pytest.fixture
def moto_server():
    server = ThreadedMotoServer(port=0)
    server.start()
    host, port = server.get_host_and_port()
    yield f"http://{host}:{port}"
    server.stop()
