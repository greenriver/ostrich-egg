from connectors.base import BaseConnector, DEFAULT_TABLE_NAME  # noqa:F401
from connectors.s3 import S3Connector
from connectors.file_system import FileSystemConnector
from typing import Literal
from enum import Enum


connector_lkp = {"s3": S3Connector, "file": FileSystemConnector}

Supported_Connectors = Enum(
    "SupportedConnectors", {key: key for key in connector_lkp.keys()}
)


def Connector(
    connection_type: Literal["s3", "file"], **parameters: dict
) -> BaseConnector:
    try:
        connector = connector_lkp[connection_type]
    except KeyError:
        raise KeyError(f"Invalid connector type: f{type}")
    return connector(**parameters)
