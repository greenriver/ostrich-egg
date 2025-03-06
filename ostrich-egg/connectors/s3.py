from functools import cached_property
from typing import Literal

import boto3
import duckdb
from jinja2 import Template

from connectors.base import BaseConnector

# refer to https://duckdb.org/docs/configuration/secrets_manager
DEFAULT_S3_SECRET_NAME = '__default_s3'


def key_as_s3_uri(bucket, key) -> str:
    """
    duckdb wants to write to an s3://uri a specific way, but sometimes we pass
    just the bucket and key separately, so this ensures we get the correct duckdb format.
    """
    import re

    if not key.startswith('s3://'):
        key = re.sub(pattern=f'^{bucket}/', string=key, repl='')
        key = f's3://{bucket}/{key}'
    return key


class S3Connector(BaseConnector):

    extensions = (
        'httpfs',
        'aws',
        'spatial',
    )

    def __init__(
        self,
        bucket: str,
        key: str,
        region='us-east-1',
        access_key_id=None,
        secret_access_key=None,
        session_token=None,
        endpoint: str = None,
        use_credential_chain: bool = False,
        use_ssl: bool = True,
        url_style: Literal['vhost', 'path'] = 'vhost',
        chain: str = None,
        *args,
        **kwargs,
    ):
        """
        See https://duckdb.org/docs/extensions/httpfs/s3api
        """
        super().__init__(*args, **kwargs)
        self.bucket = bucket
        self.key = key
        self.region = region
        self.access_key_id = access_key_id
        self.secret_access_key = secret_access_key
        self.session_token = session_token
        self.use_credential_chain = use_credential_chain
        self.chain = chain
        self.endpoint = endpoint
        self.use_ssl = use_ssl
        self.url_style = url_style

    def __exit__(self):
        self.duckdb_connection.close()

    @cached_property
    def duckdb_connection(self):
        return duckdb.connect()

    def init_duckdb(self):
        super().init_duckdb()
        self.create_s3_secret()

    def get_instance_creds(self):
        aws_session = boto3.Session()
        creds = aws_session.get_credentials().get_frozen_credentials()
        self.region = aws_session.region_name or self.region
        return creds

    def create_s3_secret(self):
        # Known issue about EKS web_identity not working out of the box https://github.com/duckdb/duckdb_aws/issues/31
        if not self.use_credential_chain and not self.secret_access_key:
            creds = self.get_instance_creds()
        else:
            creds = None
        context = {
            "use_credential_chain": self.use_credential_chain,
            "use_ssl": 'false' if not self.use_ssl else 'true',
            "creds": creds,
            "params": {
                "key_id": self.access_key_id,
                "secret_access_key": self.secret_access_key,
                "session_token": self.session_token,
                "chain": self.chain,
                "region": self.region,
                "endpoint": self.endpoint,
                "url_style": self.url_style,
            },
        }
        create_secret_template = """
            create secret if not exists __default_s3 (
                type s3
                , use_ssl {{use_ssl}}

                {%- if creds is not none %}
                , key_id '{{creds.access_key}}'
                , secret '{{creds.secret_key}}'
                , session_token '{{creds.token}}'
                {%- endif -%}

                {%- if use_credential_chain is true %}
                , provider credential_chain
                {%- endif %}

                {% set params = params| default({}) %}
                {%- for key, val in params.items()  %}
                    {%- if val is not none %}
                    , {{key}}  '{{val}}'
                    {%- endif -%}
                {%- endfor %}
            )
            """

        create_secret_sql = Template(create_secret_template).render(context)
        self.duckdb_connection.execute(create_secret_sql)

    def drop_secret(self):
        self.duckdb_connection.execute(f"drop secret {DEFAULT_S3_SECRET_NAME}")

    def reset_secret(self):
        self.drop_secret()
        self.create_s3_secret()

    def load_source_table(self, table_name: str = None, bucket: str = None, source_file: str = None, **kwargs):
        """
        Ultimately, the in-memory process needs to read the s3 file.
        So, probably what would wind up an interface so that all connectors `create_table`
        we shall allow our connection to register the s3 file as a table.
        """
        table_name = table_name or self.table_name
        bucket = bucket or self.bucket
        key = source_file or self.key
        key = key_as_s3_uri(bucket=bucket, key=key)
        self.init_duckdb()
        self.duckdb_connection.execute(
            f'''
            create or replace table "{table_name}" as (
                select * from '{key}'
            )
        '''
        )
