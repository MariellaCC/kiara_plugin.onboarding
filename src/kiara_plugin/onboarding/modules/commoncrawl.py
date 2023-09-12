# -*- coding: utf-8 -*-
import hashlib
import shutil
from pathlib import Path
from typing import Any, Mapping

import orjson
from pydantic import Field

from kiara.api import KiaraModule, KiaraModuleConfig, ValueMap, ValueMapSchema
from kiara.exceptions import KiaraProcessingException
from kiara.models.filesystem import KiaraFileBundle


class GetCommoncrawlIndexes(KiaraModule):
    """Get Common Crawl archives indexes for a given query performed via Amazon Web Services (AWS) and Athena.
    This process requires an AWS account and an S3 bucket. It may trigger some fees billed by AWS.
    Additional information on the process followed available at: https://commoncrawl.org/2018/03/index-to-warc-files-and-urls-in-columnar-format/.
    """

    _module_type_name = "onboard.get_cc_indexes"

    def create_inputs_schema(
        self,
    ) -> ValueMapSchema:

        return {
            "query": {"type": "string", "doc": "The query to run on AWS/Athena."},
            "aws_access_key_id": {"type": "string", "doc": "The AWS access key id."},
            "aws_secret_access_key": {"type": "string", "doc": "The AWS secret access key."},
            "aws_s3_bucket": {"type": "string", "doc": "Name of S3 bucket to store results."},
            "dbb_name": {"type": "string", "doc": "Name of the database to create."},
            "table_name": {"type": "string", "doc": "Name of the table to create."},
        }

    def create_outputs_schema(
        self,
    ) -> ValueMapSchema:

        return {
            "cc_indexes": {
                "type": "dict",
            }
        }


    def process(self, inputs: ValueMap, outputs: ValueMap):

        import boto3

        user_query = inputs.get_value_data("query")
        aws_access_key_id = inputs.get_value_data("aws_access_key_id")
        aws_secret_access_key = inputs.get_value_data("aws_secret_access_key")
        aws_s3_bucket = inputs.get_value_data("aws_s3_bucket")
        dbb_name = inputs.get_value_data("dbb_name")
        table_name = inputs.get_value_data("table_name")
        table_name = inputs.get_value_data("table_name")

        # AWS region (must be us-east-1 for commoncrawl usage)
        region_name = 'us-east-1'

        # set up Athena with AWS credentials
        try:
            client = boto3.client('athena', region_name=region_name, aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
        except Exception as e:
            return f"Error setting-up AWS/Athena client: {e}"

        # query to create table with columns needed for commoncrawl info retrieval as per tutorial: https://commoncrawl.org/2018/03/index-to-warc-files-and-urls-in-columnar-format/
        table_creation_q = f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS {dbb_name}.{table_name} ( url_surtkey STRING, url STRING, url_host_name STRING, url_host_tld STRING, url_host_2nd_last_part STRING, url_host_3rd_last_part STRING, url_host_4th_last_part STRING, url_host_5th_last_part STRING, url_host_registry_suffix STRING, url_host_registered_domain STRING, url_host_private_suffix STRING, url_host_private_domain STRING, url_protocol STRING, url_port INT, url_path STRING, url_query STRING, fetch_time TIMESTAMP, fetch_status SMALLINT, content_digest STRING, content_mime_type STRING, content_mime_detected STRING, content_charset STRING, content_languages STRING, warc_filename STRING, warc_record_offset INT, warc_record_length INT, warc_segment STRING)
        PARTITIONED BY ( crawl STRING, subset STRING)
        STORED AS parquet
        LOCATION 's3://commoncrawl/cc-index/table/cc-main/warc/'
        """

        # step needed as per commoncrawl tutorial mentioned above
        table_repair_q = f'MSCK REPAIR TABLE {dbb_name}.{table_name}'

        # create AWS database on user's S3 bucket
        try:
            client.start_query_execution(
            QueryString=f'CREATE DATABASE {dbb_name}',
            ResultConfiguration={'OutputLocation': aws_s3_bucket})
        except Exception as e:
            return f"Error creating database on S3 bucket: {e}"

        # create table for database
        try:
            client.start_query_execution(
            QueryString=table_creation_q,
            ResultConfiguration={'OutputLocation': aws_s3_bucket})
        except Exception as e:
            return f"Error creating table: {e}"
        
        try:
            client.start_query_execution(
            QueryString=table_repair_q,
            ResultConfiguration={'OutputLocation': aws_s3_bucket})
        except Exception as e:
            return f"Error setting-up database: {e}"
        
        try:
            query_exec = client.start_query_execution(
            QueryString=user_query,
            ResultConfiguration={'OutputLocation': aws_s3_bucket})
        except Exception as e:
            return f"Error executing query: {e}"
        
        query_id = query_exec['QueryExecutionId']

        try:
            response = client.get_query_results(QueryExecutionId=query_id)
        except Exception as e:
            return f"Error returning query results: {e}"
        
        outputs.set_value("cc_indexes", response)