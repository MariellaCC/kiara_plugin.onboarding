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


class RunCcQuery(KiaraModule):
    """Execute a Common Crawl archives indexes for a given query performed via Amazon Web Services (AWS) and Athena.
    This process requires an AWS account and an S3 bucket. It may trigger some fees billed by AWS.
    Additional information on the process followed available at: https://commoncrawl.org/2018/03/index-to-warc-files-and-urls-in-columnar-format/.
    """

    _module_type_name = "onboard.run_cc_query"

    def create_inputs_schema(
        self,
    ) -> ValueMapSchema:

        return {
            "query": {"type": "string", "doc": "The query to run on AWS/Athena."},
            "aws_access_key_id": {"type": "string", "doc": "The AWS access key id."},
            "aws_secret_access_key": {"type": "string", "doc": "The AWS secret access key."},
            "aws_s3_bucket": {"type": "string", "doc": "Name of S3 bucket to store results."},
            "db_name": {"type": "string", "doc": "Name of the database to create."},
            "table_name": {"type": "string", "doc": "Name of the table to create."},
        }

    def create_outputs_schema(
        self,
    ) -> ValueMapSchema:

        return {
            "cc_query_id": {
                "type": "string",
            }
        }


    def process(self, inputs: ValueMap, outputs: ValueMap):

        #TODO test query with result limitation, before running the query
        # check query status before retrieving result

        import boto3

        user_query = inputs.get_value_data("query")
        aws_access_key_id = inputs.get_value_data("aws_access_key_id")
        aws_secret_access_key = inputs.get_value_data("aws_secret_access_key")
        aws_s3_bucket = inputs.get_value_data("aws_s3_bucket")
        dbb_name = inputs.get_value_data("db_name")
        table_name = inputs.get_value_data("table_name")
        table_name = inputs.get_value_data("table_name")

        # AWS region (must be us-east-1 for commoncrawl usage)
        region_name = 'us-east-1'

        # set up Athena with AWS credentials
        try:
            client = boto3.client('athena', region_name=region_name, aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
        except Exception as e:
            print(f"Error setting-up AWS/Athena client: {e}")

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
            print(f"Error creating database on S3 bucket: {e}")

        # create table for database
        try:
            client.start_query_execution(
            QueryString=table_creation_q,
            ResultConfiguration={'OutputLocation': aws_s3_bucket})
        except Exception as e:
            print(f"Error creating table: {e}")
        
        try:
            client.start_query_execution(
            QueryString=table_repair_q,
            ResultConfiguration={'OutputLocation': aws_s3_bucket})
        except Exception as e:
            print(f"Error setting-up database: {e}")
        
        try:
            query_exec = client.start_query_execution(
            QueryString=user_query,
            ResultConfiguration={'OutputLocation': aws_s3_bucket})
        except Exception as e:
            print(f"Error executing query: {e}")
        
        query_id = query_exec['QueryExecutionId']
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/athena/client/get_query_execution.html
        # test if query is still running

        # try:
        #     response = client.get_query_results(QueryExecutionId=query_id)
        # except Exception as e:
        #     print(f"Error returning query results: {e}")
        
        outputs.set_value("cc_query_id", query_id)




class GetCcQueryStatus(KiaraModule):
    """Get the status of a Common Crawl archives indexes query.
    """

    _module_type_name = "onboard.get_cc_query_status"

    def create_inputs_schema(
        self,
    ) -> ValueMapSchema:

        return {
            "cc_query_id": {"type": "string", "doc": "AWS/Athena query id."},
            "aws_access_key_id": {"type": "string", "doc": "The AWS access key id."},
            "aws_secret_access_key": {"type": "string", "doc": "The AWS secret access key."},
        }

    def create_outputs_schema(
        self,
    ) -> ValueMapSchema:

        return {
            "cc_query_status": {
                "type": "dict",
            }
        }


    def process(self, inputs: ValueMap, outputs: ValueMap):

        import boto3

        cc_query_id = inputs.get_value_data("cc_query_id")
        aws_access_key_id = inputs.get_value_data("aws_access_key_id")
        aws_secret_access_key = inputs.get_value_data("aws_secret_access_key")

        # AWS region (must be us-east-1 for commoncrawl usage)
        region_name = 'us-east-1'

        # set up Athena with AWS credentials
        try:
            client = boto3.client('athena', region_name=region_name, aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
        except Exception as e:
            print(f"Error setting-up AWS/Athena client: {e}")

        # get query status
        try:
            response = client.get_query_execution(QueryExecutionId=cc_query_id)
        except Exception as e:
            print(f"Error returning query execution: {e}")
        
        outputs.set_value("cc_query_status", response)


class GetCcQueryResult(KiaraModule):
    """Get the result of a Common Crawl archives indexes query.
    """

    _module_type_name = "onboard.get_cc_query_result"

    def create_inputs_schema(
        self,
    ) -> ValueMapSchema:

        return {
            "cc_query_id": {"type": "string", "doc": "AWS/Athena query id."},
            "aws_access_key_id": {"type": "string", "doc": "The AWS access key id."},
            "aws_secret_access_key": {"type": "string", "doc": "The AWS secret access key."},
        }

    def create_outputs_schema(
        self,
    ) -> ValueMapSchema:

        return {
            "cc_query_result": {
                "type": "dict",
            }
        }


    def process(self, inputs: ValueMap, outputs: ValueMap):

        import boto3

        cc_query_id = inputs.get_value_data("cc_query_id")
        aws_access_key_id = inputs.get_value_data("aws_access_key_id")
        aws_secret_access_key = inputs.get_value_data("aws_secret_access_key")

        # AWS region (must be us-east-1 for commoncrawl usage)
        region_name = 'us-east-1'

        # set up Athena with AWS credentials
        try:
            client = boto3.client('athena', region_name=region_name, aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
        except Exception as e:
            print(f"Error setting-up AWS/Athena client: {e}")

        # get query status
        try:
            response = client.get_query_results(QueryExecutionId=cc_query_id)
        except Exception as e:
            print(f"Error returning query execution: {e}")
        
        outputs.set_value("cc_query_result", response)


# Reprendre ici
class GetCcPages(KiaraModule):
    """Get the result of a Common Crawl archives indexes query.
    """

    _module_type_name = "onboard.get_cc_pages"

    def create_inputs_schema(
        self,
    ) -> ValueMapSchema:

        return {
            "cc_query_result": {"type": "dict", "doc": "Commoncrewl archive indexes obtained via an AWS/Athena query."},
        }

    def create_outputs_schema(
        self,
    ) -> ValueMapSchema:

        return {
            "cc_query_pages": {
                "type": "table",
            }
        }


    def process(self, inputs: ValueMap, outputs: ValueMap):

        import httpx
        import gzip
        import pyarrow as pa

        cc_query_result = inputs.get_value_data("cc_query_result")

        cc_url = 'https://data.commoncrawl.org/'

        warc_filenames = []
        web_pages = []
        status = []
        

        for row in cc_query_result['ResultSet']['Rows']:
            for key, value in row.items():
                
                warc_filename = value[23]
                warc_record_offset = value[24]
                warc_record_length = value[25]

                start = warc_record_offset
                end = start + warc_record_length - 1
                headers = {"Range": f"bytes={start}-{end}"}

                warc_filenames.append(warc_filename)

                full_url =f"{cc_url}{warc_filename}"

                try:
                    # Send the HTTP request
                    with httpx.Client() as client:
                        r = client.get(full_url, headers=headers)

                        # Decompress the gzipped data
                        decompressed_data = gzip.decompress(r.content)
                        web_pages.append(decompressed_data)
                        status.append('success')
                
                except Exception as e:

                    status.append(e)
            

        cc_filename = pa.array(warc_filenames)
        web_page = pa.array(web_pages)
        get_status = pa.array(status)
        cols = ['cc_filename', 'web_page', 'status']
        res = pa.table([cc_filename, web_page, get_status], names=cols)
        
        outputs.set_value("cc_query_pages", res)