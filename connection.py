from elasticsearch import Elasticsearch
import os


"""
# For Elastic Cloud Demos
online_client = Elasticsearch(
    cloud_id=os.environ.get("CLOUD_ID", None),
    http_auth=(
        os.environ.get("ES_CLIENT_USERNAME", None),
        os.environ.get("ES_CLIENT_PASSWORD", None),
    ),
)
"""

local_client = Elasticsearch(
        hosts=[os.environ.get('ES_HOST')],
        http_auth=(
        os.environ.get("ES_CLIENT_USERNAME"),
        os.environ.get("ES_PWD"),
        ),
    )
