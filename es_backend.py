# Standard library
import os

# Packages
from elasticsearch import Elasticsearch


# Connect to Elasticsearch
es_client = Elasticsearch(os.getenv("ELASTICSEARCH_URL", "http://localhost:9200"))
