# Standard library
import os
import unittest

# Packages
from elasticsearch import Elasticsearch
from haystack.preview.document_stores.errors import DuplicateDocumentError
from haystack.preview.document_stores.protocols import DuplicatePolicy
from haystack.preview.dataclasses.document import Document
import pandas as pd

# Local
from elasticsearch_haystack.documentstore import ElasticsearchStore


NUM_MOVIES = 3000
INDEX_NAME = "movies"


class TestElasticsearchStore(unittest.TestCase):
    def setUp(self):
        """
        Create datastore object, make sure the elasticsearch is empty
        """

        # Connect to Elasticsearch
        self._es_client = Elasticsearch(
            os.getenv("ELASTICSEARCH_URL", "http://localhost:9200")
        )

        # Create Store object
        self._es_store = ElasticsearchStore(
            es_client=self._es_client, index_name=INDEX_NAME
        )

        # Empty elasticsearch
        self._es_client.delete_by_query(index=INDEX_NAME, query={"match_all": {}})

        # Get the data
        self._movies_dataframe = (
            pd.read_csv("data/wiki_movie_plots_deduped.csv")
            .dropna()
            .sample(NUM_MOVIES)
            .reset_index()
        )

    def test_write_documents(self) -> None:
        """
        Write many documents using "write documents", check all were inserted
        """

        # Create Haystack documents for insertion
        documents = []

        for index, row in self._movies_dataframe.iterrows():
            # We need to add a string representation of the entire row to "content",
            # because the ID hash is calculated based on the "content"
            documents.append(
                Document.from_dict(
                    {
                        "id": index,
                        "content": str(row),
                        "metadata": {
                            "title": row["Title"],
                            "ethnicity": row["Origin/Ethnicity"],
                            "director": row["Director"],
                            "cast": row["Cast"],
                            "genre": row["Genre"],
                            "plot": row["Plot"],
                            "year": row["Release Year"],
                            "wiki_page": row["Wiki Page"],
                        },
                    }
                )
            )

        # Write the documents using the Store object
        self._es_store.write_documents(documents)

        # Check all were inserted
        count = int(
            self._es_client.cat.count(index=INDEX_NAME, format="json")[0]["count"]
        )
        self.assertEqual(count, NUM_MOVIES)

    def test_duplicates_fail(self) -> None:
        """
        Insert documents including duplicates,
        check they fail with DuplicateDocumentError
        """

        # Create Haystack documents for insertion
        documents = [
            Document.from_dict(
                {
                    "content": "A unique document",
                }
            ),
            Document.from_dict(
                {
                    "content": "A duplicated document",
                }
            ),
            Document.from_dict(
                {
                    "content": "A duplicated document",
                }
            ),
        ]

        # Write the documents using the Store object
        with self.assertRaises(DuplicateDocumentError):
            self._es_store.write_documents(documents)  # This will FAIL by default

    def test_duplicates_skip(self) -> None:
        """
        Insert documents including duplicates,
        check duplicate documents are skipped
        """

        # Create Haystack documents for insertion
        documents = [
            Document.from_dict(
                {
                    "content": "A unique document",
                }
            ),
            Document.from_dict(
                {
                    "content": "A duplicated document",
                }
            ),
            Document.from_dict(
                {
                    "content": "A duplicated document",
                    "metadata": {"meta_key": "meta_value"},
                }
            ),
        ]

        # Write the documents using the Store object
        self._es_store.write_documents(documents, policy=DuplicatePolicy.SKIP)

        # Check 2 were inserted
        count = int(
            self._es_client.cat.count(index=INDEX_NAME, format="json")[0]["count"]
        )
        self.assertEqual(count, 2)

        # Check no metadata was added (duplicate was skipped)
        returned_document = self._es_client.get(index=INDEX_NAME, id=documents[2].id)
        self.assertIsNone(returned_document["_source"].get("meta_key"))

    def test_duplicates_overwrite(self) -> None:
        """
        Insert documents including duplicates,
        check the duplicate overwrites the old version
        """

        # Create Haystack documents for insertion
        documents = [
            Document.from_dict(
                {
                    "content": "A unique document",
                }
            ),
            Document.from_dict(
                {
                    "content": "A duplicated document",
                }
            ),
            Document.from_dict(
                {
                    "content": "A duplicated document",
                    "metadata": {"meta_key": "meta_value"},
                }
            ),
        ]

        # Write the documents using the Store object
        self._es_store.write_documents(documents, policy=DuplicatePolicy.OVERWRITE)

        # Check 2 were inserted
        count = int(
            self._es_client.cat.count(index=INDEX_NAME, format="json")[0]["count"]
        )
        self.assertEqual(count, 2)

        # Check no metadata was added (duplicate was skipped)
        returned_document = self._es_client.get(index=INDEX_NAME, id=documents[2].id)
        self.assertEqual(returned_document["_source"].get("meta_key"), "meta_value")

    def test_count_documents(self) -> None:
        """
        Insert documents just as in "test_write_documents", but then use the
        "count_documents" method to count them
        """

        # Create Haystack documents for insertion
        documents = []

        for index, row in self._movies_dataframe.iterrows():
            # We need to add a string representation of the entire row to "content",
            # because the ID hash is calculated based on the "content"
            documents.append(
                Document.from_dict(
                    {
                        "id": index,
                        "content": str(row),
                        "metadata": {
                            "title": row["Title"],
                            "ethnicity": row["Origin/Ethnicity"],
                            "director": row["Director"],
                            "cast": row["Cast"],
                            "genre": row["Genre"],
                            "plot": row["Plot"],
                            "year": row["Release Year"],
                            "wiki_page": row["Wiki Page"],
                        },
                    }
                )
            )

        # Write the documents using the Store object
        self._es_store.write_documents(documents)

        # Check the Store can count
        store_count = self._es_store.count_documents()
        self.assertEqual(store_count, NUM_MOVIES)

    def test_create_class_from_dict(self) -> None:
        """
        Check we can create an identical class from a dict of the old one
        """

        store_dict = self._es_store.to_dict()
        new_store = ElasticsearchStore.from_dict(store_dict)

        self.assertNotEqual(new_store, self._es_store)
        self.assertEqual(new_store.__dict__, self._es_store.__dict__)
