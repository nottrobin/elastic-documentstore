# Standard library
from typing import Type, Optional, Any

# Packages
from elasticsearch import Elasticsearch, NotFoundError
from elasticsearch.helpers import bulk
from haystack.preview.document_stores.errors import (
    DuplicateDocumentError,
    MissingDocumentError,
)
from haystack.preview.document_stores.protocols import DuplicatePolicy
from haystack.preview.document_stores.decorator import document_store
from haystack.preview.dataclasses.document import Document


def _convert_es_source_to_document(source: dict) -> Document:
    """
    First, add Document properties to document object
    (defined in haystack.preview.dataclasses.document.Document)
    and remove them from the ES hit source.
    Then add all the remaining items as "metadata".
    """

    document_dict = {
        "content": source.pop("content", None),
        "content_type": source.pop("content_type", None),
        "id_hash_keys": source.pop("id_hash_keys", None),
        "score": source.pop("score", None),
        "embedding": source.pop("embedding", None),
        "metadata": source,  # All remaining items become metadata
    }

    return Document.from_dict(document_dict)


def _convert_document_to_es_source(document: Document) -> dict:
    """
    First, add Document properties to the document object
    (defined in haystack.preview.dataclasses.document.Document)
    and remove them from the document_dict.
    Then add "metadata" as further properties on the ES object,
    alongside document properties.
    """

    document_dict = document.to_dict()

    del document_dict["id"]  # Remove the ID, this is handled separately

    return {
        "content": document_dict.pop("content", None),
        "content_type": document_dict.pop("content_type", None),
        "id_hash_keys": document_dict.pop("id_hash_keys", None),
        "source": document_dict.pop("source", None),
        "embedding": document_dict.pop("embedding", None),
        **document_dict.pop("metadata", {}),
    }


@document_store
class ElasticsearchStore:
    """
    A Haystack v2 DocumentStore class,
    to interface with a single Index within an Elasticsearch backend.
    """

    def __init__(self, es_client: Type[Elasticsearch], index_name: str) -> None:
        """
        The store expects to be passed a pre-configured instance of Elasticsearch,
        pointing to a Elasticsearch with an index already set up. This is to keep
        this class as naive and single-purpose as possible.

        :param es_client: An Elasticsearch v8.0 client object,
            configured to connect to an Elasticsearch backend
        :param index_name: The name of the index within Elasticsearch for this
            class to operate on
        """

        self._es_client = es_client
        self._index_name = index_name

    def to_dict(self) -> dict[str, Any]:
        """
        As the counterpart to from_dict,
        this method needs to serialise the store to allow it to
        be recreated from this dictionary.
        """

        return {"es_client": self._es_client, "index_name": self._index_name}

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "ElasticsearchStore":
        """
        Re-create this store from a dictionary containing its initialisation options.
        """

        return cls(es_client=data["es_client"], index_name=data["index_name"])

    def count_documents(self) -> int:
        """
        Returns the number of documents stored.
        """

        return int(
            self._es_client.cat.count(index=self._index_name, format="json")[0]["count"]
        )

    def filter_documents(
        self, filters: Optional[dict[str, Any]] = None
    ) -> list[Document]:
        """
        Returns the documents that match the filters provided.

        Filters are defined as nested dictionaries.
        The keys of the dictionaries can be a logical operator (`"$and"`,
        `"$or"`, `"$not"`), a comparison operator
        (`"$eq"`, `$ne`, `"$in"`, `$nin`, `"$gt"`, `"$gte"`, `"$lt"`, `"$lte"`)
        or a metadata field name.

        Logical operator keys take a dictionary of metadata field names and/or logical
        operators as value. Metadata field names take a dictionary of comparison
        operators as value. Comparison operator keys take a single value or
        (in case of `"$in"`) a list of values as value. If no logical operator
        is provided, `"$and"` is used as default operation. If no comparison operator
        is provided, `"$eq"` (or `"$in"` if the comparison value is a list) is used
        as default operation.

        Example:

        ```python
        filters = {
            "$and": {
                "type": {"$eq": "article"},
                "date": {"$gte": "2015-01-01", "$lt": "2021-01-01"},
                "rating": {"$gte": 3},
                "$or": {
                    "genre": {"$in": ["economy", "politics"]},
                    "publisher": {"$eq": "nytimes"}
                }
            }
        }
        # or simpler using default operators
        filters = {
            "type": "article",
            "date": {"$gte": "2015-01-01", "$lt": "2021-01-01"},
            "rating": {"$gte": 3},
            "$or": {
                "genre": ["economy", "politics"],
                "publisher": "nytimes"
            }
        }
        ```

        To use the same logical operator multiple times on the same level,
        logical operators can take a list of
        dictionaries as value.

        Example:

        ```python
        filters = {
            "$or": [
                {
                    "$and": {
                        "Type": "News Paper",
                        "Date": {
                            "$lt": "2019-01-01"
                        }
                    }
                },
                {
                    "$and": {
                        "Type": "Blog Post",
                        "Date": {
                            "$gte": "2019-01-01"
                        }
                    }
                }
            ]
        }
        ```

        :param filters: the filters to apply to the document list.
        :return: a list of Documents that match the given filters.
        """

        raise NotImplementedError()

    def write_documents(
        self,
        documents: list[Document],
        policy: DuplicatePolicy = DuplicatePolicy.FAIL,
    ) -> None:
        """
        Writes (or overwrites) documents into the DocumentStore.

        Elasticsearch expects dictionary objects of key to value mappings.
        This doesn't map perfectly onto
        Haystack's document content types ("text", "table", "image", "audio").
        The one that can support
        such a mapping is a "table" type with a single row.
        Therefore, this is the type of document
        that this method expects to receive.

        By default, Elasticsearch overwrites documents with existing IDs.
        It has no native functionality
        for FAIL or SKIP behaviour. To implement this behaviour we have to
        manually check whether an
        object exists for a given ID first, which may slow things down.
        So the "OVERWRITE" behaviour
        will likely be quickest to perform, unless there are a large number of
        overwrites that could be skipped.

        :param documents: A Haystack v2 Documents.
        :param policy: Documents with the same ID count as duplicates.
        When duplicates are met,
            the DocumentStore can:
             - skip: keep the existing document and ignore the new one.
             - overwrite: remove the old document and write the new one.
             - fail: an error is raised
        :raises DuplicateError: Exception trigger on duplicate document
            if `policy=DuplicatePolicy.FAIL`
        :return: None
        """

        bulk_data = []
        added_ids = []

        for document in documents:
            if policy in (DuplicatePolicy.FAIL, DuplicatePolicy.SKIP):
                # Only look up the document if we have to
                es_response = self._es_client.options(ignore_status=404).get(
                    index=self._index_name,
                    id=document.id,
                )

                if document.id in added_ids or es_response.get("found"):
                    if policy is DuplicatePolicy.FAIL:
                        raise DuplicateDocumentError()
                    elif policy is DuplicatePolicy.SKIP:
                        continue

            bulk_data.append(
                {
                    "_index": self._index_name,
                    "_id": document.id,
                    "_source": _convert_document_to_es_source(document),
                }
            )
            added_ids.append(document.id)

        bulk(self._es_client, bulk_data)
        self._es_client.indices.refresh(index=self._index_name)

    def delete_documents(self, document_ids: list[str]) -> None:
        """
        Deletes all documents with a matching document_ids from the DocumentStore.
        Fails with `MissingDocumentError` if no document with this id is present
        in the DocumentStore.

        :param object_ids: the object_ids to delete
        """

        for document_id in document_ids:
            try:
                self._es_client.delete(index=self._index_name, id=document_id)
                self._es_client.indices.refresh(index=self._index_name)
            except NotFoundError:
                raise MissingDocumentError
