# Haystack v2 DocumentStore for ElasticSearch

A Haystack v2 DocumentStore class, `elasticsearch_haystack.documentstore.ElasticsearchStore`, to interface with a single Index within an Elasticsearch backend.

## Set up

Start by spinning up Elasticsearch and installing Poetry dependencies (first [get Docker](https://docs.docker.com/engine/install/) or [get Poetry](https://python-poetry.org/docs/#installation) if needed):

``` bash
docker compose up -d
poetry install
```

## Running tests

``` bash
poetry run tox          # run tests of the documentstore
poetry run tox -e lint  # check the format of Python files
```

## Playing around

Jump into a Python shell with Poetry:

``` bash
poetry run ipython3
```

And start playing around:

``` python3
from elasticsearch import Elasticsearch
from elasticsearch_haystack.documentstore import ElasticsearchStore
from haystack.preview.dataclasses.document import Document

# Set up the store
# ===
es_client = Elasticsearch("http://localhost:9200")
es_store = ElasticsearchStore(es_client, index_name="movies")

# Recreate the store from dict
# ===
es_store = ElasticsearchStore.from_dict(es_store.to_dict())

# Insert 1000 movies into the store
# ===
movies_dataframe = (
    pd.read_csv("data/wiki_movie_plots_deduped.csv")
    .dropna()
    .sample(1000)
    .reset_index()
)

documents = []

for index, row in movies_dataframe.iterrows():
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

es_store.write_documents(documents)

# Count documents
# ===
es_store.count_documents()

# Delete a random document
# ===
es_store.delete_documents([documents[26].id])
```
