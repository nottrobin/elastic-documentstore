# Haystack v2 DocumentStore for ElasticSearch

## Usage

``` bash
poetry install
docker compose up --wait
poetry run ./import-movies  # Add 5000 movies from data CSV
poetry run ./get-movies     # Get some movies with Jack Nicholson
```

This should output:

``` bash
Inserted 5000 movies
Found 7 matching movies:
- The King of Marvin Gardens
- The Fortune
- The Witches of Eastwick
- Ironweed
- Something's Gotta Give
- The Two Jakes
- Mars Attacks!
```
