help:
	@echo "Available targets:"
	@grep -E '^[a-zA-Z0-9_-]+:' Makefile | sed 's/:.*//' | sort | uniq

default: help

transform: ingest
	docker compose run --remove-orphans spark-transform spark-submit ./python/transform.py

ingest: clean build-image
	docker compose run --remove-orphans spark-transform spark-submit ./python/ingest.py

build-image:
	docker compose build

pyspark-shell: build-image
	docker compose run -it --rm spark-transform pyspark

clean:
	rm -r ./data/output/*