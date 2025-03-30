help:
	@echo "Available targets:"
	@grep -E '^[a-zA-Z0-9_-]+:' Makefile | sed 's/:.*//' | sort | uniq

default: help

run-transform:
	docker compose run --remove-orphans spark-transform spark-submit ./python/spark-transform.py

build-image:
	docker compose build

pyspark-shell:
	docker run --rm -it spark-transform pyspark

