# ETL PySpark Project

This project contains PySpark scripts to perform basic data transformations and statistical calculations. It includes commands to run the transformations using `make` and direct `spark-submit` commands for users who do not have Docker installed.

## Prerequisites

- [Docker](https://www.docker.com/) and [Docker Compose](https://docs.docker.com/compose/) (if running with Docker)
- For users wanting to run scripts in pyenv required modules are in requirements.txt

## Running the ETL Process

You can execute the transformations using the `Makefile` commands below.

### Using Makefile (with Docker)

#### Show Available Commands
```sh
make
```

#### Run the Data Transformation, Beginning to End
```sh
make transform
```
This runs the transformation script after ensuring the data has been ingested.

#### Run the Data Ingestion Only
```sh
make ingest
```
This runs the ingestion script, preparing data for transformation.

#### Build the Docker Image
```sh
make build-image
```
This builds the required Docker image for running PySpark jobs.

#### Start an Interactive PySpark Shell
```sh
make pyspark-shell
```
This launches an interactive PySpark shell within the Docker environment.

#### Delete Output Data
```sh
make clean
```
This removes all output data from the `./data/output/` directory.

### Running Directly with Spark (Without Docker)

If you do not have Docker installed, you can run the PySpark scripts directly using `spark-submit`:

#### Run the Data Transformation
```sh
spark-submit ./python/transform.py
```

#### Run the Data Ingestion
```sh
spark-submit ./python/ingest.py
```

## Project Structure
```
./
├── data/
│   ├── input/     # Raw input data
│   ├── output/    # Processed output data
├── python/
│   ├── ingest.py  # Data ingestion script
│   ├── transform.py  # Data transformation script
├── Dockerfile
├── docker-compose.yml
├── Makefile
├── README.md
```

## TODO
- Add error handling in `ingest.py` and `transform.py`
- Add unit tests throughout
- Add exception tables for ingestion
- Add StartDate and EndDate to Dimension tables


