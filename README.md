# S3 Events Handler

S3 events handler for use in VRT v2 flow.

## Synopsis

The S3 events handler is a service that handles incoming s3 events of type `ObjectCreated` and `ObjectRemoved`.

### Object Created

This will generate a sidecar for an incoming essence. The generated XML gets put onto the MediaHaven transport server and a message gets published so that the essence gets moved to the transport server.

The object-created flow:

- Parse incoming S3 message
- Check if item is not already in MediaHaven
- Get PID from the PID webservice
- Generate MediaHaven sidecar XML
- FTP sidecar to MediaHaven transport server
- Publish message to move the file from S3 to the transport server

### Object Removed

This will remove the essence and all of its collaterals in MediaHaven.

The object-removed flow:

- Query the essence from MediaHaven with the s3_object_key and s3_bucket
- Collect all the local_ids of the fragments of the returned essence
- Query the collaterals from MediaHaven based on those local_ids
- Remove the collaterals
- Remove the essence

## Prerequisites

- Git
- Docker (optional)
- Python 3.6+
- Access to the [meemoo PyPi](http://do-prd-mvn-01.do.viaa.be:8081)

## Usage

1. Clone this repository with:

   `$ git clone https://github.com/viaacode/s3-events-handler.git`

2. Change into the new directory.

3. Set the needed config:

    Included in this repository is a `config.yml` file detailing the required configuration.
    There is also an `.env.example` file containing all the needed env variables used in the `config.yml` file.
    All values in the config have to be set in order for the application to function correctly.
    You can use `!ENV ${EXAMPLE}` as a config value to make the application get the `EXAMPLE` environment variable.

### Running locally

**Note**: As per the aforementioned requirements, this is a Python3
application. Check your Python version with `python --version`. You may want to
substitute the `python` command below with `python3` if your default Python version
is < 3. In that case, you probably also want to use `pip3` command.

1. Start by creating a virtual environment:

    `$ python -m venv env`

2. Activate the virtual environment:

    `$ source env/bin/activate`

3. Install the external modules:

    ```
    $ pip install -r requirements.txt \
        --extra-index-url http://do-prd-mvn-01.do.viaa.be:8081/repository/pypi-all/simple \
        --trusted-host do-prd-mvn-01.do.viaa.be
    ```

4. Run the tests with:

    `$ pytest -v --cov=./meemoo --cov=main`

5. Run the application:

    `$ python main.py`


### Running using Docker

1. Build the container:

   `$ docker build -t s3-events-handler .`

2. Run the container (with specified `.env` file):

   `$ docker run --env-file .env --rm s3-events-handler:latest`