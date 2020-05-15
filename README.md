# Nano Borndigital

Nano borndigital for use in VRT v2 flow.

## Synopsis

The nano borndigital is a service that generates a sidecar for an incoming essence. The generated XML get put onto the MediaHaven transport server and a message gets published so that the essence gets moved to the transport server.

The main flow:
- Parse incoming S3 message
- Get PID from the PID webservice
- Generate MediaHaven sidecar XML 
- FTP sidecar to MediaHaven transport server
- Publish message to move the file from S3 to the transport server


## Usage

Since we use pipenv to manage packages and virtual environments you have to install it. You can do this manually or using: 
$ make init

After installing pipenv you can install the requirements using:
$ make install

To run the application you will have to set some environment variables.
You can manually set them or you can make an `.env` file containing the following variables:
    - MEDIAHAVEN_FTP_USER=
    - MEDIAHAVEN_FTP_PASSWD=
    - RABBIT_MQ_USER=
    - RABBIT_MQ_PASSWD=
    - APP_ENVIRONMENT=
    - S3_EVENTS_QUEUE=
    - S3_EVENTS_EXCHANGE=
    - FILETRANSFER_QUEUE=
    - FILETRANSFER_EXCHANGE=
Make sure you have a valid `config.yml` file. An example is added in the repository.

Afterwards you can run the application using:
$ make run

Tests can be ran using:
$ make tests