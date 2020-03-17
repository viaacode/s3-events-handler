FROM python:3.7

# This is the location where our files will go in the container.
VOLUME /usr/src/app
WORKDIR /usr/src/app

# Copy all files
COPY . .

# Initialize virtual environment and install external packages
RUN make init
RUN make install

# This command will be run when starting the container. It is the same one that can be used to run the application locally.
CMD [ "make", "run"]