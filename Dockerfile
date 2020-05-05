FROM python:3.7

# Make a new group and user so we don't run as root.
RUN addgroup --system appgroup && adduser --system appuser --ingroup appgroup

# This is the location where our files will go in the container.
VOLUME /app
WORKDIR /app

# Copy all files
COPY --chown=appuser:appgroup . .

ARG PIPENV_VENV_IN_PROJECT=1

# Initialize virtual environment and install external packages
RUN make init
RUN pipenv install --deploy --system

USER appuser

# This command will be run when starting the container. It is the same one that can be used to run the application locally.
CMD [ "python3", "main.py"]