.ONESHELL:
SHELL = /bin/bash

.PHONY: all test

test:
	pip install -r requirements.txt --extra-index-url http://do-prd-mvn-01.do.viaa.be:8081/repository/pypi-all/simple --trusted-host do-prd-mvn-01.do.viaa.be
	export $(shell cat .env.example | xargs)
	pytest -vvv --cov=./meemoo --junit-xml=./tests/test_results.xml