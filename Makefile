# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
SHELL := /bin/sh
PY_VERSION := 3.8

export PYTHONUNBUFFERED := 1

SRC_DIR := src
SAM_DIR := .aws-sam

SRC_TEMPLATE_NAME := comprehend-semi-structured-documents-annotation-template

# Required environment variables (user must override)

# user can optionally override the following by setting environment variables with the same names before running make

# Path to system pip
PIP ?= pip
# Default AWS CLI region
AWS_REGION ?= us-west-2

# AWS CLI profile names
AWS_PROFILE ?= default

# Default value for prehuman lambda timeout in second
PRE_HUMAN_LAMBDA_TIMEOUT_IN_SECONDS ?= 300

# Default value for consolidation lambda timeout in second
CONSOLIDATION_LAMBDA_TIMEOUT_IN_SECONDS ?= 300

PYTHON := $(shell /usr/bin/which python$(PY_VERSION))

.DEFAULT_GOAL := build

clean:
	rm -f $(SRC_DIR)/requirements.txt
	rm -rf $(SAM_DIR)
	rm -rf samconfig.toml
	rm -rf .pytest_cache
	rm -rf src/__pycache__

# Use this command after checking out the package to install tools and dependencies from pipfile.lock
bootstrap:
	$(PYTHON) -m $(PIP) install aws-sam-cli
	$(PYTHON) -m $(PIP) install pipenv
	$(PYTHON) -m $(PIP) install awscli 
	pipenv sync --dev # Install locked dependencies, including dev-dependencies
	pipenv shell

# Activate
activate:
	pipenv shell # activate the virtualenv

# Update the dependency
update:
	pipenv install # Install dependencies and update the pipfile.lock file
	pipenv sync --dev #  Install locked dependencies from pipfile.lock
	pipenv lock --requirements > $(SRC_DIR)/requirements.txt

# Run basic python checkstyle and cnf-lint, and build the CFN template using sam cli
build:
	pipenv lock --requirements > $(SRC_DIR)/requirements.txt
	pipenv run flake8 $(SRC_DIR)
	pipenv run pydocstyle $(SRC_DIR)
	pipenv run cfn-lint $(SRC_TEMPLATE_NAME).yml
	sam build --profile $(AWS_PROFILE) --template $(SRC_TEMPLATE_NAME).yml

unit-testing: build
	pipenv run py.test --cov=$(SRC_DIR) --cov-fail-under=97 -vv test/unit -s --cov-report html

# can be triggered as `make integ-testing LAMBDA_NAME=access-control`
integ-testing: unit-testing
	pipenv run py.test  -s -vv test/integ/test_$(LAMBDA_NAME).py

# Deploy the sam packaged CFN template with --guided option
deploy-guided:
	sam deploy -g --profile $(AWS_PROFILE) --region $(AWS_REGION) --parameter-overrides "ParameterKey=PreHumanLambdaTimeoutInSeconds,ParameterValue=$(PRE_HUMAN_LAMBDA_TIMEOUT_IN_SECONDS) ParameterKey=ConsolidationLambdaTimeoutInSeconds,ParameterValue=$(CONSOLIDATION_LAMBDA_TIMEOUT_IN_SECONDS)" --capabilities CAPABILITY_IAM

# Deploy the sam packaged CFN template without --guided option, use default samconfig.toml if present
deploy:
	sam deploy --profile $(AWS_PROFILE) --region $(AWS_REGION) --parameter-overrides "ParameterKey=PreHumanLambdaTimeoutInSeconds,ParameterValue=$(PRE_HUMAN_LAMBDA_TIMEOUT_IN_SECONDS) ParameterKey=ConsolidationLambdaTimeoutInSeconds,ParameterValue=$(CONSOLIDATION_LAMBDA_TIMEOUT_IN_SECONDS)" --capabilities CAPABILITY_IAM
