# Comprehend SSIE Annotation Tool

# Prerequisite
* Install python3.8.x (e.g. You can use [pyenv](https://github.com/pyenv/pyenv) for python version management)
* Install [jq](https://stedolan.github.io/jq/download/)
* Run the following command to install [pipenv](https://pypi.org/project/pipenv/), [aws-sam-toolkit](https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/what-is-sam.html), dependencies and setup virtualenv, etc.
```
make bootstrap
```

# How to Build, Package and Deploy
## [Optional] Step 0: update dependencies
Run the following command to update the dependencies defined in Pipfile.
Use it after you edit the Pipfile
```
make update
```

## Step 1: Build
Run the following command to run basic stylecheck, cfn-lint, etc, and build the cloudformation template
```
make build
```

## Step 2: Package and Deploy
Run the following command package the cfn template to be ready for cloudFormation deployment. And follow iteractive guidance for deployment
```
make deploy-guided
```

Note:
1. Alternatively, you can use run ```make deploy``` if there is a local samconfig.toml file
2. To deploy to different region using different credentials. You can specify *AWS_PROFILE* and *AWS_REGION* option. e.g. ```make deploy-guided AWS_PROFILE=<profile-name> AWS_REGION=<aws-region-name>```
3. The CloudFormation allows you to override the following Parameters. To specify the override values, you can run the following command: ```make deploy PRE_HUMAN_LAMBDA_TIMEOUT_IN_SECONDS=600 CONSOLIDATION_LAMBDA_TIMEOUT_IN_SECONDS=600```:
    1. *PRE_HUMAN_LAMBDA_TIMEOUT_IN_SECONDS*: The timeout value for PreHumanLambda to execute. Default to be 300 seconds
    2. *CONSOLIDATION_LAMBDA_TIMEOUT_IN_SECONDS*: The timeout value for Consolidation Lambda to execute. Default to be 300 seconds



# Start Labeling Job Using comprehend-ssie-annotation-tool-cli.py
## Step 1: [One-time setup] Create a Private Workforce for Future Labeling Jobs
Please refer to [official SageMaker Ground Truth Guide](https://docs.aws.amazon.com/sagemaker/latest/dg/sms-workforce-private-use-cognito.html) to create a private workforce, and record the corresponding workteam ARN (e.g. arn:aws:sagemaker:{AWS_REGION}:{AWS_ACCOUNT_ID}:workteam/private-crowd/{WORKFORCE_TEAM_ARN})

## Step 2: Upload Source Semi-Structured Documents to S3 bucket
From the `How to Build, Package and Deploy` Section, a CloudFormation stack has been deployed, which contains a S3 bucket. This bucket is used to store all data that is needed for the Labeling job, and it is also referred by the Lambda IAM Execution Role policy to ensure Lambda functions have necessary permission to access the data. The S3 bucket Name can be easily found in CloudFormation Stack Outputs with Key of `SemiStructuredDocumentsS3Bucket`

you need to upload the source Semi-Structure Documents into this Bucket, sample AWS CLI you can use to upload source documents from local directory to S3 bucket

```
AWS_REGION=`aws configure get region`;
AWS_ACCOUNT_ID=`aws sts get-caller-identity | jq -r '.Account'`;
aws s3 cp --recursive <local-path-to-source-docs> s3://comprehend-semi-structured-documents-${AWS_REGION}-${AWS_ACCOUNT_ID}/source-semi-structured-documents/
```

## Step 3: start-labeling-job
`comprehend-ssie-annotation-tool-cli.py` under bin/ directory is a simple wrapper command that can be used streamline the creation of SageMaker GroundTruth Job. Under the hood, this CLI will read the source documents from S3 path that you specify as an argument, create a corresponding input manifest file with a single page of one source document per line, the input Manifest file is then used as input to the Labeling Job. In addition, you also provide a list of Entity types that you define, which will become the visible types in GroundTruth UI for Annotators to label each page of the docuemnt. The following is an example of using the CLI to start labeling job.
```
AWS_REGION=`aws configure get region`;
AWS_ACCOUNT_ID=`aws sts get-caller-identity | jq -r '.Account'`;
python bin/comprehend-ssie-annotation-tool-cli.py \
    --input-s3-path s3://comprehend-semi-structured-documents-${AWS_REGION}-${AWS_ACCOUNT_ID}/source-semi-structured-documents/ \
    --cfn-name comprehend-semi-structured-documents-annotation-template \
    --work-team-name <private-work-team-name> \
    --region ${AWS_REGION} \
    --job-name-prefix "${USER}-job" \
    --entity-types "EntityTypeA, EnityTypeB, EntityTypeC" \
    --annotator-metadata "key=Info,value=Sample information,key=Due Date,value=Sample date value 12/12/1212"
```

For better annotation accuracy, we also expose the option to do more than 1 blind pass (blind1/blind2) in the CLI command of annotation through your dataset, which is equivalent to 2 workers per dataset object in Sagemaker GroundTruth. You can run a verification job comparing these 2 annotations to create the final set. The verification job can also be run after a single blind pass of annotation.
To start a verification job, omit the arguments:
--input-s3-path
--entity-types
and include the blind1 or blind1/blind2 labeling job name arguments:
--blind1-labeling-job-name
--blind2-labeling-job-name
The verification job will use the document and entity types from the blind1 (and blind2) jobs.
```
AWS_REGION=`aws configure get region`;
AWS_ACCOUNT_ID=`aws sts get-caller-identity | jq -r '.Account'`;
python bin/comprehend-ssie-annotation-tool-cli.py \
    --cfn-name comprehend-semi-structured-documents-annotation-template \
    --work-team-name <private-work-team-name> \
    --region ${AWS_REGION} \
    --job-name-prefix "${USER}-job" \
    --annotator-metadata "key=Info,value=Sample information,key=Due Date,value=Sample date value 12/12/1212" \
    --blind1-labeling-job-name <sagemaker-blind1-labeling-job-name> \
    --blind2-labeling-job-name <sagemaker-blind2-labeling-job-nam>
```

Additional customizable options:
1. Specify `--use-textract-only` flag to instruct the annotation tool to only use [Amazon Textract AnalyzeDocument API](https://docs.aws.amazon.com/textract/latest/dg/API_AnalyzeDocument.html) to parse the PDF document. By default, the tool tries to auto-detect what types of source PDF document format is, and use either [PDFPlumber](https://github.com/jsvine/pdfplumber) or Textract to parse the PDF Documents.

For more information about the CLI options, use the `-h` option. e.g.
```
python bin/comprehend-ssie-annotation-tool-cli.py -h
```


## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This project is licensed under the terms of the Apache 2.0 license. See `LICENSE`.
Included AWS Lambda functions are licensed under the MIT-0 license. See `LICENSE-LAMBDA`.
