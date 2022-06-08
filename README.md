# Comprehend Semi-Structured Documents Annotation Tool
 
# Step 0: Prerequisites
* Install python3.8.x (e.g. You can use [pyenv](https://github.com/pyenv/pyenv) for python version management)
* Install [jq](https://stedolan.github.io/jq/download/)
* Have the latest version of [aws-cli](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-install.html) installed and your AWS credentials configured (https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-files.html)



# How to Build, Package and Deploy

## Option 1 (Recommended):

### Single-Step:
Run the following command to install all dependencies into a virtualenv, build the CloudFormation stack from the template, and deploy the stack to your AWS account with interactive guidance. 
```
make ready-and-deploy-guided
```
## Option 2:

### Step 1: Bootstrap
Run the following command to install [pipenv](https://pypi.org/project/pipenv/), [aws-sam-toolkit](https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/what-is-sam.html), dependencies and setup virtualenv, etc.
```
make bootstrap
```

### Step 2: Enter the shell containing all required dependencies
```
make activate
```

### Step 3: Build
Run the following command to run basic stylecheck, cfn-lint, etc, and build the CloudFormation template
```
make build
```

### Step 4: Package and Deploy
Run the following command package the CloudFormation template to be ready for CloudFormation deployment, and follow interactive guidance for deployment.
This CloudFormation stack will manage the created lambdas, IAM roles and S3 bucket. (IMPORTANT: Keep note of the CloudFormation name as that will be used later)
```
make deploy-guided 
```

Note: 
- Alternatively, you can use run ```make deploy``` if there is already a local samconfig.toml file
- To deploy to different region using different credentials. You can specify *AWS_PROFILE* and *AWS_REGION* option. e.g. ```make deploy-guided AWS_PROFILE=<profile-name> AWS_REGION=<aws-region-name>```
- The CloudFormation allows you to override the following Parameters. To specify the override values, you can run the following command: ```make deploy PRE_HUMAN_LAMBDA_TIMEOUT_IN_SECONDS=600 CONSOLIDATION_LAMBDA_TIMEOUT_IN_SECONDS=600```:
    1. *PRE_HUMAN_LAMBDA_TIMEOUT_IN_SECONDS*: The timeout value for PreHumanLambda to execute. Default to be 300 seconds
    2. *CONSOLIDATION_LAMBDA_TIMEOUT_IN_SECONDS*: The timeout value for Consolidation Lambda to execute. Default to be 300 seconds
    3. *PRE_HUMAN_LAMBDA_MEMORY_IN_MB*: The memory value for PreHumanLambda to execute. Default to be 10240 MB
    4. *CONSOLIDATION_LAMBDA_MEMORY_IN_MB*: The memory value for Consolidation Lambda to execute. Default to be 10240MB
- To update dependencies in the Pipfile, run `make update` and continue to `Step 2: Build`.



# Start Labeling Job Using comprehend-ssie-annotation-tool-cli.py

## Step 1: [One-time setup] Create a Private Workforce for Future Labeling Jobs
Please refer to [official SageMaker Ground Truth Guide](https://docs.aws.amazon.com/sagemaker/latest/dg/sms-workforce-private-use-cognito.html) to create a private workforce, and record the corresponding workteam ARN (e.g. arn:aws:sagemaker:{AWS_REGION}:{AWS_ACCOUNT_ID}:workteam/private-crowd/{WORKFORCE_TEAM_ARN})

## Step 2: Upload Source Semi-Structured Documents to S3 bucket 
From the `How to Build, Package and Deploy` Section, a CloudFormation stack has been deployed, which contains an S3 bucket. This bucket is used to store all data that is needed for the Labeling job, and it is also referred to by the Lambda IAM Execution Role policy to ensure Lambda functions have necessary permission to access the data. The S3 bucket Name can be easily found in CloudFormation Stack Outputs with Key of `SemiStructuredDocumentsS3Bucket`.

You need to upload the source Semi-Structure Documents into this Bucket. Here is a sample AWS CLI command you can use to upload source documents from local directory to S3 bucket:
- local-path-to-source-docs: relative or absolute file path to local source documents
- source-folder-name: name of a folder within the CloudFormation stack's managed S3 bucket

```
AWS_REGION=`aws configure get region`;
AWS_ACCOUNT_ID=`aws sts get-caller-identity | jq -r '.Account'`;
aws s3 cp --recursive <local-path-to-source-docs> s3://comprehend-semi-structured-docs-${AWS_REGION}-${AWS_ACCOUNT_ID}/<source-folder-name>/
```

## Step 3: Create the labeling job:
Prerequisites:
1. Make sure you are in the pipenv shell by running `pipenv shell`. You should see something like: `Shell for /Users/<user>/.local/share/virtualenvs <package_name>-zsZ94mSG already activated. No action taken to avoid nested environments.` Otherwise, run `make bootstrap` to enter the pipenv shell.
2. Make sure you are running this command from the package root directory.

`comprehend-ssie-annotation-tool-cli.py` under the bin/ directory is a simple wrapper command that can be used streamline the creation of SageMaker GroundTruth Labeling Job. Under the hood, this CLI script will read the source documents from the S3 path specified as an argument, create a corresponding input manifest file with a single page of one source document per line, and the input Manifest file is then used as input to the Labeling Job. In addition, you also provide a list of Entity types that you define which will become the visible types in GroundTruth UI for Annotators to label each page of the document. The following is an example of using the CLI to start a labeling job: 
- `input-s3-path`: S3 Uri to the source documents you copied earlier in `Upload Source Semi-Structured Documents to S3 bucket`
- `cfn-name`: The name of the CloudFormation stack name entered in the `Package and Deploy` step.
- `work-team-name`: The workforce name created from `[One-time setup] Create a Private Workforce for Future Labeling Jobs`
- `region`: The AWS region. ex. us-west-2
- `job-name-prefix`: The prefix to have for the Sagemaker GroundTruth labeling job (LIMIT: 29 characters). Note: Extra text will be appended to job name prefix, ex. `-labeling-job-task-20210902T232116`, unless `--no-suffix` is used (See Additional customizable options (3)).
- `entity-types`: The entities you would like to use during the labeling job (separated by commas)

```
AWS_REGION=`aws configure get region`;
AWS_ACCOUNT_ID=`aws sts get-caller-identity | jq -r '.Account'`;
python bin/comprehend-ssie-annotation-tool-cli.py \
    --input-s3-path s3://comprehend-semi-structured-docs-${AWS_REGION}-${AWS_ACCOUNT_ID}/<source-folder-name>/ \
    --cfn-name sam-app \
    --work-team-name <private-work-team-name> \
    --region ${AWS_REGION} \
    --job-name-prefix "${USER}-job" \
    --entity-types "EntityTypeA, EnityTypeB, EntityTypeC"
```
The job has now been created and can be accessed the Sagemaker GroundTruth labeling portal.


For more information about the CLI options, use the `-h` option. e.g. 
```
python bin/comprehend-ssie-annotation-tool-cli.py -h 
```

Additional customizable options:
1. Specify `--use-textract-only` flag to instruct the annotation tool to only use [Amazon Textract AnalyzeDocument API](https://docs.aws.amazon.com/textract/latest/dg/API_AnalyzeDocument.html) to parse the PDF document. By default, the tool tries to auto-detect what types of source PDF document format is, and use either [PDFPlumber](https://github.com/jsvine/pdfplumber) or Textract to parse the PDF Documents. 
2. Include `--annotator-metadata` parameter to reveal key-value information to annotators. Default metadata about the document is already revealed to the annotator within the UI side panel.
3. Include `--no-suffix` parameter to indicate to only use `--job-name-prefix` for the job name. By default, a unique ID is suffixed to the `--job-name-prefix`.
4. Include `--task-time-limit` parameter to indicate a desired time limit in seconds for each task in the Sagemaker GroundTruth labeling job. By default, the time limit is set to 3600 seconds or 1 hour.
5. To edit/verify previous annotations, we also expose a "verification" feature. Omit `--input-s3-path` and `--entity-types` from the labeling job creation script, and include `--blind1-labeling-job-name` which should be the name of the previous annotation's job name.
    ```
    AWS_REGION=`aws configure get region`;
    AWS_ACCOUNT_ID=`aws sts get-caller-identity | jq -r '.Account'`;
    python bin/comprehend-ssie-annotation-tool-cli.py \
        --cfn-name sam-app \
        --work-team-name <private-work-team-name> \
        --region ${AWS_REGION} \
        --job-name-prefix "${USER}-job" \
        --blind1-labeling-job-name <sagemaker-blind1-labeling-job-name>
    ```
6. For better annotation accuracy, we also expose the option of running a verification job comparing 2 annotation jobs (blind1/blind2) to create a final set.
    - To start a 2-job verification job, omit `input-s3-path` and `entity-types` and include `blind1-labeling-job-name` and `blind2-labeling-job-name`. The verification job will use the document and entity types from the first annotation (blind1) job.
        ```
        AWS_REGION=`aws configure get region`;
        AWS_ACCOUNT_ID=`aws sts get-caller-identity | jq -r '.Account'`;
        python bin/comprehend-ssie-annotation-tool-cli.py \
            --cfn-name sam-app \
            --work-team-name <private-work-team-name> \
            --region ${AWS_REGION} \
            --job-name-prefix "${USER}-job" \
            --blind1-labeling-job-name <sagemaker-blind1-labeling-job-name> \
            --blind2-labeling-job-name <sagemaker-blind2-labeling-job-nam>
        ```
7. Include `--task-availability-time-limit` parameter to indicate a desired time limit in seconds for the Sagemaker GroundTruth labeling job. By default, the time limit is set to 864000 seconds or 10 days.
8. Specify the `--only-include-expired-tasks` flag in conjunction with `--blind1-labeling-job-name` to indicate to only include tasks which had expired (only available for use in a verification job).
