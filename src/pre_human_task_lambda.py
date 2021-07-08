# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

"""Pre Human Lambda function handler."""
import json
import boto3
import pdfplumber
import os
import base64
from io import BytesIO
from pdf2image import convert_from_bytes
from s3_helper import S3Client
from typing import Union
from block_helper import JSONHandler, Geometry, Block, Relationship


VERSION = "2021-04-30"
TOTAL_IMAGE_SIZE_TO_PAGE_SIZE_RATIO_THREASHOLD = 0.25
POPPLER_PATH = '/opt/bin/'


def s3_to_bytes(s3, s3_ref):
    """Return the bytes for an S3 reference undecoded."""
    bucket, key = S3Client.bucket_key_from_s3_uri(s3_ref)
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        payload = response['Body'].read()
        return payload
    except Exception as e:
        print(e)
        print('Error getting object {} from bucket {}. '
              'Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket))
        raise e


def is_scanned_pdf(images, page_width, page_height):
    """Return whether a PDF is a scanned PDF given its images and page dimensions."""
    page_size = page_width * page_height
    if len(images) >= 1:
        print(f'Total number of images in a single PDF page {len(images)}')
        image_size_total = 0
        for image in images:
            image_size_total += image['width'] * image['height']
        image_size_to_page_size_ratio = image_size_total / page_size
        print(f"image_size_total = {image_size_total}, page_size = {page_size}, ratio = {image_size_to_page_size_ratio}, threshold = {TOTAL_IMAGE_SIZE_TO_PAGE_SIZE_RATIO_THREASHOLD}")
        return image_size_to_page_size_ratio >= TOTAL_IMAGE_SIZE_TO_PAGE_SIZE_RATIO_THREASHOLD
    else:
        return False


def get_pdf_blocks(pdf_bytes, page_num, use_textract_only):
    """Get the Block objects from a PDF and also return it's type."""
    bytes_io_obj = BytesIO(pdf_bytes)
    blocks = []
    is_native_pdf = True
    with pdfplumber.open(bytes_io_obj) as pdf:
        page = pdf.pages[page_num - 1]
        width, height = page.width, page.height

        if use_textract_only or is_scanned_pdf(page.images, width, height):
            print(f"use_textract_only = {use_textract_only} or Scanned PDF. getting blocks from textract")
            blocks = blocks_from_scanned_pdf(pdf_bytes, page_num)
            is_native_pdf = False
        else:
            print(f"use_textract_only = {use_textract_only} or Native PDF, getting blocks from pdf parser")
            blocks = blocks_from_native_pdf(page, page_num, width, height)
    return blocks, is_native_pdf


def output_pdf_temp_file_to_s3(s3, source_ref: str, content: Union[str, list], page_num: int, job_id: str):
    """Write a temporary file to a created S3 location and return the file key."""
    bucket, temp_folder_key = get_temp_folder_bucket_key_from_s3_uri(source_ref, job_id)
    print(f'uploading data to bucket = {bucket}, path = {temp_folder_key}')
    _, pdf_key = S3Client.bucket_key_from_s3_uri(source_ref)
    pdf_key_file_name = S3Client.remove_extension(pdf_key).split('/')[-1]
    if type(content) == bytes:
        file_key = f'{temp_folder_key}/{pdf_key_file_name}_{page_num}_base64'
    else:
        file_key = f'{temp_folder_key}/{pdf_key_file_name}_{page_num}_blocks.json'
        content = json.dumps(content, default=JSONHandler)
    s3.put_object(Body=content, Bucket=bucket, Key=file_key)
    return f's3://{bucket}/{file_key}'


def get_temp_folder_bucket_key_from_s3_uri(s3_ref: str, job_id: str):
    """Get the temporary file folder name from an S3 reference and return the folder name and its bucket."""
    bucket, pdf_key = S3Client.bucket_key_from_s3_uri(s3_ref)
    # parts = pdf_key.split('/')
    # folder_key = f"{'/'.join(parts[0:-2])}intermediate-files"
    folder_key = f"comprehend-semi-structured-docs-intermediate-output/{job_id}"
    return bucket, folder_key


def get_geometry_from_plumber_word(word, page_width, page_height):
    """Return a Geometry object from a PDFPlumber parsed word."""
    return Geometry(
        float(abs(word['x0'] - word['x1']) / page_width),
        float(abs(word['top'] - word['bottom']) / page_height),
        float(word['x0'] / page_width),
        float(word['top'] / page_height)
    )


def plumber_line_to_blocks(page, plumber_line, blockIndex, page_width, page_height):
    """Return a list of line and word blocks for a PDFPlumber parsed line."""
    blockIndex += 1
    blockLine = Block(page, 'LINE', ' '.join([plumber_word['text'] for plumber_word in plumber_line]), blockIndex)

    blockWordList = []
    ids = []
    for plumber_word in plumber_line:
        blockIndex += 1
        blockWord = Block(page, 'WORD', plumber_word['text'], blockIndex,
                          get_geometry_from_plumber_word(plumber_word, page_width, page_height), blockLine.blockIndex)
        blockLine.extend_geometry(blockWord.Geometry)

        blockWordList.append(blockWord)

        ids.append(blockWord.Id)

    blockLine.Relationships.append(Relationship(ids, 'CHILD'))
    ret_blocks = [blockLine]
    ret_blocks.extend(blockWordList)
    return ret_blocks, blockIndex


def blocks_from_native_pdf(page, page_num, page_width, page_height):
    """Return a list of blocks from a native PDF."""
    # print('blocks_from_native_pdf')
    text = page.extract_text()
    lines = [strippedLine for strippedLine in [lineWithSpace.strip() for lineWithSpace in text.split('\n')] if strippedLine]
    lineIndex = 0
    lineWords = lines[lineIndex].split()
    wordIndex = 0

    plumberText = []
    plumberLine = []

    words = page.extract_words()
    token_sub_search_index = 0
    word_sub_search_index = 0
    token_block_list_idx = 0
    while token_block_list_idx < len(words):
        token_block = words[token_block_list_idx]
        block_word_index_in_line_word = lineWords[wordIndex][word_sub_search_index:].find(token_block['text'][token_sub_search_index:])
        if block_word_index_in_line_word > -1:
            # block word is a sub-part of text word ex. text: "word__in__line", block: "word"
            if lineWords[wordIndex][word_sub_search_index:] == token_block['text'][:token_sub_search_index]:
                word_sub_search_index = len(lineWords[wordIndex])
            else:
                word_sub_search_index = word_sub_search_index + block_word_index_in_line_word + len(token_block['text'][token_sub_search_index:])
            plumberLine.append(token_block)
            if word_sub_search_index == len(lineWords[wordIndex]):
                if wordIndex < len(lineWords) - 1:
                    wordIndex += 1
                else:
                    if lineIndex < len(lines) - 1:
                        lineIndex += 1
                        lineWords = lines[lineIndex].split()
                        wordIndex = 0
                        plumberText.append(plumberLine)
                        plumberLine = []
                word_sub_search_index = 0

            token_block_list_idx += 1
            token_sub_search_index = 0
        else:
            # text word is a sub-part of block word ex. text: "word", block: "word__in___line"
            token_sub_search_index += len(lineWords[wordIndex])
            if wordIndex < len(lineWords) - 1:
                wordIndex += 1
            else:
                if lineIndex < len(lines) - 1:
                    lineIndex += 1
                    lineWords = lines[lineIndex].split()
                    wordIndex = 0
                    if plumberLine:
                        plumberText.append(plumberLine)
                        plumberLine = []

    if plumberLine:
        plumberText.append(plumberLine)

    blocks = []
    blockIndex = -1
    if plumberText:
        for plumberLine in plumberText:
            lineAndWordBlocks, blockIndex = plumber_line_to_blocks(page_num, plumberLine, blockIndex, page_width, page_height)
            blocks.extend(lineAndWordBlocks)
    return blocks


def analyze_document(byte_array):
    """Call Textract's detect_document_text method and return the payload."""
    textract_client = boto3.client('textract')

    response = textract_client.detect_document_text(
        Document={
            'Bytes': byte_array
        }
    )

    return response


def textract_block_to_block_relationship(tr):
    """Return a block relationship object from a Textract block object."""
    ids = []
    if tr:
        for r in tr:
            if r['Type'] != 'CHILD':
                continue
            ids.extend(r['Ids'])
            break
    return Relationship(ids, 'CHILD')


def textract_block_to_block(page, tb, index, parent_index=-1):
    """Return a block object from a Textract block object."""
    textract_block_bounding_box = tb['Geometry']['BoundingBox']
    block = Block(page, tb['BlockType'], tb['Text'], index,
                  Geometry(textract_block_bounding_box['Width'], textract_block_bounding_box['Height'],
                  textract_block_bounding_box['Left'], textract_block_bounding_box['Top']))
    block.Id = tb['Id']
    block.Relationships = [] if 'Relationships' not in tb else [textract_block_to_block_relationship(tb['Relationships'])]
    block.parentBlockIndex = parent_index
    return block


def blocks_from_scanned_pdf(pdf_bytes, page_number):
    """Return a list of blocks from a scanned PDF."""
    # print('blocks_from_scanned_pdf')
    pdf_ppm_image_files = convert_from_bytes(pdf_bytes, poppler_path=POPPLER_PATH)
    pdf_page_ppm_image_file = pdf_ppm_image_files[page_number - 1]
    bytes_io_obj = BytesIO()
    pdf_page_ppm_image_file.save(bytes_io_obj, format='PNG')
    png_byte_value = bytes_io_obj.getvalue()
    print(f"len of png_byte_value = {len(png_byte_value)}")
    try:
        result = analyze_document(png_byte_value)
        textract_blocks = result['Blocks']
        textract_line_blocks = [block for block in textract_blocks if block['BlockType'] == 'LINE']
        textract_word_blocks = [block for block in textract_blocks if block['BlockType'] == 'WORD']
        print("== Textract blocks ==")
        print(f"number of total textract blocks = {len(textract_blocks)}")
        print(f"number of textract line blocks = {len(textract_line_blocks)}")
        print(f"number of textract word blocks = {len(textract_word_blocks)}")

        # use to quickly retrieve word blocks
        idToWordBlock = {b['Id']: b for b in textract_blocks if b['BlockType'] == 'WORD'}

        blocks = []
        # for each textract line block, create a line block, then create the word blocks by looping through its Relationships,
        #   if the relationship is of type CHILD, loop through the relationships Ids array and create word blocks
        index = -1
        for textract_lb in textract_line_blocks:
            index += 1
            line_block = textract_block_to_block(page_number, textract_lb, index)
            line_index = index

            blocks.append(line_block)
            if line_block.Relationships:
                for id in line_block.Relationships[0].Ids:
                    index += 1
                    textract_word_block = idToWordBlock[id]
                    word_block = textract_block_to_block(page_number, textract_word_block, index, line_index)
                    blocks.append(word_block)

        line_blocks = [block for block in blocks if block.BlockType == 'LINE']
        word_blocks = [block for block in blocks if block.BlockType == 'WORD']

        print(" == Blocks after conversion== ")
        print(f"number of after conversion blocks = {len(blocks)}")
        print(f"number of after conversion line blocks = {len(line_blocks)}")
        print(f"number of after conversion word blocks = {len(word_blocks)}")
        return blocks
    except Exception as e:
        print(f"Failed to analyze {page_number} due to {e}")
    return []


def lambda_handler(event, context):
    """
    Sample PreHumanTaskLambda (pre-processing lambda) for custom labeling jobs.
    For custom AWS SageMaker Ground Truth Labeling Jobs, you have to specify a PreHumanTaskLambda (pre-processing lambda).
    AWS SageMaker invokes this lambda for each item to be labeled. Output of this lambda, is merged with the specified
    custom UI template. This code assumes that specified custom template have only one placeholder "taskObject".
    If your UI template have more parameters, please modify output of this lambda.

    Parameters
    ----------
    event: dict, required
        Content of event looks some thing like following
        {
           "version":"2018-10-16",
           "labelingJobArn":"<your labeling job ARN>",
           "dataObject":{
              "source-ref":"s3://<your bucket>/<your keys>/awesome.pdf",
              "page": "<page number, if not provided will default to 1>"
              "metadata": {
                  "pages": "<total # of pages in the PDF>",
                  "use-textract-only": <True or False>,
                  "labels": <list of label strings>
              },
              "annotator-metadata": <dictionary defined during job creation>,
              "primary-annotation-ref": "<S3 Uri for primary annotation>" or None,
              "secondary-annotation-ref": "<S3 Uri for secondary annotation>" or None
           }
        }
        As SageMaker product evolves, content of event object will change. For a latest version refer following URL
        Event doc: https://docs.aws.amazon.com/sagemaker/latest/dg/sms-custom-templates-step3.html
    context: object, required
        Lambda Context runtime methods and attributes
        Context doc: https://docs.aws.amazon.com/lambda/latest/dg/python-context-object.html

    Returns
    -------
    output: dict
        This output is an example JSON. We assume that your template have only one placeholder named "taskObject".
        If your template have more than one placeholder, make sure to add one more attribute under "taskInput"
        {
           "taskInput":{
              "taskObject": {
                  "pdfBase64S3Ref": <S3 reference to the PDF page's base64 string>,
                  "pdfBlocksS3Ref": <S3 reference to the PDF page's block objects>,
                  "pdfType": <NativePDF or ScannedPDF>,
                  "version": <current date in YYYY-MM-DD format>,
                  ...<other properties in the inputted dataObject>
              },
              "labels": <list of label strings>
           },
           "humanAnnotationRequired":"true"
        }
        Note: Output of this lambda will be merged with the template, you specify in your labeling job.
        You can use preview button on SageMaker Ground Truth console to make sure merge is successful.
        Return doc: https://docs.aws.amazon.com/sagemaker/latest/dg/sms-custom-templates-step3.html

    """
    # Event received
    print("Received event: " + json.dumps(event, indent=2))
    job_arn = event['labelingJobArn']
    job_id = job_arn.split('/')[-1]
    print(f'labeling job id = {job_id}')
    data_obj = event["dataObject"]

    print(f"POPPLER_PATH={POPPLER_PATH} file permission info")
    print(f"READ Permission: {os.access(POPPLER_PATH, os.R_OK)}")
    print(f"WRITE Permission: {os.access(POPPLER_PATH, os.W_OK)}")
    print(f"EXEC Permission: {os.access(POPPLER_PATH, os.X_OK)}")

    metadata = data_obj['metadata']

    page_num = int(data_obj["page"]) if "page" in data_obj else 1
    metadata['page'] = str(page_num)

    # Get source-ref if specified
    source_ref = data_obj["source-ref"] if "source-ref" in data_obj else None
    metadata["source_ref"] = source_ref

    # document_id will consist of '{filename}_{page #}'
    doc_filename = os.path.splitext(os.path.basename(source_ref))[0]
    metadata["document_id"] = f"{doc_filename}_{metadata['page']}" if source_ref else None

    use_textract_only = metadata["use-textract-only"] if "use-textract-only" in metadata else False
    metadata["use-textract-only"] = "true" if use_textract_only else "false"

    s3 = boto3.client('s3')

    pdf_bytes = s3_to_bytes(s3, source_ref)
    print(f"pdf_bytes length = {len(pdf_bytes)}")
    pdf_page_base64 = base64.b64encode(pdf_bytes)

    # Decide whether to extract blocks from input job's blocks S3 object or from PDF file
    if "primary-annotation-ref" in data_obj:
        primary_annotation_obj = json.loads(s3_to_bytes(s3, data_obj["primary-annotation-ref"]).decode('utf-8'))
        pdf_blocks = primary_annotation_obj['Blocks']
        is_native_pdf = primary_annotation_obj['DocumentType'] == 'NativePDF'
    else:
        pdf_blocks, is_native_pdf = get_pdf_blocks(pdf_bytes, page_num, use_textract_only)

    pdf_base64_s3_ref = output_pdf_temp_file_to_s3(s3, source_ref, pdf_page_base64, page_num, job_id)
    pdf_block_s3_ref = output_pdf_temp_file_to_s3(s3, source_ref, pdf_blocks, page_num, job_id)

    task_object = {
        "pdfBase64S3Ref": pdf_base64_s3_ref,
        "pdfBlocksS3Ref": pdf_block_s3_ref,
        "pdfType": "NativePDF" if is_native_pdf else "ScannedPDF",
        "version": VERSION,
        "metadata": metadata,
        "annotatorMetadata": data_obj["annotator-metadata"] if "annotator-metadata" in data_obj else None
    }

    # Set primary and secondary annotation S3 references for UI to use
    if "primary-annotation-ref" in data_obj:
        task_object["primaryAnnotationS3Ref"] = data_obj["primary-annotation-ref"]
        if "secondary-annotation-ref" in data_obj:
            task_object["secondaryAnnotationS3Ref"] = data_obj["secondary-annotation-ref"]

    print(task_object)

    # Build response object
    output = {
        "taskInput": {
            "taskObject": task_object,
            "labels": metadata["labels"]
        },
        "humanAnnotationRequired": "true"
    }

    # If neither source nor source-ref specified, mark the annotation failed
    if source_ref is None:
        print(" Failed to pre-process {} !".format(event["labelingJobArn"]))
        output["humanAnnotationRequired"] = "false"

    return output
