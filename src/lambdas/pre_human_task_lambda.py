# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

"""Pre Human Lambda function handler."""
import json
import os
import base64
from io import BytesIO
from typing import List, Union
import traceback

import pdfplumber

from utils.s3_helper import S3Client
from utils.block_helper import JSONHandler, Geometry, Block, Relationship
from constants import general
from type.semi_structured_annotation import SemiStructuredAnnotation, SemiStructuredDocumentType
from utils.textract_helper import TextractClient
from utils.pdf_utils import convert_pdf_to_png_bytes


def is_scanned_pdf(images, page_width: float, page_height: float):
    """Return whether a PDF is a scanned PDF given its images and page dimensions."""
    page_size = page_width * page_height
    if len(images) >= 1:
        print(f'Total number of images in a single PDF page {len(images)}')
        image_size_total = 0
        for image in images:
            image_size_total += float(image['width']) * float(image['height'])
        image_size_to_page_size_ratio = image_size_total / page_size
        print(f"image_size_total = {image_size_total}, page_size = {page_size}, ratio = {image_size_to_page_size_ratio}, threshold = {general.TOTAL_IMAGE_SIZE_TO_PAGE_SIZE_RATIO_THRESHOLD}")
        return image_size_to_page_size_ratio >= general.TOTAL_IMAGE_SIZE_TO_PAGE_SIZE_RATIO_THRESHOLD
    else:
        return False


def get_pdf_blocks(pdf_bytes: bytes, page_num: int, use_textract_only: bool, source_ref: str, textract_client: TextractClient, poppler_path=general.POPPLER_PATH):
    """Get the Block objects from a PDF and also return it's type."""
    bytes_io_obj = BytesIO(pdf_bytes)
    blocks = []
    is_native_pdf = False

    # Outer try-catch is use for catching any errors with PDF (.open, .pages) or Page (.images, .extract_text, .extract_word) methods
    #   https://github.com/jsvine/pdfplumber/blob/ecc9e8e16dfc2cfc1fef0749ddb75198ab6594d4/pdfplumber/pdf.py
    #   https://github.com/jsvine/pdfplumber/blob/stable/pdfplumber/page.py
    try:
        with pdfplumber.open(bytes_io_obj) as pdf:
            page = pdf.pages[page_num - 1]
            width, height = float(page.width), float(page.height)

            if use_textract_only or is_scanned_pdf(page.images, width, height):
                print(f"use_textract_only = {use_textract_only} or Scanned PDF. getting blocks from textract")
                blocks = blocks_from_scanned_pdf(
                    pdf_bytes,
                    page_num,
                    poppler_path=poppler_path,
                    textract_client=textract_client
                )
            else:
                print(f"use_textract_only = {use_textract_only} or Native PDF, getting blocks from pdf parser")
                blocks = blocks_from_native_pdf(page, page_num, width, height)
                is_native_pdf = True
    except Exception:
        print(f"Exception occurred opening {source_ref}, treating as Textract use-case")
        traceback.print_exc()  # print stacktrace
        blocks = blocks_from_scanned_pdf(pdf_bytes, page_num, poppler_path=poppler_path, textract_client=textract_client)
    return blocks, is_native_pdf


def output_pdf_temp_file_to_s3(s3_client: S3Client, source_ref: str, content: Union[bytes, list], page_num: int, job_id: str):
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
    s3_write_path = f's3://{bucket}/{file_key}'
    s3_client.write_content(s3_path=s3_write_path, content=content)
    return s3_write_path


def get_temp_folder_bucket_key_from_s3_uri(s3_ref: str, job_id: str):
    """Get the temporary file folder name from an S3 reference and return the folder name and its bucket."""
    bucket, _ = S3Client.bucket_key_from_s3_uri(s3_ref)
    folder_key = f"comprehend-semi-structured-docs-intermediate-output/{job_id}"
    return bucket, folder_key


def get_geometry_from_plumber_word(word: dict, page_width: float, page_height: float):
    """Return a Geometry object from a PDFPlumber parsed word."""
    return Geometry(
        float(abs(word['x0'] - word['x1'])) / page_width,
        float(abs(word['top'] - word['bottom'])) / page_height,
        float(word['x0']) / page_width,
        float(word['top']) / page_height
    )


def plumber_line_to_blocks(page: int, plumber_line: List[dict], block_index: int, page_width: float, page_height: float):
    """Return a list of line and word blocks for a PDFPlumber parsed line."""
    block_index += 1
    block_line = Block(page, 'LINE', ' '.join([plumber_word['text'] for plumber_word in plumber_line]), block_index)

    block_word_list = []
    ids = []
    for plumber_word in plumber_line:
        block_index += 1
        block_word = Block(page, 'WORD', plumber_word['text'], block_index,
                           get_geometry_from_plumber_word(plumber_word, page_width, page_height), block_line.blockIndex)
        block_line.extend_geometry(block_word.Geometry)

        block_word_list.append(block_word)

        ids.append(block_word.Id)

    block_line.Relationships.append(Relationship(ids, 'CHILD'))
    ret_blocks = [block_line]
    ret_blocks.extend(block_word_list)
    return ret_blocks, block_index


def blocks_from_native_pdf(pdfplumber_page, page_num: int, page_width: float, page_height: float):
    """Return a list of blocks from a native PDF."""
    blocks = []

    text = pdfplumber_page.extract_text()
    lines = [stripped_line for stripped_line in [line_with_space.strip() for line_with_space in (text if text else '').split('\n')] if stripped_line]
    if lines:
        line_index = 0
        line_words = lines[line_index].split()
        word_index = 0

        plumber_text = []
        plumber_line = []

        words = pdfplumber_page.extract_words()
        token_sub_search_index = 0
        word_sub_search_index = 0
        token_block_list_idx = 0
        while token_block_list_idx < len(words):
            token_block = words[token_block_list_idx]
            block_word_index_in_line_word = line_words[word_index][word_sub_search_index:].find(token_block['text'][token_sub_search_index:])
            if block_word_index_in_line_word > -1:
                # block word is a sub-part of text word ex. text: "word__in__line", block: "word"
                if line_words[word_index][word_sub_search_index:] == token_block['text'][:token_sub_search_index]:
                    word_sub_search_index = len(line_words[word_index])
                else:
                    word_sub_search_index = word_sub_search_index + block_word_index_in_line_word + len(token_block['text'][token_sub_search_index:])
                plumber_line.append(token_block)
                if word_sub_search_index == len(line_words[word_index]):
                    if word_index < len(line_words) - 1:
                        word_index += 1
                    else:
                        if line_index < len(lines) - 1:
                            line_index += 1
                            line_words = lines[line_index].split()
                            word_index = 0
                            plumber_text.append(plumber_line)
                            plumber_line = []
                    word_sub_search_index = 0

                token_block_list_idx += 1
                token_sub_search_index = 0
            else:
                # text word is a sub-part of block word ex. text: "word", block: "word__in___line"
                token_sub_search_index += len(line_words[word_index])
                if word_index < len(line_words) - 1:
                    word_index += 1
                else:
                    if line_index < len(lines) - 1:
                        line_index += 1
                        line_words = lines[line_index].split()
                        word_index = 0
                        if plumber_line:
                            plumber_text.append(plumber_line)
                            plumber_line = []

        if plumber_line:
            plumber_text.append(plumber_line)

        block_index = -1
        if plumber_text:
            for plumber_line in plumber_text:
                lineAndWordBlocks, block_index = plumber_line_to_blocks(page_num, plumber_line, block_index, page_width, page_height)
                blocks.extend(lineAndWordBlocks)
    return blocks


def textract_block_to_block_relationship(textract_relationship_list: List[dict]):
    """Return a block relationship object from a Textract block object."""
    ids = []
    if textract_relationship_list:
        for relationship in textract_relationship_list:
            if relationship['Type'] != 'CHILD':
                continue
            ids.extend(relationship['Ids'])
            break
    return Relationship(ids, 'CHILD')


def textract_block_to_block(page: int, textract_block: dict, index: int, parent_index: int = -1):
    """Return a block object from a Textract block object."""
    textract_block_bounding_box = textract_block['Geometry']['BoundingBox']
    block = Block(page, textract_block['BlockType'], textract_block['Text'], index,
                  Geometry(textract_block_bounding_box['Width'], textract_block_bounding_box['Height'],
                  textract_block_bounding_box['Left'], textract_block_bounding_box['Top']))
    block.Id = textract_block['Id']
    block.Relationships = [] if 'Relationships' not in textract_block else [textract_block_to_block_relationship(textract_block['Relationships'])]
    block.parentBlockIndex = parent_index
    return block


def blocks_from_scanned_pdf(pdf_bytes: bytes, page_number: int, poppler_path: str, textract_client: TextractClient):
    """Return a list of blocks from a scanned PDF."""
    page_png_byte_value = convert_pdf_to_png_bytes(pdf_bytes=pdf_bytes, poppler_path=poppler_path, page_number=page_number)
    print(f"len of page_png_byte_value = {len(page_png_byte_value)}")
    try:
        result = textract_client.detect_document_text(page_png_byte_value)
        textract_blocks = result["Blocks"]
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
        print(f"Failed to analyze page {page_number} due to {e}")
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

    print(f"POPPLER_PATH: {general.POPPLER_PATH} file permission info")
    print(f"READ Permission: {os.access(general.POPPLER_PATH, os.R_OK)}")
    print(f"WRITE Permission: {os.access(general.POPPLER_PATH, os.W_OK)}")
    print(f"EXEC Permission: {os.access(general.POPPLER_PATH, os.X_OK)}")

    metadata = data_obj.get("metadata")

    metadata["page"] = data_obj.get("page", "1")
    page_num = int(metadata["page"])

    # Get source-ref if specified
    source_ref = data_obj.get("source-ref")
    metadata["source_ref"] = source_ref

    # document_id will consist of '{filename}_{page #}'
    doc_filename = os.path.splitext(os.path.basename(source_ref))[0]
    metadata["document_id"] = f"{doc_filename}_{metadata['page']}" if source_ref else None

    use_textract_only = metadata.get("use-textract-only", False)
    metadata["use-textract-only"] = "true" if use_textract_only else "false"

    s3_client = S3Client()
    textract_client = TextractClient()

    pdf_s3_resp = s3_client.get_object_response_from_s3(source_ref)
    pdf_bytes = pdf_s3_resp['Body'].read()
    print(f"pdf_bytes length = {len(pdf_bytes)}")
    pdf_page_base64 = base64.b64encode(pdf_bytes)
    do_ocr = True

    # Decide whether to extract blocks from input jobs' blocks or from PDF file
    primary_annotation_ref = data_obj.get("primary-annotation-ref")
    if primary_annotation_ref:
        do_ocr = False
        primary_annotation_s3_resp = s3_client.get_object_response_from_s3(primary_annotation_ref)

        # use pdf blocks from most recently modified annotation file
        secondary_annotation_ref = data_obj.get("secondary-annotation-ref")
        if secondary_annotation_ref:
            secondary_annotation_s3_resp = s3_client.get_object_response_from_s3(secondary_annotation_ref)

            primary_annotation_date = primary_annotation_s3_resp["LastModified"]
            secondary_annotation_date = secondary_annotation_s3_resp["LastModified"]

            if primary_annotation_date >= secondary_annotation_date:
                annotation_bytes = primary_annotation_s3_resp["Body"].read()
            else:
                annotation_bytes = secondary_annotation_s3_resp["Body"].read()

                # set most recent annotation file as primary annotation reference
                data_obj["primary-annotation-ref"], data_obj["secondary-annotation-ref"] = \
                    data_obj["secondary-annotation-ref"], data_obj["primary-annotation-ref"]
        else:
            annotation_bytes = primary_annotation_s3_resp["Body"].read()

        annotation_dict = json.loads(annotation_bytes.decode('utf-8'))
        annotation_obj = SemiStructuredAnnotation(**annotation_dict)

        if annotation_obj.Blocks:
            pdf_blocks = annotation_obj.Blocks
            is_native_pdf = annotation_obj.DocumentType == SemiStructuredDocumentType.NativePDF.value
        else:
            print('Remove annotation references as no extracted blocks are found in the latest annotation file.')
            data_obj.pop("primary-annotation-ref", None)
            data_obj.pop("secondary-annotation-ref", None)
            do_ocr = True

    if do_ocr:
        print(f'Attempting OCR with use-textract-only: {use_textract_only}')
        pdf_blocks, is_native_pdf = get_pdf_blocks(pdf_bytes, page_num, use_textract_only, source_ref=source_ref, textract_client=textract_client)

    # create intermediate files and write to S3
    pdf_base64_s3_ref = output_pdf_temp_file_to_s3(s3_client, source_ref, pdf_page_base64, page_num, job_id)
    pdf_block_s3_ref = output_pdf_temp_file_to_s3(s3_client, source_ref, pdf_blocks, page_num, job_id)

    task_object = {
        "pdfBase64S3Ref": pdf_base64_s3_ref,
        "pdfBlocksS3Ref": pdf_block_s3_ref,
        "pdfType": SemiStructuredDocumentType.NativePDF.value if is_native_pdf else SemiStructuredDocumentType.ScannedPDF.value,
        "version": general.VERSION,
        "metadata": metadata,
        "annotatorMetadata": data_obj.get("annotator-metadata"),
        "primaryAnnotationS3Ref": data_obj.get("primary-annotation-ref"),
        "secondaryAnnotationS3Ref": data_obj.get("secondary-annotation-ref")
    }

    # Build response object
    output = {
        "taskInput": {
            "taskObject": task_object,
            "labels": metadata.get("labels")
        },
        "humanAnnotationRequired": "true"
    }

    # If neither source nor source-ref specified, mark the annotation failed
    if source_ref is None:
        print(" Failed to pre-process {} !".format(event["labelingJobArn"]))
        output["humanAnnotationRequired"] = "false"

    print(output)

    return output
