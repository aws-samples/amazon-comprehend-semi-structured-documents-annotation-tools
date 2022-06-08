# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""PDF utility functions."""

from io import BytesIO
from typing import List, Tuple
from pdf2image import convert_from_bytes


def ppm_image_file_to_png_bytes(ppm_image_file) -> bytes:
    """Convert a PPM Image file to PNG bytes."""
    bytes_io_obj = BytesIO()
    ppm_image_file.save(bytes_io_obj, format='PNG')
    return bytes_io_obj.getvalue()


def convert_pdf_to_png_bytes(pdf_bytes, poppler_path: str, page_number: Tuple[int, None] = None) -> Tuple[List[bytes], bytes]:
    """Convert PDF bytes to PNG bytes for all pages."""
    pdf_ppm_image_files = convert_from_bytes(pdf_bytes, poppler_path=poppler_path)
    if page_number:
        return ppm_image_file_to_png_bytes(ppm_image_file=pdf_ppm_image_files[page_number - 1])
    else:
        all_pages_png_bytes = []
        for pdf_ppm_image_file in pdf_ppm_image_files:
            all_pages_png_bytes.append(ppm_image_file_to_png_bytes(ppm_image_file=pdf_ppm_image_file))
        return all_pages_png_bytes


def get_pdf_bytes(path_to_pdf: str) -> bytes:
    """Get bytes for local PDF file."""
    pdf_file = open(path_to_pdf, 'rb')
    pdf_file_content_bytes = pdf_file.read()
    pdf_file.close()
    return pdf_file_content_bytes
