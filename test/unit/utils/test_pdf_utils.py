from unittest.case import TestCase
import io
import pdfplumber

from utils.pdf_utils import get_pdf_bytes, convert_pdf_to_png_bytes


class PDFUtilsTest(TestCase):

    def test_convert_pdf_to_png_bytes(self):
        sample_file_path = 'test/unit/resources/sample_documents/scanned.pdf'
        pdf_bytes = open(sample_file_path, 'rb').read()
        received_png_byte_list = convert_pdf_to_png_bytes(pdf_bytes=pdf_bytes, poppler_path=None)
        self.assertEqual(5, len(received_png_byte_list))

        received_png_bytes = convert_pdf_to_png_bytes(pdf_bytes=pdf_bytes, page_number=2, poppler_path=None)
        self.assertTrue(isinstance(received_png_bytes, bytes))

    def test_get_pdf_bytes(self):
        pdf_path = 'test/unit/resources/sample_documents/sample_file1.pdf'
        pdf_bytes = get_pdf_bytes(pdf_path)
        bytes_io_obj = io.BytesIO(pdf_bytes)
        path_metadata = {}
        bytes_metadata = {}
        with pdfplumber.open(bytes_io_obj) as pdf:
            path_metadata = pdf.metadata
        with pdfplumber.open(pdf_path) as pdf:
            bytes_metadata = pdf.metadata
        self.assertDictEqual(path_metadata, bytes_metadata)
