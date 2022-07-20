from unittest.case import TestCase
import io
import pdfplumber

from utils.pdf_utils import get_pdf_bytes, get_pdf_page_bytes


class PDFUtilsTest(TestCase):

    def test_get_pdf_page_bytes(self):
        sample_file_path = 'test/unit/resources/sample_documents/scanned.pdf'
        pdf_bytes = open(sample_file_path, 'rb').read()
        pdf_bytes_io = io.BytesIO(pdf_bytes)
        received_pdf_page_bytes = get_pdf_page_bytes(pdf_bytes_io=pdf_bytes_io, page_number=2)
        self.assertTrue(isinstance(received_pdf_page_bytes, bytes))


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
