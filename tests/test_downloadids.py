import os
import re
import unittest
from download_idfiles import download_latest_ids, parse_filename, FILE_TYPES, clear_folder, write_to_disk


class TestDownloadIds(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        try:
            os.chdir('test_ids')
        except FileNotFoundError:
            os.mkdir('test_ids')
            os.chdir('test_ids')

        cls.files = os.listdir('.')

        write_to_disk('test.json.gz', '')

    def parse_filename_pass(self):
        collection, date = parse_filename(self.files[0])
        matches = re.match('[\d]{2}_[\d]{2}_[\d]{4}', date)
        self.assertIn(collection, FILE_TYPES.values())
        self.assertIsNotNone(matches)

    def parse_filename_fail(self):
        collection, date = parse_filename('aksdjfkjdshf')
        self.assertIsNone(collection)
        self.assertIsNone(date)

    def clear_folder(self):
        clear_folder()

    def tearDown(self):
        os.chdir('..')
        os.rmdir('test_ids')