import filecmp
import os
import json
import tempfile
import unittest
from DC_MNREGA_data.data_preprocessor import MNREGADataLoader

# module_dir_ is the path to where this test is running from.
module_dir_ = os.path.dirname(__file__)


class TestPreprocess(unittest.TestCase):

    def test_create_csv(self):
        xlsx_file = os.path.join(module_dir_, 'test_data/test.xls')
        base_url = "https://mnregaweb4.nic.in/netnrega/Citizen_html/financialstatement.aspx?lflag=eng&fin_year=2022-2023&source=national&labels=labels&Digest=cN96LBEGlHkRAwn+MUntcQ"
        expected_file = open(os.path.join(module_dir_,
                                          'test_data/expected_mnrega_data.csv'))
        expected_data = expected_file.read()
        expected_file.close()

        result_file_path = os.path.join(module_dir_,
                                        'test_data/test_cleaned.csv')

        loader = MNREGADataLoader(xlsx_file, base_url)
        loader.load()
        loader.process()
        loader.save(result_file_path)

        result_file = open(result_file_path)
        result_data = result_file.read()
        result_file.close()

        os.remove(result_file_path)
        self.assertEqual(expected_data, result_data)


if __name__ == '__main__':
    unittest.main()
