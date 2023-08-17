import unittest

from smartpy.aws import S3


class TestS3(unittest.TestCase):

    def test_createDeleteObjects(self):
        s3 = S3()
        test_file_key = 'test_file.json'
        # Upload and get_check_results if present
        s3.uploadDictAsJson(CRYPTOSTREET_UTILITY_S3_BUCKET, test_file_key, {'tests':'tests'})
        self.assertTrue(s3.isFile(CRYPTOSTREET_UTILITY_S3_BUCKET, test_file_key))
        # Delete and get_check_results if absent
        s3.deleteFile(CRYPTOSTREET_UTILITY_S3_BUCKET, test_file_key)
        self.assertFalse(s3.isFile(CRYPTOSTREET_UTILITY_S3_BUCKET, test_file_key))


if __name__ == '__main__':
    unittest.main()
