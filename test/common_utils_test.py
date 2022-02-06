from dotenv import load_dotenv
import unittest
import os
import sys
sys.path.append("..")

import src.common_utils as cu

class Test_Utils(unittest.TestCase):
    def test_create_connection(self):
        load_dotenv()
        connection = cu.create_postgres_connection(user=os.getenv("DB_USER"),
                                                password=os.getenv("PASSWORD"),
                                                host=os.getenv("HOST"),
                                                port=os.getenv("PORT"),
                                                database=os.getenv("DATABASE"))
        cursor = connection.cursor()
        cursor.execute("SELECT 1")
        actual = cursor.fetchone()[0]
        expected = 1
        self.assertEqual(expected, actual)


if __name__ == "__main__":
    unittest.main()