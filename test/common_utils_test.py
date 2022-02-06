from dotenv import load_dotenv
import os
import sys
sys.path.append("..")

import src.common_utils as cu


def test_create_connection():
    load_dotenv()
    connection = cu.create_postgres_connection(user=os.getenv("DB_USER"),
                                               password=os.getenv("PASSWORD"),
                                               host=os.getenv("HOST"),
                                               port=os.getenv("PORT"),
                                               database=os.getenv("DATABASE"))
    print(type(connection))


if __name__ == "__main__":
    test_create_connection()