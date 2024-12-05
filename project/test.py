import unittest
import os
import pandas as pd
import sqlite3
from pipeline import main

class TestDataPipeline(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """
        Run the data pipeline before executing tests.
        """
        main()  # Run the ETL pipeline

    def test_sqlite_file_exists(self):
        """
        Test if the SQLite database file exists.
        """
        database_path = './data/Diabetes_data.sqlite'
        self.assertTrue(
            os.path.isfile(database_path),
            "SQLite database file does not exist."
        )

    def test_sqlite_table_content(self):
        """
        Test if the SQLite database table 'diabetes_data' has data.
        """
        database_path = './data/Diabetes_data.sqlite'
        conn = sqlite3.connect(database_path)
        query = "SELECT COUNT(*) FROM diabetes_data"
        cursor = conn.cursor()
        cursor.execute(query)
        count = cursor.fetchone()[0]
        conn.close()
        self.assertTrue(count > 0, "SQLite table 'diabetes_data' is empty.")

    def test_data_integrity(self):
        """
        Test if the data in the database meets expected conditions.
        """
        database_path = './data/Diabetes_data.sqlite'
        conn = sqlite3.connect(database_path)
        df = pd.read_sql_query("SELECT * FROM diabetes_data", conn)
        conn.close()
        self.assertFalse(df.empty, "Table 'diabetes_data' is empty.")
        self.assertTrue(
            all(df['Year'].apply(lambda x: isinstance(x, int))),
            "Invalid 'Year' column values in the table."
        )
        self.assertTrue(
            all(df['Month'].apply(lambda x: isinstance(x, int))),
            "Invalid 'Month' column values in the table."
        )

if __name__ == '__main__':
    unittest.main()
