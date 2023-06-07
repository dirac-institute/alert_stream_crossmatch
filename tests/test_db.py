import sqlite3
import unittest

def add_column_to_table(conn, table_name, column_name, data_type):
    cursor = conn.cursor()

    # Construct the ALTER TABLE command to add the new column
    alter_query = f"ALTER TABLE {table_name} ADD COLUMN {column_name} {data_type}"

    # Execute the ALTER TABLE command
    cursor.execute(alter_query)

    # Commit the changes to the database
    conn.commit()

class TestAddColumnToTable(unittest.TestCase):
    def setUp(self):
        self.conn = sqlite3.connect('data/test.db')
        self.cursor = self.conn.cursor()

        # Create a sample table for testing
        self.cursor.execute("CREATE TABLE test_table (id INTEGER, name TEXT)")

    def tearDown(self):
        self.cursor.close()
        self.conn.close()

    def test_add_column(self):
        # Call the function to add a new column
        add_column_to_table(self.conn, 'test_table', 'age', 'INTEGER')

        # Fetch the table schema to check if the column exists
        self.cursor.execute("PRAGMA table_info(test_table)")
        columns = [column[1] for column in self.cursor.fetchall()]

        # Assert that the 'age' column is present in the table
        self.assertIn('age', columns)

if __name__ == '__main__':
    unittest.main()
