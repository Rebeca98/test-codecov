import psycopg2

from dotenv import load_dotenv

import os

dotenv_path = os.getenv("DOTENV_PATH")

load_dotenv(dotenv_path)

DB_NAME = os.environ.get("DB_PUFFERFISH_NAME", "")
DB_HOST = os.environ.get("DB_PUFFERFISH_HOST", "")
DB_PASSWORD = os.environ.get("DB_PUFFERFISH_PASSWORD", "")
DB_USER = os.environ.get("DB_PUFFERFISH_USER", "")
DB_PORT = os.environ.get("DB_PUFFERFISH_PORT", "")


def query_processed_files(original_path, original_name):
    try:
        # Establish a connection to the PostgreSQL database
        conn = psycopg2.connect(
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )

        # Create a cursor object to interact with the database
        cursor = conn.cursor()

        # Define the SQL query with a parameter
        query = """
            SELECT id, project_name, original_path, original_name, new_name, new_path, updated_at, created_at
            FROM processedfiles
            WHERE original_name = %s and original_path = %s;
        """

        # Execute the query with the parameter
        cursor.execute(query, (original_name, original_path))

        # Fetch all the rows
        rows = cursor.fetchall()

    except Exception as e:
        print(f"Error: {e}")

    finally:
        # Close the cursor and connection
        if cursor:
            cursor.close()
        if conn:
            conn.close()
    return rows
