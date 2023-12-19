import os
import pytest
from peewee import PostgresqlDatabase

DB_BALAM_NAME = os.environ.get("DB_BALAM_TEST_NAME", "")
DB_BALAM_HOST = os.environ.get("DB_BALAM_TEST_HOST", "")
DB_BALAM_PASSWORD = os.environ.get("DB_BALAM_TEST_PASSWORD", "")
DB_BALAM_USER = os.environ.get("DB_BALAM_TEST_USER", "")
DB_BALAM_PORT = os.environ.get("DB_BALAM_TEST_PORT", "")

# Define the database variable in the module scope

# Fixture for setting up and tearing down the database connection


@pytest.fixture()
def setup_database():
    # Setup code before the test session
    database = PostgresqlDatabase(DB_BALAM_NAME, **{'host': DB_BALAM_HOST, 'port': int(
        DB_BALAM_PORT), 'user': DB_BALAM_USER, 'password': DB_BALAM_PASSWORD})

    yield database  # This is where the test runs
    # Teardown code after the test session
    database.close()

# Test function using the fixture


def test_database_connection(setup_database):
    # The 'setup_database' fixture will ensure the database connection is established before this test runs
    assert setup_database.connect()

    # Your test logic here
    # For example, query the database and assert some conditions
