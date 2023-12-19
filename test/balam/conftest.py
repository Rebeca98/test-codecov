import os
import pytest
from peewee import PostgresqlDatabase


@pytest.fixture(scope='session')
def setup_database():
    DB_BALAM_NAME = os.environ.get("DB_BALAM_TEST_NAME", "")
    DB_BALAM_HOST = os.environ.get("DB_BALAM_TEST_HOST", "")
    DB_BALAM_PASSWORD = os.environ.get("DB_BALAM_TEST_PASSWORD", "")
    DB_BALAM_USER = os.environ.get("DB_BALAM_TEST_USER", "")
    DB_BALAM_PORT = os.environ.get("DB_BALAM_TEST_PORT", "")
    database = PostgresqlDatabase(DB_BALAM_NAME, **{'host': DB_BALAM_HOST, 'port': int(
        DB_BALAM_PORT), 'user': DB_BALAM_USER, 'password': DB_BALAM_PASSWORD})
    # Setup code before the test session
    yield database  # This is where the test runs
    # Teardown code after the test session
    database.close()
