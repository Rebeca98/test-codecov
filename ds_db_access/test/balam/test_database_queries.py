# Test Successful Insertion

import datetime
import pytest
from ds_db_access.balam.database_queries import insert_observations_method

# 1. Test Successful Insertion


def test_insert_observations_method():
    name = "machine"
    description = "Observations made by AI models"
    primary_key = insert_observations_method(name, description)

    assert primary_key is not None

# 2. Test Insertion with Default Description:


def test_insert_observations_method_default_description():
    name = "human"
    primary_key = insert_observations_method(name)

    assert primary_key is not None

    # You might need to query the database to check if the entry is present
    # and verify its attributes, including the absence of a description.

# 3. Test Missing Name
# insert an observation method without providing a name
# Verify that the function raises an appropriate exception or handles the error gracefully.


def test_insert_observations_method_missing_name():
    with pytest.raises(Exception):  # Adjust the exception based on your actual implementation
        insert_observations_method(None)

# 4. Test Performance
