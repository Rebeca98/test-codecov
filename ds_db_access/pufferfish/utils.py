import pandas as pd
from ds_db_access.src.pufferfish.database_queries import query_processed_files


def create_dataframe(original_path, original_name):
    # Call the query_processed_files function to get rows
    rows = query_processed_files(original_path, original_name)

    # Create a Pandas DataFrame from the fetched rows
    columns = ["id", "project_name", "original_path", "original_name",
               "new_name", "new_path", "updated_at", "created_at"]
    df = pd.DataFrame(rows, columns=columns)

    # Print or manipulate the DataFrame as needed
    return df
