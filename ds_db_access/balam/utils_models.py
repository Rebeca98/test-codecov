
import pandas as pd
from tqdm import tqdm
import os
from uuid import UUID
from typing import Union


from ds_db_access.balam.database_queries import get_file_id_by_url
from ds_db_access.balam.database_queries import get_pipeline_id_by_name_version
from ds_db_access.balam.database_queries import get_processed_data
from ds_db_access.balam.database_queries import get_user_id
from ds_db_access.balam.database_queries import get_project_id_by_title
from ds_db_access.balam.database_queries import insert_processed_files
from ds_db_access.balam.database_queries import get_files_data_to_process
from ds_db_access.balam.database_queries import insert_pipeline_info
from ds_db_access.balam.database_queries import delete_obs_geom
from ds_db_access.balam.database_queries import get_pipeline_execution_params
from ds_db_access.balam.database_queries import insert_observations_and_observations_geom
from ds_db_access.balam.database_queries import delete_observations
from ds_db_access.balam.database_queries import delete_processed_files
from ds_db_access.balam.database_queries import insert_observations_method
from ds_db_access.balam.database_queries import get_obs_method_id
from ds_db_access.balam.database_queries import insert_events
from ds_db_access.balam.database_queries import insert_events_files
from ds_db_access.balam.database_queries import get_files_id_not_in_events
from conabio_ml.utils.logger import get_logger
from ds_db_access.balam.params_db import S3_PATH


logger = get_logger(__name__)


def find_file_url(file_path: str, s3_path: str) -> str:
    """
    Generate a URL for a file located in a specific directory.

    This function constructs a URL for a file based on its relative path
    within a designated directory.

    Parameters
    ----------
    file_path : str
        The relative path to the file within the specified directory.

    Returns
    -------
    str
        The URL representing the file's location.
    """
    url = os.path.join(s3_path, file_path)

    return url


def lambda_handler(s):
    parts = s.split('/')
    data_index = parts.index('data')
    result = '/'.join(parts[data_index + 1:])
    return result

# region GET FUNCTIONS


def get_observation_method_id(observation_method):
    observation_method_id = get_obs_method_id(observation_method=observation_method)
    return observation_method_id


def get_user_id_by_username(username: str) -> str:
    """
    Retrieve the user ID by providing a username.

    This function retrieves the user ID associated with a given username
    from the database.

    Parameters
    ----------
    username : str
        The username of the user to find in the database.

    Returns
    -------
    str
        The user ID if the user is found, or None if the user with the
        given username doesn't exist.
    """
    user_id = get_user_id(username=username)
    return str(user_id)


def get_pipeline_id(pipeline_name: str, pipeline_version: str) -> str:
    """
    Retrieve the pipeline ID by providing the pipeline name and version.

    This function retrieves the ID of a pipeline by specifying its name
    and version. It queries the database to find the corresponding
    pipeline ID.

    Parameters
    ----------
    pipeline_name : str
        The name of the pipeline.
    pipeline_version : str
        The version of the pipeline.

    Returns
    -------
    str
        The pipeline ID if the pipeline with the given name and version
        is found, or None if it doesn't exist.
    """

    pipeline_id = get_pipeline_id_by_name_version(
        pipeline_name=pipeline_name, pipeline_version=pipeline_version)
    return str(pipeline_id)


def get_project_by_title(project_name: str) -> str:
    """
    Retrieve the project ID by providing the project title.

    This function retrieves the ID of a project by specifying its title.
    It queries the database to find the corresponding project ID.

    Parameters
    ----------
    project_name : str
        The title of the project.

    Returns
    -------
    str
        The project ID if the project with the given title is found, or
        None if it doesn't exist.
    """
    project_id = get_project_id_by_title(project_name)
    return str(project_id)


def get_pipeline_exec_params(pipeline_name: str, pipeline_version: str):
    """
    Retrieve the execution parameters for a specific pipeline.

    This function retrieves the execution parameters for a pipeline by
    providing its name and version. It queries the database to obtain
    the execution parameters associated with the pipeline.

    Parameters
    ----------
    pipeline_name : str
        The name of the pipeline.
    pipeline_version : str
        The version of the pipeline.

    Returns
    -------
    List[Dict[str, any]]
        A list of dictionaries containing the execution parameters for
        the specified pipeline.
    """
    data = get_pipeline_execution_params(
        pipeline_name=pipeline_name, pipeline_version=pipeline_version)
    return data


def get_data_to_process(project_title: str,
                        site: str,
                        filetype: str,
                        pipeline_name: str,
                        pipeline_version: str) -> pd.DataFrame:
    """
    Retrieve data to process based on project, site, and file type.

    This function retrieves data files to process based on the specified
    project title, site, and file type. It queries the database and
    returns the data in a Pandas DataFrame for further processing.

    Parameters
    ----------
    project_title : str
        The title of the project for which data should be retrieved.
    site : str
        The identifier of the site where the data was collected.
    filetype : str
        The type of data files to retrieve ('image' or 'video').
    pipeline_name : str
        The name of the pipeline.
    pipeline_version : str
        The version of the pipeline.

    Returns
    -------
    pd.DataFrame
        A Pandas DataFrame containing information about the data files
        to process, including file paths and metadata.

    Notes
    -----
    The returned DataFrame contains columns: 'file_path', 'file_id',
    'timestamp', 'longitude', 'latitude', 'site_identifier',
    'sampling_area', 'device', 'ecosystem'.
    """
    if filetype == 'image':
        mime_type = 'image/%'
    else:
        mime_type = 'video/%'

    try:
        results = get_files_data_to_process(mime_type=mime_type, pipeline_name=pipeline_name,
                                            pipeline_version=pipeline_version, site=site, project_title=project_title)
    except ValueError as e:
        print(f"Error: {e}")
        return

    data = pd.DataFrame(results)

    if data.shape[0] > 0:
        data = data.rename(columns={'url': 'file_path'})
        data['file_path'] = data['file_path'].apply(lambda_handler)
        data['datetime'] = pd.to_datetime(
            data['date'].astype(str) + ' ' + data['time'])
    return data


def get_data_processed(filetype: str, project_title: str, site: str, pipeline_name: str, pipeline_version: str) -> pd.DataFrame:
    """
    Retrieve processed data based on file type, project, site, and
    pipeline.

    This function retrieves processed data files based on the specified
    file type, project title, site, and pipeline information.
    It queries the database and returns the data in a Pandas DataFrame
    for further processing.

    Parameters
    ----------
    filetype : str
        The type of data files to retrieve ('image' or 'video').
    project_title : str
        The title of the project for which data should be retrieved.
    site : str
        The identifier of the site where the data was collected.
    pipeline_name : str
        The name of the pipeline used for processing.
    pipeline_version : str
        The version of the pipeline used for processing.

    Returns
    -------
    pd.DataFrame
        A Pandas DataFrame containing information about the processed
        data files, including file paths and metadata.

    Notes
    -----
    - The returned DataFrame contains columns: 'file_path', 'file_id',
    'timestamp', 'longitude', 'latitude', 'site_identifier',
    'sampling_area', 'device', 'ecosystem'.
    """

    if filetype == 'image':
        mime_type = 'image/%'
    else:
        mime_type = 'video/%'

    try:
        data = get_processed_data(mime_type=mime_type, project_title=project_title,
                                  site=site, pipeline_name=pipeline_name, pipeline_version=pipeline_version)
    except ValueError as e:
        print(f"Error: {e}")
        return

    data = pd.DataFrame(data)

    if data.shape[0] > 0:
        data = data.rename(columns={'url': 'file_path'})
        data['file_path'] = data['file_path'].apply(lambda_handler)
        data['datetime'] = pd.to_datetime(
            data['date'].astype(str) + ' ' + data['time'])

    return data


def get_files_with_no_events(project_title: str, filetype: str, site_identifier: str):

    if filetype == 'image':
        mime_type = 'image/%'
    else:
        mime_type = 'video/%'
    try:
        data = get_files_id_not_in_events(project_title=project_title,
                                          mime_type=mime_type,
                                          site_identifier=site_identifier)
    except ValueError as e:
        print(f"Error: {e}")
        return
    data = pd.DataFrame(data)

    if data.shape[0] > 0:
        data = data.rename(columns={'url': 'file_path'})
        data['file_path'] = data['file_path'].apply(lambda_handler)
        data['date_captured'] = pd.to_datetime(
            data['date'].astype(str) + ' ' + data['time'])
    return data


# endregion

# region INSERT FUNCTIONS


def insert_events_table(filetype: str, data_df: pd.DataFrame):

    events_ids = []
    total_iterations = len(data_df)

    if filetype == 'image':
        for event_id in tqdm(data_df['seq_id'].unique(), desc='Inserting Events'):
            try:
                primary_key = insert_events(event_id=event_id,
                                            event_type='photo_sequence')
                events_ids.append(primary_key)
            except ValueError as e:
                print(f"Error: {e}")

        for row in tqdm(data_df.to_dict('records'),
                        total=total_iterations, desc="Inserting Events Files"):
            try:
                primary_key = insert_events_files(event_id=row['seq_id'],
                                                  file_id=row['image_id'])

            except ValueError as e:
                print(f"Error: {e}")
    else:
        return

    return len(events_ids)


def insert_observations(observations_df: pd.DataFrame,
                        project_id: str,
                        pipeline_id: str,
                        username: str,
                        observation_method: str,
                        s3_path: str):
    """
    Insert observations and associated geometries into the database.

    This function inserts observation data, including observations and
    associated geometries, into the database based on the provided
    DataFrame and other parameters. It iterates through the DataFrame,
    associates observations with files, and adds relevant information
    to the database.

    Parameters
    ----------
    observations_df : pd.DataFrame
        A Pandas DataFrame containing observation data to be inserted.
    project_id : str
        The ID of the project to which the observations are related.
    pipeline_id : str
        The ID of the pipeline used for processing (can be None).
    username : str
        The username of the user inserting the observations.
    observation_method_id : str
        The ID of the observation method used for these observations.

    Returns
    -------
    int
        The total number of observations inserted into the database.
    """

    try:
        user_id = get_user_id_by_username(username)
    except ValueError as e:
        print(f"Error: {e}")
        return

    try:
        observation_method_id = get_observation_method_id(observation_method)
    except ValueError as e:
        print(f"Error: {e}")
        return

    total_iterations = len(observations_df)

    observations_df['url'] = observations_df.apply(
        lambda row: find_file_url(row['file_path'], s3_path=s3_path), axis=1)

    processed_files = []
    for row in tqdm(observations_df.to_dict('records'), total=total_iterations, desc="Inserting observations"):
        try:
            file_id = get_file_id_by_url(row["url"])
            processed_files.append(file_id)
            observation_tag = {"predicted_label": row['observation_tag']}
            if 'scientific_name' in observations_df.columns:
                observation_tag['scientific_name'] = row['scientific_name']

            insert_observations_and_observations_geom(file_id=file_id,
                                                      observation_id=row['id'],
                                                      observation_type=row['observation_type'],
                                                      observation_tag=observation_tag,
                                                      bbox=row['bbox'],
                                                      score=row['score'],
                                                      confidence=row['confidence'],
                                                      project_id=project_id,
                                                      pipeline_id=pipeline_id,
                                                      user_id=user_id,
                                                      observation_method_id=observation_method_id,
                                                      video_frame_num=row['video_frame_num'],
                                                      taxon_id=row['taxon_id']
                                                      )
        except:
            print("no se pudo")  # agregar un logging
    if pipeline_id is not None:
        for _file_id in tqdm(set(processed_files), desc="Inserting processed files", unit="file"):
            insert_processed_files(file_id=_file_id, pipeline_id=pipeline_id)

    return total_iterations


def insert_pipeline(pipeline_id: Union[UUID, str],
                    pipeline_name: str,
                    pipeline_version: str,
                    url_repo_model: str,
                    execution_params: dict,
                    comments: str,
                    created_at: str,
                    updated_at: str,
                    last_exec: str,
                    ):
    """Insert a new pipeline into the database.

    This function inserts a new pipeline into the database by providing
    essential information such as the pipeline name,
    version, URL to the source code repository, execution parameters,
    and comments. The function returns the primary key
    of the newly inserted pipeline.

    Parameters
    ----------
    pipeline_name : str
        The name of the pipeline to be inserted.
    pipeline_version : str
        The version of the pipeline.
    url_repo_model : str
        The URL of the repository containing the pipeline's source code.
    execution_params : dict
        A dictionary describing the parameters used in the pipeline.
    comments : str
        Comments on the pipeline.

    Returns
    -------
    str
        The primary key of the new pipeline inserted in the database.
    """
    try:
        primary_key = insert_pipeline_info(pipeline_id,
                                           pipeline_name,
                                           pipeline_version,
                                           url_repo_model,
                                           execution_params,
                                           comments,
                                           created_at,
                                           updated_at,
                                           last_exec,
                                           )
    except Exception as e:
        print(f"Error: {e}")
    return primary_key


def insert_observationsMethod(name: str, description: str):
    try:
        primary_key = insert_observations_method(name=name, description=description)
    except Exception as e:
        print(f"Error: {e}")
    return primary_key


# endregion

# region DELETE FUNCTIONS


def delete_observations_and_geoms(project_id: str, site_identifier: str, filetype: str, pipeline_id: str):
    """Delete observations, geometries, and processed files associated
    with a specific project, site, filetype, and pipeline.

    This function is used to delete observations, their associated
    geometries, and processed files that are specific to a
    given project, site, file type, and pipeline. The 'filetype'
    parameter should be 'image' or 'video' to determine the
    appropriate mime type for deletion.

    Parameters
    ----------
    project_id : str
        The identifier of the project from which observations should
        be deleted.
    site_identifier : str
        The identifier of the site associated with the observations.
    filetype : str
        The type of files being processed ('image' or 'video').
    pipeline_id : str
        The identifier of the pipeline related to the observations.
    """
    if filetype == 'image':
        mime_type = 'image/%'
    else:
        mime_type = 'video/%'
    delete_count_obs = delete_observations(project_id=project_id, site_identifier=site_identifier,
                                           mime_type=mime_type, pipeline_id=pipeline_id)
    logger.info(f"{delete_count_obs} observations deleted.")
    delete_count_geom = delete_obs_geom()
    logger.info(f"{delete_count_geom} observations goemetries deleted.")
    delete_count_processed_files = delete_processed_files(
        project_id=project_id, pipeline_id=pipeline_id, site_identifier=site_identifier, mime_type=mime_type)
    logger.info(f"{delete_count_processed_files} processed files deleted.")


# endregion
