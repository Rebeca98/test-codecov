
import pandas as pd

from ds_db_access.balam.utils_models import delete_observations_and_geoms
from ds_db_access.balam.utils_models import get_data_to_process
from ds_db_access.balam.utils_models import get_project_by_title
from ds_db_access.balam.utils_models import get_pipeline_id, get_data_processed

from ds_db_access.balam.be_datasets import ObservationsVideoPredictionDataset
from ds_db_access.balam.be_datasets import ObservationsImagePredictionDataset


def get_new_dataset(project_title: str, filetype: str, site: str):

    if filetype == 'image':
        ds = ObservationsImagePredictionDataset.get_new_data(
            project_title=project_title,
            filetype=filetype,
            site=site
        )
        ds.set_seq_ids()
    else:
        ds = ObservationsVideoPredictionDataset.get_new_data(
            project_title=project_title,
            filetype=filetype,
            site=site)
    return ds


def store_observations_from_csv(project_title: str,
                                pipeline_name: str,
                                pipeline_version: str,
                                **kwargs):
    videos_ds_path = kwargs.get('videos_ds_path', None)
    results_path = kwargs.get('results_path', None)

    if videos_ds_path is not None:
        obs_ds = ObservationsVideoPredictionDataset.from_csv(results_path)
        video_ds = ObservationsVideoPredictionDataset.from_csv(videos_ds_path)

        video_ds_df = video_ds.as_dataframe()
        obs_ds_df = obs_ds.as_dataframe()

        merged_df = obs_ds_df.merge(
            video_ds_df, on='video_id', how='left').reset_index()
        merged_df.rename(columns={'item_y': 'item'}, inplace=True)
        merged_df.rename(columns={'id_x': 'id'}, inplace=True)
        obs_ds = ObservationsVideoPredictionDataset.from_dataframes([merged_df])
    else:
        obs_ds = ObservationsImagePredictionDataset.from_csv(results_path)

    obs_ds.store_observations(
        project_title=project_title,
        pipeline_name=pipeline_name,
        pipeline_version=pipeline_version,
        **kwargs)


def store_events(project_title: str, filetype: str, site: str):

    ds = get_new_dataset(project_title=project_title,
                         filetype=filetype,
                         site=site)

    ds.store_events(filetype=filetype)


# region MAP FUNCTIONS


def map_fields_from_db_schema(data: pd.DataFrame, filetype: str):
    """Map fields from the database schema to a unified schema.

    Parameters
    ----------
    data : pd.DataFrame
        The input DataFrame containing data retrieved from the database.
    filetype : str
        The type of files in the dataset (e.g., 'image' or 'video').

    Returns
    -------
    pd.DataFrame
        A DataFrame with fields mapped to a unified schema.
    """
    # Define column name mappings
    media_id_field = 'image_id' if filetype.startswith('image') else 'video_id'
    column_mappings = {'file_id': media_id_field,
                       'file_path': 'item',
                       'timestamp': 'date_captured',
                       }

    data = data.rename(
        columns=column_mappings)

    if 'observation_type' not in data.columns:
        data['observation_type'] = None

    if 'observation_id' in data.columns:
        data = data.rename(
            columns={'observation_id': 'id'})

    data['label'] = data.apply(lambda row: row.get(
        'taxon_id') or row['observation_type'], axis=1)

    data['location'] = data.apply(lambda row: f"{row.get('site_identifier')}-{row.get('sampling_area')}-{row['device']}",
                                  axis=1)

    return data


def map_fields_to_db_schema(data: pd.DataFrame):
    """Map fields to match the database schema.

    Parameters
    ----------
    data : pd.DataFrame
        The input DataFrame containing data to be mapped.

    Returns
    -------
    pd.DataFrame
        A DataFrame with fields adjusted to match the database schema.
    """
    column_mappings = {'item': 'file_path'
                       }
    data = data.rename(columns=column_mappings)

    essential_columns = ['video_frame_num',
                         'bbox', 'confidence', 'score', 'taxon_id']

    for column in essential_columns:
        if column not in data.columns:
            data[column] = None

    data['observation_tag'] = data.apply(
        lambda row: row.get('label'), axis=1)

    return data


# endregion


# region GET FUNCTIONS

def get_processed_dataframe(filetype: str,
                            project_title: str,
                            site: str,
                            pipeline_name: str,
                            pipeline_version: str) -> pd.DataFrame:
    """Retrieve and process processed data from the database.

    This function retrieves processed data from the database based on
    the specified parameters and returns it as a Pandas DataFrame. 
    It also maps fields to the appropriate schema. If no data is found 
    in the database, the function returns None.

    Parameters
    ----------
    filetype : str
        The type of files to be retrieved (e.g., 'video' or 'image').
    project_title : str
        The title of the project from which the data should be 
        retrieved.
    site : str
        The site identifier associated with the data.
    pipeline_name : str
        The name of the pipeline used for processing.
    pipeline_version : str
        The version of the pipeline used for processing.

    Returns
    -------
    pd.DataFrame
        A DataFrame containing processed data from the database. 
        Returns None if no data is found.    
    """

    data = get_data_processed(filetype=filetype,
                              project_title=project_title,
                              site=site,
                              pipeline_name=pipeline_name,
                              pipeline_version=pipeline_version)
    if data.shape[0] > 0:
        data = map_fields_from_db_schema(data, filetype)
    return data


def get_dataframe_to_process(project_title: str,
                             filetype: str,
                             site: str,
                             pipeline_name: str,
                             pipeline_version: str
                             ):
    """_summary_

    Parameters
    ----------
    project_title : str
        _description_
    filetype : str
        _description_
    site : str
        _description_
    pipeline_name : str
        _description_
    pipeline_version : str
        _description_

    Returns
    -------
    _type_
        _description_
    """

    data = get_data_to_process(
        project_title=project_title,
        site=site,
        filetype=filetype,
        pipeline_name=pipeline_name,
        pipeline_version=pipeline_version
    )

    if data.shape[0] > 0:
        data = map_fields_from_db_schema(data, filetype)

    return data

# endregion


# region DELETE FUNCTION

def delete_observations_products(project_title: str,
                                 filetype: str,
                                 pipeline_name: str,
                                 pipeline_version: str,
                                 site_identifier: str
                                 ):
    """Deletes observations and associated geometries based on project, 
    site, and pipeline.

    This function retrieves the project and pipeline IDs based on the 
    provided project title, pipeline name, and pipeline version. 
    If both IDs are successfully obtained, it proceeds to delete 
    observations and associated geometries. If either the project or 
    pipeline ID is not properly obtained, the function exits with an 
    error message.

    Parameters
    ----------
    project_title : str
        The title of the project for which observations should 
        be deleted.
    filetype : str
        The type of files to be deleted (e.g., 'image' or 'video').
    pipeline_name : str
        The name of the pipeline for which observations should 
        be deleted.
    pipeline_version : str
        The version of the pipeline.
    site_identifier : str
        The identifier of the site from which observations should 
        be deleted.

    Returns
    -------
    None
        This function does not return any values, but it deletes 
        observations and associated
        geometries or exits with an error message.
    """

    project_id = get_project_by_title(project_title)
    pipeline_id = get_pipeline_id(pipeline_name=pipeline_name,
                                  pipeline_version=pipeline_version)

    delete_observations_and_geoms(
        project_id=project_id,
        site_identifier=site_identifier,
        filetype=filetype,
        pipeline_id=pipeline_id)


# endregion
