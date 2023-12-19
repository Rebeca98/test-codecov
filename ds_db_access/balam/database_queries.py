from tqdm import tqdm
import datetime
from typing import Optional
from peewee import fn
from peewee import SQL
from peewee import DoesNotExist
from peewee import IntegrityError
from typing import Union
from uuid import UUID
from ds_db_access.balam.balam_models import ObservationMethod
from ds_db_access.balam.balam_models import PipelineInfo
from ds_db_access.balam.balam_models import Sites
from ds_db_access.balam.balam_models import SamplingPoints
from ds_db_access.balam.balam_models import ObservationGeom
from ds_db_access.balam.balam_models import Observations
from ds_db_access.balam.balam_models import Files
from ds_db_access.balam.balam_models import Projects
from ds_db_access.balam.balam_models import SamplingAreas
from ds_db_access.balam.balam_models import ProjectDevices
from ds_db_access.balam.balam_models import Ecosystems
from ds_db_access.balam.balam_models import ProcessedFiles
from ds_db_access.balam.balam_models import Users
from ds_db_access.balam.balam_models import EventsFiles
from ds_db_access.balam.balam_models import Events


import uuid
from conabio_ml.utils.logger import get_logger

from ds_db_access.balam.params_db import DATETIME

logger = get_logger(__name__)

# region INSERT FUNCTIONS


def insert_observations_method(observation_method_id: Union[UUID, str],
                               name: str,
                               created_at: str = str(datetime.datetime.now()),
                               updated_at: str = str(datetime.datetime.now()),
                               description: Optional[str] = None,

                               ) -> str:
    """Insert an observations method into the ObservationMethod table.

    Parameters
    ----------
    name : str
        The name of the method used to generate an observation. Options: machine, human.

    description : str, optional
        A description of the observation method (default is None).

    Returns
    -------
    str
        Primary key of the new ObservationMethod item.
    """
    data = {
        ObservationMethod.id: observation_method_id,
        ObservationMethod.created_at: created_at,
        ObservationMethod.updated_at: updated_at,
        ObservationMethod.name: name,
        ObservationMethod.description: description
    }
    try:
        primary_key = ObservationMethod.insert(data).execute()
    except IntegrityError as e:
        if 'duplicate' in str(e):
            raise IntegrityError(f"Uniqueness Violation Detected: {e}") from e

        raise IntegrityError(f"Another type of integrity error:{e}") from e

    return primary_key


def insert_pipeline_info(pipeline_id: Union[UUID, str],
                         pipeline_name: str,
                         pipeline_version: str,
                         url_repo_model: str,
                         execution_params: dict,
                         comments: str,
                         created_at: str = str(datetime.datetime.now()),
                         updated_at: str = str(datetime.datetime.now()),
                         last_exec: str = str(datetime.datetime.now())
                         ) -> str:
    """Insert a new pipeline into the PipelineInfo table.

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
        (e.g., {"thres": 0.4, "min_score_threshold": 0.1, "freq_video_sampling": 2})
    comments : str
        Comments on the pipeline.

    Returns
    -------
    str
        The primary key of the new pipeline inserted in the PipelineInfo table.
    """

    try:
        data = {PipelineInfo.id: pipeline_id,  # str(uuid.uuid4()),
                PipelineInfo.created_at: created_at,  # datetime.datetime.now(),
                PipelineInfo.updated_at: updated_at,  # datetime.datetime.now(),
                PipelineInfo.name: pipeline_name,
                PipelineInfo.version: pipeline_version,
                PipelineInfo.url_repo_model: url_repo_model,
                PipelineInfo.execution_params: execution_params,
                PipelineInfo.last_execution: last_exec,
                PipelineInfo.comments: comments}
        primary_key = PipelineInfo.insert(data).execute()
    except IntegrityError as e:
        if 'duplicate' in str(e):
            raise IntegrityError(f"Uniqueness Violation Detected: {e}") from e
        raise IntegrityError(f"Another type of integrity error:{e}") from e

    return primary_key


def insert_processed_files(file_id: str, pipeline_id: str) -> str:
    """Insert a file processed by a pipeline into the ProcessedFiles table.

    Parameters
    ----------
    file_id : str
        The unique identifier of the file from the Files table.
    pipeline_id : str
        The unique identifier of the pipeline from the PipelineInfo table.

    Returns
    -------
    str
        The primary key of the inserted file in the ProcessedFiles table.
    """

    if not Files.select().where(Files.id == file_id).exists():
        raise DoesNotExist(f"Files with id {file_id} does not exist in the Files table.")
    if not PipelineInfo.select().where(PipelineInfo.id == pipeline_id).exists():
        raise DoesNotExist(
            f"Pipeline with id {pipeline_id} does not exist in the PipelineInfo table.")

    combined_id = uuid.uuid5(uuid.NAMESPACE_DNS, f"{file_id}{pipeline_id}")

    data = {
        ProcessedFiles.id: combined_id,
        ProcessedFiles.created_at: datetime.datetime.now(),
        ProcessedFiles.updated_at: datetime.datetime.now(),
        ProcessedFiles.file: file_id,
        ProcessedFiles.pipeline: pipeline_id
    }
    try:
        primary_key = ProcessedFiles.insert(data).execute()
    except IntegrityError as e:
        if 'duplicate' in str(e):
            raise IntegrityError(f"Uniqueness Violation Detected: {e}") from e
        else:
            raise IntegrityError(f"Another type of integrity error:{e}") from e

    return primary_key


def insert_observations_and_observations_geom(file_id: str,
                                              observation_id: str,
                                              observation_type: str,
                                              observation_tag: dict,
                                              bbox: str,
                                              score: float,
                                              confidence: float,
                                              project_id: str,
                                              pipeline_id: str,
                                              user_id: str,
                                              observation_method_id: str,
                                              video_frame_num: int,
                                              taxon_id: str) -> str:
    """Insert observations and geometries into the Observations and ObservationGeom tables.

    Parameters
    ----------
    file_id : str
        The unique identifier of the file from the Files table.
    observation_type : str
        Type of observation (options: 'animal', 'empty', 'person').
    observation_tag : dict
        Tag describing the kind of observation (canonical name or arbitrary group).
        (e.g., {"predicted_label":"small bird"})
    bbox : str
        Bounding box generated from detections with a specific observation_type or, in some cases, observation_tag.
    score : float
        Confidence value from the detector model (e.g., MegaDetector).
    confidence : float
        Confidence value from the classification model. In human annotations, this value is 1.
    project_id : str
        ID related to the project in the Project table.
    pipeline_id : str
        ID related to the pipeline in the PipelineInfo table.
    user_id : str
        ID related to the user who inserts the observations in the Users table.
    observation_method_id : str
        ID related to the observation method in the ObservationMethod table.
    video_frame_num : int
        Frame number within the video, and has nothing to do with the sample rate you use.
    taxon_id : str
        ID related to the taxon obtained with the BED system.

    Returns
    -------
    str
        The primary key of the observation inserted in the Observations table.
    """
    geom_id = None

    if bbox is not None:
        data_obs_geom = {ObservationGeom.id: str(uuid.uuid4()),
                         ObservationGeom.created_at: datetime.datetime.now(),
                         ObservationGeom.updated_at: datetime.datetime.now(),
                         ObservationGeom.bbox: bbox,
                         ObservationGeom.video_frame_num: video_frame_num
                         }
        geom_id = ObservationGeom.insert(data_obs_geom).execute()

    if not Files.select().where(Files.id == file_id).exists():
        raise DoesNotExist(f"Files with id {file_id} does not exist in the Files table.")

    if not Projects.select().where(Projects.id == project_id).exists():
        raise DoesNotExist(f"Projects with id {project_id} does not exist in the Projects table.")

    if not PipelineInfo.select().where(PipelineInfo.id == pipeline_id).exists():
        raise DoesNotExist(
            f"Pipeline with id {pipeline_id} does not exist in the PipelineInfo table.")

    if not Users.select().where(Users.id == user_id).exists():
        raise DoesNotExist(f"Users with id {user_id} does not exist in the Users table.")

    if not ObservationMethod.select().where(ObservationMethod.id == observation_method_id).exists():
        raise DoesNotExist(
            f"ObservationMethod with id {observation_method_id} does not exist in the ObservationMethod table.")

    data_obs = {Observations.id: str(observation_id),
                Observations.created_at: datetime.datetime.now(),
                Observations.updated_at: datetime.datetime.now(),
                Observations.file: file_id,
                Observations.observation_type: observation_type,
                Observations.observation_tag: observation_tag,
                Observations.project: project_id,
                Observations.pipeline: pipeline_id,
                Observations.geom: geom_id,
                Observations.user: user_id,
                Observations.score: score,
                Observations.confidence: confidence,
                Observations.observation_method: observation_method_id,
                Observations.taxon_id: taxon_id
                }

    try:
        primary_key = Observations.insert(data_obs).execute()
    except IntegrityError as e:
        if 'duplicate' in str(e):
            raise IntegrityError(f"Uniqueness Violation Detected: {e}") from e
        else:
            raise IntegrityError(f"Another type of integrity error:{e}") from e

    return primary_key


def insert_events(event_id: str, event_type: str) -> str:
    """_summary_

    Parameters
    ----------
    event_type : str
        _description_
    identifier : str
        _description_

    Returns
    -------
    str
        _description_
    """
    data = {Events.id: event_id,
            Events.created_at: datetime.datetime.now(),
            Events.updated_at: datetime.datetime.now(),
            Events.identifier: str(uuid.uuid4()),
            Events.event_type: event_type
            }
    try:
        primary_key = Events.insert(data).execute()
    except IntegrityError as e:
        if 'duplicate' in str(e):
            raise IntegrityError(f"Uniqueness Violation Detected: {e}") from e
        else:
            raise IntegrityError(f"Another type of integrity error:{e}") from e
    return primary_key


def insert_events_files(event_id: str, file_id: str) -> str:
    """
    event = ForeignKeyField(column_name='event_id', field='id', model=Events)
    file = ForeignKeyField(column_name='file_id', field='id', model=Files)
    id = BigAutoField()
    """
    if not Files.select().where(Files.id == file_id).exists():
        raise DoesNotExist(f"File with id {file_id} does not exist in the Files table.")

    if not Events.select().where(Events.id == event_id).exists():
        raise DoesNotExist(f"Event with id {event_id} does not exist in the Events table.")

    data = {
        EventsFiles.event: event_id,
        EventsFiles.file: file_id
    }
    try:
        primary_key = EventsFiles.insert(data).execute()

    except IntegrityError as e:
        if 'duplicate' in str(e):
            raise IntegrityError(f"Uniqueness Violation Detected: {e}") from e

        raise IntegrityError(f"Another type of integrity error:{e}") from e
    return primary_key

# endregion


# region GET FUNCTIONS
def get_obs_method_id(observation_method: str):
    """_summary_

    Parameters
    ----------
    observation_method : str
        _description_

    Returns
    -------
    _type_
        _description_

    Raises
    ------
    ValueError
        _description_
    """
    try:
        obs_method = ObservationMethod.get(ObservationMethod.name == observation_method)
        obs_method_id = obs_method.id
    except DoesNotExist as e:
        logger.error(f"Failed to obtain observation_method_id: {e}")
        raise ValueError("Failed to obtain observation_method_id.") from e
    return str(obs_method_id)


def get_files_data_to_process(mime_type: str, pipeline_name: str,
                              pipeline_version: str, site: str,
                              project_title: str):
    """Retrieve data to process based on MIME type, pipeline, site,
    and project.

    Parameters
    ----------
    mime_type : str
        The MIME type (type and format of data) of the data (options:
        'video/%', 'image/%').
    pipeline_name : str
        Name of the pipeline (from the PipelineInfo table) (e.g., 'filtering_videos_without_wildlife').
    pipeline_version : str
        Version of the pipeline (from the PipelineInfo table)
        (e.g., 'balanced').
    site : str
        The site identifier.
    project_title : str
        The title of the project.

    Returns
    -------
    List[Dict[str, any]]
        A list of dictionaries containing query results.
    """
    try:
        files_query = (
            Files
            .select(
                Files.id.alias('file_id'),
                Files.url,
                Sites.identifier.alias('site_identifier'),
                fn.DATE(
                    SQL(f"CAST(file_metadata->>'{DATETIME[project_title]}' AS text)")).alias('date'),
                fn.SUBSTRING(
                    SQL(f"CAST(file_metadata->>'{DATETIME[project_title]}' AS text)"), 12, 8).alias('time'),
                Files.file_metadata['Longitude'].alias('longitude'),
                Files.file_metadata['Latitude'].alias('latitude'),
                Sites.identifier.alias('site'),
                SamplingAreas.identifier.alias('sampling_area'),
                ProjectDevices.project_serial_number.alias('device'),
                Ecosystems.name.alias('ecosystem'),
                Events.identifier.alias('seq_id')
            )
            .join(SamplingPoints, on=(SamplingPoints.id == Files.sampling_point))
            .join(SamplingAreas, on=(SamplingAreas.id == SamplingPoints.sampling_area))
            .join(Sites, on=(Sites.id == SamplingPoints.site))
            .join(ProjectDevices, on=(SamplingPoints.device == ProjectDevices.id))
            .left_outer_join(EventsFiles, on=(EventsFiles.file == Files.id))
            .left_outer_join(Events, on=(Events.id == EventsFiles.event))
            .left_outer_join(Ecosystems, on=(Ecosystems.id == Sites.ecosystem))
            .where(
                (Files.id.not_in(
                    ProcessedFiles
                    .select(ProcessedFiles.file)
                    .where(
                        ProcessedFiles.pipeline <<
                        PipelineInfo.select(PipelineInfo.id).where(
                            PipelineInfo.name == pipeline_name,
                            PipelineInfo.version == pipeline_version))
                )) &
                (Files.project << Projects.select(Projects.id).where(Projects.title == project_title)) &
                (Files.mime_type ** mime_type) &
                (Sites.identifier == site)
            ).objects()
        )

        # Create a list of dictionaries containing query results.
        results_data = [
            {
                'file_id': str(files.file_id),
                'url': files.url,
                'date': files.date,
                'time': files.time,
                'longitude': files.longitude,
                'latitude': files.latitude,
                'site_identifier': files.site_identifier,
                'sampling_area': files.sampling_area,
                'device': files.device,
                'ecosystem': files.ecosystem,
                'seq_id': files.seq_id
            }
            for files in files_query
        ]
    except DoesNotExist as exc:
        logger.error(
            f"DoesNotExist")
        raise ValueError("Failed to obtain processed files") from exc
    return results_data


def get_processed_data(mime_type: str,
                       project_title: str,
                       site: str,
                       pipeline_name: str,
                       pipeline_version: str):
    """Retrieve data processed by a specific pipeline for a project
    and site.

    Parameters
    ----------
    mime_type : str
        The MIME type (type and format of data) of data (e.g.,
        'image/%', 'video/%').
    project_title : str
        Title of the project (from the Projects table) (e.g.,
        'Indonesia').
    site : str
        Site identifier (from the Sites table) (e.g., '13').
    pipeline_name : str
        Name of the pipeline (from the PipelineInfo table) (e.g., 'filtering_videos_without_wildlife').
    pipeline_version : str
        Version of the pipeline (from the PipelineInfo table) (e.g.,
        'balanced').

    Returns
    -------
    List[Dict[str, any]]
        A list of dictionaries containing query results.
    """
    try:
        query = (Files
                 .select(
                     Files.id.alias('file_id'),
                     Files.url.alias('url'),
                     fn.DATE(
                         SQL(f"CAST(file_metadata->>'{DATETIME[project_title]}' AS text)")).alias('date'),
                     fn.SUBSTRING(
                         SQL(f"CAST(file_metadata->>'{DATETIME[project_title]}' AS text)"), 12, 8).alias('time'),
                     Files.file_metadata['Longitude'].alias('longitude'),
                     Files.file_metadata['Latitude'].alias('latitude'),
                     Sites.identifier.alias('site_identifier'),
                     SamplingAreas.identifier.alias('sampling_area'),
                     ProjectDevices.project_serial_number.alias('device'),
                     Ecosystems.name.alias('ecosystem'),
                     Observations.id.alias('obs_id'),
                     Observations.confidence,
                     Observations.score,
                     Observations.observation_tag,
                     Observations.observation_type,
                     ObservationGeom.bbox,
                     ObservationGeom.video_frame_num,
                     Events.identifier.alias('seq_id')
                 )
                 .join(SamplingPoints, on=(SamplingPoints.id == Files.sampling_point_id))
                 .join(Sites, on=(Sites.id == SamplingPoints.site_id))
                 .join(SamplingAreas, on=(SamplingAreas.id == SamplingPoints.sampling_area_id))

                 .join(ProjectDevices, on=(SamplingPoints.device_id == ProjectDevices.id))
                 .left_outer_join(Ecosystems, on=(Ecosystems.id == Sites.ecosystem_id))
                 .join(Observations, on=(Observations.file_id == Files.id))
                 .join(PipelineInfo,  on=(PipelineInfo.id == Observations.pipeline_id))

                 .left_outer_join(ObservationGeom,  on=(ObservationGeom.id == Observations.geom_id))
                 .left_outer_join(EventsFiles, on=(EventsFiles.file_id == Files.id))
                 .left_outer_join(Events, on=(Events.id == EventsFiles.event_id))
                 .where(
                     PipelineInfo.name == pipeline_name,
                     PipelineInfo.version == pipeline_version,
                     Files.project_id << Projects.select(Projects.id).where(
                         Projects.title == project_title),
                     Files.mime_type ** mime_type,
                     Sites.identifier == site
                 )).objects()

        results_data = [
            {'file_id': str(files.file_id),
             'url': files.url,
             'date': files.date,
             'time': files.time,
             'longitude': files.longitude,
             'latitude': files.latitude,
             'site_identifier': files.site_identifier,
             'sampling_area': files.sampling_area,
             'device': files.device,
             'ecosystem': files.ecosystem,
             'confidence': files.confidence,
             'observation_tag': files.observation_tag,
             'observation_type': files.observation_type,
             'bbox': files.bbox,
             'video_frame_num': files.video_frame_num,
             'classificationProbability': files.confidence,
             'score': files.score,
             'observation_id': files.obs_id,
             'seq_id': files.seq_id
             } for files in query]
    except DoesNotExist:
        logger.error(
            f"DoesNotExist")
        raise ValueError("Failed to obtain processed files")

    return results_data


def get_file_id_by_url(url: str) -> Optional[str]:
    """Retrieve the file ID using the file URL.

    Parameters
    ----------
    url : str
        The URL of the file (from the Files table).

    Returns
    -------
    str
        The file ID if found, or None if the URL doesn't exist in the Files table.
    """
    try:
        files_info = Files.get(
            (Files.url == url)
        )
        files_id = files_info.id
    except Files.DoesNotExist as e:
        logger.error(f"Failed to obtain file_id: {e}")
        raise ValueError("Failed to obtain file_id.")
    return str(files_id)


def get_project_id_by_title(title: str) -> str:
    """Retrieve the project ID using the project title.

    Parameters
    ----------
    title : str
        The title of the project (from the Project table).

    Returns
    -------
    str
        The project ID if found, or None if the title doesn't exist
        in the Project table.
    """

    try:
        project = Projects.get(Projects.title == title)
        project_id = project.id
    except Projects.DoesNotExist as e:
        logger.error(f"Failed to obtain project_id: {e}")
        raise ValueError("Failed to obtain project_id.")
    return str(project_id)


def get_pipeline_id_by_name_version(pipeline_name: str, pipeline_version: str) -> Optional[str]:
    """Retrieve the pipeline ID using the pipeline name and version.

    Parameters
    ----------
    pipeline_name : str
        The name of the pipeline (from the PipelineInfo table).
    pipeline_version : str
        The version of the pipeline (from the PipelineInfo table).

    Returns
    -------
    Optional[str]
        The pipeline ID if found, or None if the pipeline with the
        given name and version doesn't exist.
    """
    try:
        pipeline = PipelineInfo.get(
            (PipelineInfo.name == pipeline_name) &
            (PipelineInfo.version == pipeline_version)
        )
        pipeline_id = pipeline.id
    except PipelineInfo.DoesNotExist as e:
        logger.error(f"Failed to obtain pipeline_id: {e}")
        raise ValueError("Failed to obtain pipeline_id.")
    return str(pipeline_id)


def get_pipeline_execution_params(pipeline_name: str, pipeline_version: str):
    """Retrieve execution parameters for a pipeline.

    Parameters
    ----------
    pipeline_id : str
        The ID of the pipeline.

    Returns
    -------
    List[Dict[str, any]]
        A list of dictionaries containing the execution parameters for
        the specified pipeline.
    """
    try:
        pipeline_info = PipelineInfo.get(
            (PipelineInfo.name == pipeline_name) &
            (PipelineInfo.version == pipeline_version))
    except DoesNotExist:
        logger.error(
            f"DoesNotExist")
        raise ValueError("Failed to obtain execuation_params")

    return [pipeline_info.execution_params]


def get_user_id(username: str) -> Optional[str]:
    """Retrieve the user ID using the username.

    Parameters
    ----------
    username : str
        The username of the user.

    Returns
    -------
    str
        The user ID if found, or None if the username doesn't exist
        in the Users table.
    """
    try:
        users = Users.get(
            Users.username == username
        )
        user_id = users.id
    except Users.DoesNotExist as e:
        logger.error(f"Failed to obtain user_id: {e}")
        raise ValueError("Failed to obtain user_id.")
    return str(user_id)

# TODO: cambiar nombre


def get_files_id_not_in_events(project_title: str, mime_type: str, site_identifier: str):
    """_summary_

    Parameters
    ----------
    project_title : str
        _description_
    mime_type : str
        _description_
    site_identifier : str
        _description_

    Returns
    -------
    _type_
        _description_
    """
    query = (
        Files
        .select(
            Files.id.alias('file_id'),
            Files.url.alias('url'),
            fn.DATE(
                SQL(f"CAST(file_metadata->>'{DATETIME[project_title]}' AS text)")).alias('date'),
            fn.SUBSTRING(
                SQL(f"CAST(file_metadata->>'{DATETIME[project_title]}'  AS text)"), 12, 8).alias('time'),
            Files.file_metadata['Longitude'].alias('longitude'),
            Files.file_metadata['Latitude'].alias('latitude'),
            Sites.identifier.alias('site_identifier'),
            SamplingAreas.identifier.alias('sampling_area'),
            ProjectDevices.project_serial_number.alias('device'),
        )
        .join(SamplingPoints, on=(SamplingPoints.id == Files.sampling_point_id))
        .join(Sites, on=(Sites.id == SamplingPoints.site_id))
        .join(SamplingAreas, on=(SamplingAreas.id == SamplingPoints.sampling_area_id))
        .join(ProjectDevices, on=(SamplingPoints.device_id == ProjectDevices.id))
        .where(
            (Files.id.not_in(
                EventsFiles
                .select(EventsFiles.file_id)
                .join(Files, on=(Files.id == EventsFiles.file_id))
                .join(SamplingPoints, on=(SamplingPoints.id == Files.sampling_point_id))
                .join(Sites, on=(Sites.id == SamplingPoints.site_id))
                .where(
                    (Files.project_id.in_(
                        Projects
                        .select(Projects.id)
                        .where(Projects.title == project_title)
                    ))
                    & (Files.mime_type ** mime_type)
                    & (Sites.identifier == site_identifier)
                )
            ))
            & (Files.project_id.in_(
                Projects
                .select(Projects.id)
                .where(Projects.title == project_title)
            ))
            & (Files.mime_type ** mime_type)
            & (Sites.identifier == site_identifier)
        )
    ).objects()
    results_data = [
        {'file_id': str(files.file_id),
         'url': str(files.url),
         'date': files.date,
         'time': files.time,
         'longitude': files.longitude,
         'latitude': files.latitude,
         'site_identifier': files.site_identifier,
         'sampling_area': files.sampling_area,
         'device': files.device,
         } for files in query]
    return results_data
# endregion

# region DELETE


def delete_observations(project_id: str, site_identifier: str,
                        mime_type: str, pipeline_id: str) -> int:
    """Delete observations that meet specified criteria.

    Parameters
    ----------
    project_id : str
        The ID of the project to filter observations.
    site_identifier : str
        The identifier of the site to filter observations.
    mime_type : str
        The MIME type to filter observations.
    pipeline_id : str
        The ID of the pipeline to filter observations.

    Returns
    -------
    int
        The number of observations deleted.

    This function deletes observations from the Observations table based
    on the provided criteria.
    """
    try:
        query = Observations.delete().where(
            (Observations.file_id <<
                Files.select(Files.id)
                .join(SamplingPoints)
                .join(Sites)
                .join(Projects)
                .where(
                    (Sites.identifier == site_identifier) &
                    (Files.mime_type ** mime_type) &
                    (Projects.id == project_id)
                )
             ) &
            (Observations.pipeline_id == pipeline_id)
        )
        deleted_count = query.execute()
        return deleted_count
    except DoesNotExist:
        return 0


def delete_obs_geom_by_id(geom_id: str) -> int:
    """Delete an observation geometry entry by its ID.

    Parameters
    ----------
    geom_id : str
        The unique identifier of the observation geometry entry.

    Returns
    -------
    int
        The number of observation geometry entries deleted (0 or 1).
    """
    try:
        query = ObservationGeom.delete().where(ObservationGeom.id == geom_id)
        deleted_count = query.execute()
        return deleted_count
    except DoesNotExist:
        return 0


def delete_obs_geom() -> int:
    """Delete observation geometries that are not associated with any
    observation entry.

    This function finds and deletes observation geometries that have no
    associated observation entries.

    Returns
    -------
    int
        Number of element deleted.
    """
    try:
        subquery = Observations.select().where(
            Observations.geom_id == ObservationGeom.id)
        query = ObservationGeom.select().where(~fn.EXISTS(subquery))

        count = 0
        with tqdm(total=query.count(), desc="Deleting Geom IDs") as pbar:
            for geom in query:
                try:
                    deleted_count = delete_obs_geom_by_id(geom)
                    count += deleted_count
                except DoesNotExist:
                    logger.error(f"Geometry with ID {geom.id} not found.")
                except Exception as e:
                    logger.error(
                        f"Failed to delete geometry with ID {geom.id}: {str(e)}")
                pbar.update(1)
    except Exception as e:
        logger.error(f"Error in delete_obs_geom: {str(e)}")
    return count


def delete_processed_files(project_id: str, pipeline_id: str, site_identifier: str, mime_type: str) -> int:
    """Delete processed files associated with a specific pipeline.

    This function deletes processed files from the ProcessedFiles table
    that are associated with the specified pipeline.

    Parameters
    ----------
    pipeline_id : str
        The ID of the pipeline for which processed files should be
        deleted.

    Returns
    -------
    int
        The number of processed files deleted.


    """
    try:
        query = ProcessedFiles.delete().where(
            (ProcessedFiles.file_id <<
                Files.select(Files.id)
                .join(SamplingPoints)
                .join(Sites)
                .join(Projects)
                .where(
                    (Sites.identifier == site_identifier) &
                    (Files.mime_type ** mime_type) &
                    (Projects.id == project_id)
                )
             ) &
            (ProcessedFiles.pipeline_id == pipeline_id)
        )
        deleted_count = query.execute()
        return deleted_count
    except DoesNotExist:
        return 0

# endregion
