import pytest

from ds_db_access.balam.database_queries import insert_events
from ds_db_access.balam.database_queries import insert_events_files
from ds_db_access.balam.database_queries import insert_observations_and_observations_geom
from ds_db_access.balam.database_queries import insert_processed_files
from ds_db_access.balam.database_queries import insert_pipeline_info
from ds_db_access.balam.database_queries import insert_observations_method


from ds_db_access.balam.balam_models import Events
from ds_db_access.balam.balam_models import Observations
from ds_db_access.balam.balam_models import ObservationMethod
from ds_db_access.balam.balam_models import ObservationGeom
from ds_db_access.balam.balam_models import PipelineInfo

from peewee import DoesNotExist, IntegrityError


# region insert_observation_method


@pytest.mark.parametrize(
    ('observation_method_id', 'name', 'description', 'created_at', 'updated_at',),
    [
        ('05a39382-06c6-4271-aef0-f334ef2ef54c',
            'Uruk-hai',
            'annotation made by non-expert',
            '2023-12-15T11:11:15Z',
            '2023-12-15T11:11:15Z',
         ),
        (
            '41f8162b-f6da-4917-92b4-558dbe8e4388',
            'Snaga',
            'annotation made by ds',
            '2023-12-15T11:11:15Z',
            '2023-12-15T11:11:15Z',
        )

    ],
)
def test_insert_observations_method(observation_method_id, name, description, created_at, updated_at):
    """Test Successful Insertion
    """
    primary_key = insert_observations_method(observation_method_id=observation_method_id,
                                             name=name,
                                             description=description,
                                             created_at=created_at,
                                             updated_at=updated_at)

    assert primary_key is not None
    observation_method = ObservationMethod.get(ObservationMethod.id == primary_key)
    assert observation_method.name == name
    assert observation_method.description == description


def test_insert_observations_method_default_values():
    """Test Insertion with Default Description
    """
    observation_method_id = 'c5565f0d-432b-4d8c-8037-9c8247c477cd'
    name = "Stoor"
    primary_key = insert_observations_method(observation_method_id=observation_method_id,
                                             name=name)

    assert primary_key is not None


def test_insert_observations_method_missing_obs_method_id():
    """Test Missing Name, insert an observation method without providing a name
    """
    name = "Fallohide"
    with pytest.raises(IntegrityError):
        insert_observations_method(name=name,
                                   observation_method_id=None)


def test_insert_observations_method_missing_name():
    """Test Missing Name, insert an observation method without providing a name
    """
    observation_method_id = '2025f2b4-b86f-42f9-8096-7353f11568a7'
    with pytest.raises(IntegrityError):
        insert_observations_method(observation_method_id=observation_method_id,
                                   name=None)
# endregion

# region insert_pipeline_info


@pytest.mark.parametrize(
    ('pipeline_id', 'pipeline_name', 'pipeline_version',
     'url_repo_model', 'execution_params', 'comments', 'created_at',
     'updated_at', 'last_exec'),
    [
        ('a0a48006-9a3f-46f4-a8a8-f99b4f298f73',
            'Palantir',
            '1.0.0',
            'https://github.com/saruman',
            {'param1': 1, 'param2': 2},
            'Test comments',
            '2023-12-15T11:11:15Z',
            '2023-12-15T11:11:15Z',
            '2023-12-15T11:11:15Z'
         ),
    ],
)
def test_insert_pipeline_info(pipeline_id, pipeline_name, pipeline_version, url_repo_model, execution_params, comments, created_at, updated_at, last_exec):
    """Test pipeline insertion

    Parameters
    ----------
    pipeline_name : _type_
        _description_
    pipeline_version : _type_
        _description_
    url_repo_model : _type_
        _description_
    execution_params : _type_
        _description_
    comments : _type_
        _description_
    expected_result : _type_
        _description_
    """
    primary_key = insert_pipeline_info(pipeline_id=pipeline_id,
                                       pipeline_name=pipeline_name,
                                       pipeline_version=pipeline_version,
                                       url_repo_model=url_repo_model,
                                       execution_params=execution_params,
                                       comments=comments,
                                       created_at=created_at,
                                       updated_at=updated_at,
                                       last_exec=last_exec
                                       )

    assert primary_key is not None

    pipeline_info = PipelineInfo.get(PipelineInfo.id == primary_key)
    assert pipeline_info.name == pipeline_name
    assert pipeline_info.version == pipeline_version
    assert pipeline_info.url_repo_model == url_repo_model
    assert pipeline_info.execution_params == execution_params
    assert pipeline_info.comments == comments


def test_insert_duplicate_pipeline():
    """Attempting to insert a pipeline with the same name and version
    """
    # Insert a pipeline
    insert_pipeline_info(
        pipeline_id='ab382864-7052-4b1e-9ebb-d17618212932',
        pipeline_name='DuplicatePipeline',
        pipeline_version='1.0.0',
        url_repo_model='https://github.com/duplicate_repo',
        execution_params={'param1': 1, 'param2': 2},
        comments='Duplicate test comments',
        last_exec='2023-12-15T11:11:15Z',
        created_at='2023-12-15T11:11:15Z',
        updated_at='2023-12-15T11:11:15Z'
    )

    # Attempt to insert a pipeline with the same name and version
    with pytest.raises(IntegrityError):
        insert_pipeline_info(
            pipeline_id='ab382864-7052-4b1e-9ebb-d17618212932',
            pipeline_name='DuplicatePipeline',
            pipeline_version='1.0.0',
            url_repo_model='https://github.com/duplicate_repo',
            execution_params={'param1': 1, 'param2': 2},
            comments='Duplicate test comments',
            last_exec='2023-12-15T11:11:15Z',
            created_at='2023-12-15T11:11:15Z',
            updated_at='2023-12-15T11:11:15Z'
        )


@pytest.mark.parametrize(
    ('pipeline_id', 'pipeline_name', 'pipeline_version', 'url_repo_model',
     'execution_params', 'comments'),
    [
        ('ab382864-7052-4b1e-9ebb-d17618212932',
            None,
            '1.0.0',
            'https://github.com/sauron',
            {'param1': 1, 'param2': 2},
            'Test comments',
         ),
        ('ab382864-7052-4b1e-9ebb-d17618212932',
            'One Ring',
            None,
            'https://github.com/sauron',
            {'param1': 1, 'param2': 2},
            'Test comments'
         ),
        ('ab382864-7052-4b1e-9ebb-d17618212932',
            'One Ring',
            '1.1.0',
            None,
            {'param1': 1, 'param2': 2},
            'Test comments'
         ),
        ('ab382864-7052-4b1e-9ebb-d17618212932',
            'One Ring',
            '1.1.0',
            'https://github.com/sauron',
            None,
            'Test comments'
         ),
        ('ab382864-7052-4b1e-9ebb-d17618212932',
            'One Ring',
            '1.1.0',
            'https://github.com/sauron',
            {'param1': 1, 'param2': 2},
            None
         ),
    ],
)
def test_insert_pipeline_witsh_none_arguments(pipeline_id, pipeline_name, pipeline_version, url_repo_model, execution_params, comments):
    """Inserting a pipeline with execution_params set to None
    """

    with pytest.raises(IntegrityError):
        insert_pipeline_info(pipeline_id=pipeline_id,
                             pipeline_name=pipeline_name,
                             pipeline_version=pipeline_version,
                             url_repo_model=url_repo_model,
                             execution_params=execution_params,
                             comments=comments
                             )
# endregion

# region insert_processed_files


@pytest.mark.parametrize(
    ('file_id', 'pipeline_id'),
    [
        (
            '7085a407-d9c5-455a-a4b1-365301363a8e',  # file_id from Balam Database
            'a0a48006-9a3f-46f4-a8a8-f99b4f298f73',  # Palantir
        ),
    ],
)
def test_insert_processed_files(file_id, pipeline_id):
    """_summary_

    Parameters
    ----------
    file_id : _type_
        _description_
    pipeline_id : _type_
        _description_
    expected_result : _type_
        _description_
    """
    primary_key = insert_processed_files(file_id=file_id, pipeline_id=pipeline_id)
    assert primary_key is not None


def test_insert_processed_file_with_nonexistent_file():
    """_summary_
    """
    nonexistent_file_id = 'a0a48006-9a3f-46f4-a8a8-f99b4f298f73'
    pipeline_id = 'a0a48006-9a3f-46f4-a8a8-f99b4f298f73'

    with pytest.raises(DoesNotExist, match=f"Files with id {nonexistent_file_id} does not exist in the Files table."):
        insert_processed_files(file_id=nonexistent_file_id,
                               pipeline_id=pipeline_id)


def test_insert_processed_file_with_nonexistent_pipeline():
    """Attempting to insert a processed file with a non-existent pipeline ID

    Parameters
    ----------
    db_session : _type_
        _description_
    """
    file_id = '7085a407-d9c5-455a-a4b1-365301363a8e'
    nonexistent_pipeline_id = '7085a407-d9c5-455a-a4b1-365301363a8e'

    with pytest.raises(DoesNotExist, match=f"Pipeline with id {nonexistent_pipeline_id} does not exist in the PipelineInfo table."):

        insert_processed_files(file_id=file_id,
                               pipeline_id=nonexistent_pipeline_id)


def test_insert_duplicate_processed_file():
    """Attempting to insert a duplicate processed file record
    """
    insert_processed_files(pipeline_id='ab382864-7052-4b1e-9ebb-d17618212932',
                           file_id='e53dc1ac-6f93-4929-91b4-8da103412b1c')

    with pytest.raises(IntegrityError):
        insert_processed_files(pipeline_id='ab382864-7052-4b1e-9ebb-d17618212932',
                               file_id='e53dc1ac-6f93-4929-91b4-8da103412b1c')

# endregion


# region insert_observations_and_observations_geom


@pytest.mark.parametrize(
    ('file_id',
     'observation_id',
     'observation_type',
     'observation_tag',
     'bbox',
     'score',
     'confidence',
     'project_id',
     'pipeline_id',
     'user_id',
     'observation_method_id',
     'video_frame_num',
     'taxon_id'),
    [
        (
            'e53dc1ac-6f93-4929-91b4-8da103412b1c',
            '76ea9a8f-02db-4ad1-91ee-7c189809a8dd',
            'animal',
            {"predicted_label": "small bird"},
            '0,0,100,100',
            0.9,
            0.95,
            'bb59d359-09e6-4de0-9562-37dba2cd00e4',
            '9837d91b-9ae3-4cea-b4f4-50c6b239c2cd',
            '699c9a06-8f1c-485f-9a57-3a28c905b9da',
            'a92d1b4c-e226-47d8-8d12-f5aaac4cd811',
            1,
            'taxon_id_1',
        ),
        # Add more test cases as needed
    ],
)
def test_insert_observations_and_observations_geom(file_id, observation_id, observation_type, observation_tag, bbox, score, confidence, project_id, pipeline_id, user_id, observation_method_id, video_frame_num, taxon_id):
    """_summary_

    Parameters
    ----------
    file_id : _type_
        _description_
    observation_id : _type_
        _description_
    observation_type : _type_
        _description_
    observation_tag : _type_
        _description_
    bbox : _type_
        _description_
    score : _type_
        _description_
    confidence : _type_
        _description_
    project_id : _type_
        _description_
    pipeline_id : _type_
        _description_
    user_id : _type_
        _description_
    observation_method_id : _type_
        _description_
    video_frame_num : _type_
        _description_
    taxon_id : _type_
        _description_
    expected_result : _type_
        _description_
    """
    result = insert_observations_and_observations_geom(file_id=file_id,
                                                       observation_id=observation_id, observation_type=observation_type, observation_tag=observation_tag,
                                                       bbox=bbox,
                                                       score=score,
                                                       confidence=confidence,
                                                       project_id=project_id,
                                                       pipeline_id=pipeline_id,
                                                       user_id=user_id,
                                                       observation_method_id=observation_method_id, video_frame_num=video_frame_num,
                                                       taxon_id=taxon_id
                                                       )

    observation = Observations.get(Observations.id == result)

    assert observation.observation_type == observation_type
    assert observation.observation_tag == observation_tag
    assert observation.score == score
    assert observation.confidence == confidence
    assert observation.taxon_id == taxon_id

    if bbox is not None:
        observation_geom = ObservationGeom.get(ObservationGeom.id == observation.geom_id)
        assert observation_geom.bbox == bbox
        assert observation_geom.video_frame_num == video_frame_num


def test_insert_observation_with_nonexistent_file():
    """Attempting to insert an observation with a non-existent file ID
    """
    nonexistent_file_id = '9754e2ad-f575-40e3-b568-3c4525ac7421'
    with pytest.raises(DoesNotExist, match=f"Files with id {nonexistent_file_id} does not exist in the Files table."):

        insert_observations_and_observations_geom(
            file_id=nonexistent_file_id,
            observation_id='56d132fd-2f97-44ff-bf56-1e091ea552c6',
            observation_type='animal',
            observation_tag={"predicted_label": "small bird"},
            bbox='0,0,100,100',
            score=0.9,
            confidence=0.95,
            project_id='a500a996-35dd-4fce-a43f-424c41e398a9',  # sipecam
            pipeline_id='9837d91b-9ae3-4cea-b4f4-50c6b239c2cd',
            user_id='699c9a06-8f1c-485f-9a57-3a28c905b9da',
            observation_method_id='a92d1b4c-e226-47d8-8d12-f5aaac4cd811',
            video_frame_num=1,
            taxon_id='taxon_id_1'
        )


def test_insert_observation_with_nonexistent_pipeline():
    """Attempting to insert an observation with a non-existent pipeline ID
    """
    # Assuming 'nonexistent_file_id' is not present in the Files table
    nonexistent_pipeline_id = '9754e2ad-f575-40e3-b568-3c4525ac7421'

    # Use pytest.raises to check if a ValueError is raised
    with pytest.raises(DoesNotExist, match=f"Pipeline with id {nonexistent_pipeline_id} does not exist in the PipelineInfo table."):

        insert_observations_and_observations_geom(
            file_id='95c80f34-14c4-43c0-b1e4-a427742578a2',
            observation_id='2bc90201-fdb0-4ab0-a38b-f02421c8955a',
            observation_type='animal',
            observation_tag={"predicted_label": "small bird"},
            bbox='0,0,100,100',
            score=0.9,
            confidence=0.95,
            project_id='a500a996-35dd-4fce-a43f-424c41e398a9',  # sipecam
            pipeline_id=nonexistent_pipeline_id,
            user_id='699c9a06-8f1c-485f-9a57-3a28c905b9da',
            observation_method_id='a92d1b4c-e226-47d8-8d12-f5aaac4cd811',
            video_frame_num=1,
            taxon_id='taxon_id_1'
        )


def test_insert_observation_with_nonexistent_user():
    """Attempting to insert an observation with a non-existent user ID
    """
    nonexistent_user_id = '9754e2ad-f575-40e3-b568-3c4525ac7421'
    with pytest.raises(DoesNotExist, match=f"Users with id {nonexistent_user_id} does not exist in the Users table."):

        insert_observations_and_observations_geom(
            file_id='95c80f34-14c4-43c0-b1e4-a427742578a2',
            observation_id='c0da7818-aa1e-4235-a82b-7555f645e007',
            observation_type='animal',
            observation_tag={"predicted_label": "small bird"},
            bbox='0,0,100,100',
            score=0.9,
            confidence=0.95,
            project_id='a500a996-35dd-4fce-a43f-424c41e398a9',  # sipecam
            pipeline_id='9837d91b-9ae3-4cea-b4f4-50c6b239c2cd',
            user_id=nonexistent_user_id,
            observation_method_id='a92d1b4c-e226-47d8-8d12-f5aaac4cd811',
            video_frame_num=1,
            taxon_id='taxon_id_1'
        )


def test_insert_observation_with_nonexistent_observation_method():
    """Attempting to insert an observation with a non-existent observation method ID
    """
    nonexistent_obs_method_id = '9754e2ad-f575-40e3-b568-3c4525ac7421'
    with pytest.raises(DoesNotExist, match=f"ObservationMethod with id {nonexistent_obs_method_id} does not exist in the ObservationMethod table."):

        insert_observations_and_observations_geom(
            file_id='95c80f34-14c4-43c0-b1e4-a427742578a2',
            observation_id='e19f25b3-c8b0-4c9a-9835-7efc84c492d7',
            observation_type='animal',
            observation_tag={"predicted_label": "small bird"},
            bbox='0,0,100,100',
            score=0.9,
            confidence=0.95,
            project_id='a500a996-35dd-4fce-a43f-424c41e398a9',  # sipecam
            pipeline_id='9837d91b-9ae3-4cea-b4f4-50c6b239c2cd',
            user_id='699c9a06-8f1c-485f-9a57-3a28c905b9da',
            observation_method_id=nonexistent_obs_method_id,
            video_frame_num=1,
            taxon_id='taxon_id_1'
        )


@pytest.mark.parametrize(
    ('file_id', 'observation_id', 'observation_type', 'observation_tag', 'bbox',
     'score', 'confidence', 'project_id', 'pipeline_id', 'user_id', 'observation_method_id',
     'video_frame_num', 'taxon_id'),
    [
        (
            '95c80f34-14c4-43c0-b1e4-a427742578a2',
            '9ee9b981-2d60-4741-b8ee-5355b93667d2',
            'animal',
            {"predicted_label": "small bird"},
            '0,0,100,100',
            0.9,
            0.95,
            'a500a996-35dd-4fce-a43f-424c41e398a9',  # sipecam
            '9837d91b-9ae3-4cea-b4f4-50c6b239c2cd',
            '699c9a06-8f1c-485f-9a57-3a28c905b9da',
            'a92d1b4c-e226-47d8-8d12-f5aaac4cd811',
            1,
            'taxon_id_1'
        ),
    ],
)
# TODO: add suggestion https://pylint.readthedocs.io/en/latest/user_guide/messages/refactor/too-many-arguments.html
def test_insert_duplicate_observation(file_id,  observation_id,
                                      observation_type,  observation_tag,
                                      bbox, score,  confidence,
                                      project_id,  pipeline_id,
                                      user_id,  observation_method_id,
                                      video_frame_num,  taxon_id):
    """Attempting to insert a duplicate observation record
    """
    insert_observations_and_observations_geom(
        file_id=file_id,
        observation_id=observation_id,
        observation_type=observation_type,
        observation_tag=observation_tag,
        bbox=bbox,
        score=score,
        confidence=confidence,
        project_id=project_id,
        pipeline_id=pipeline_id,
        user_id=user_id,
        observation_method_id=observation_method_id,
        video_frame_num=video_frame_num,
        taxon_id=taxon_id
    )

    with pytest.raises(IntegrityError):
        insert_observations_and_observations_geom(
            file_id=file_id,
            observation_id=observation_id,
            observation_type=observation_type,
            observation_tag=observation_tag,
            bbox=bbox,
            score=score,
            confidence=confidence,
            project_id=project_id,
            pipeline_id=pipeline_id,
            user_id=user_id,
            observation_method_id=observation_method_id,
            video_frame_num=video_frame_num,
            taxon_id=taxon_id
        )


# endregion

# region events


@pytest.mark.parametrize(
    ('event_id', 'event_type'),
    [
        (
            'b24c7ad0-98cf-4057-8e98-c4d803b64e63',
            'video',
        ),
    ],
)
def test_insert_events(event_id, event_type):
    """_summary_

    Parameters
    ----------
    event_id : _type_
        _description_
    event_type : _type_
        _description_
    expected_result : _type_
        _description_
    """
    result = insert_events(event_id=event_id, event_type=event_type)
    event = Events.get(Events.id == result)
    assert event.event_type == event_type


def test_insert_duplicate_event():
    """Attempting to insert an event with a duplicate event ID
    """
    insert_events(event_id='25433691-a649-4688-ac31-41bc534c85fa',
                  event_type='some_event_type')

    with pytest.raises(IntegrityError):
        insert_events(event_id='25433691-a649-4688-ac31-41bc534c85fa',
                      event_type='another_event_type')


# endregion

# region insert_events_files

@pytest.mark.parametrize(
    ('event_id', 'file_id'),
    [
        (
            '25433691-a649-4688-ac31-41bc534c85fa',
            '95c80f34-14c4-43c0-b1e4-a427742578a2'
        ),
    ],
)
def test_insert_events_files(event_id, file_id):
    """insert_events_files function

    Parameters
    ----------
    db_session : _type_
        _description_
    event_id : _type_
        _description_
    file_id : _type_
        _description_
    expected_result : _type_
        _description_
    """
    primary_key = insert_events_files(event_id=event_id,
                                      file_id=file_id)
    assert primary_key is not None


def test_insert_duplicate_events_files():
    """Attempting to insert a duplicate EventsFiles record
    """
    insert_events_files(event_id='b24c7ad0-98cf-4057-8e98-c4d803b64e63',
                        file_id='95c80f34-14c4-43c0-b1e4-a427742578a2')

    with pytest.raises(IntegrityError):
        insert_events_files(event_id='b24c7ad0-98cf-4057-8e98-c4d803b64e63',
                            file_id='95c80f34-14c4-43c0-b1e4-a427742578a2')


def test_insert_non_existing_event_id_eventsfiles():
    """_summary_
    """
    with pytest.raises(DoesNotExist):
        # pytest.raises(DoesNotExist, match=f"ObservationMethod with id {nonexistent_obs_method_id} does not exist in the ObservationMethod table.")
        insert_events_files(event_id='0c888139-8103-4f4f-af52-cac73abe6743',
                            file_id='95c80f34-14c4-43c0-b1e4-a427742578a2')


def test_insert_non_existing_file_id_eventsfiles():
    """_summary_
    """
    with pytest.raises(DoesNotExist):
        # pytest.raises(DoesNotExist, match=f"ObservationMethod with id {nonexistent_obs_method_id} does not exist in the ObservationMethod table.")
        insert_events_files(event_id='25433691-a649-4688-ac31-41bc534c85fa',
                            file_id='bb2d6dc1-ec29-4bc1-b277-af18a87821e7')
# endregion

# region get function


def test_get_nonexistent_event():
    """Attempting to retrieve a non-existent event
    """
    with pytest.raises(DoesNotExist):
        # pytest.raises(DoesNotExist, match=f"ObservationMethod with id {nonexistent_obs_method_id} does not exist in the ObservationMethod table.")
        Events.get(Events.id == '7202aed1-86b1-4495-8707-93c35921f434')


# endregion
