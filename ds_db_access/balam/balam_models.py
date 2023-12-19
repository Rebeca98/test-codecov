from peewee import *
from playhouse.postgres_ext import *
from dotenv import load_dotenv
import os

dotenv_path = os.getenv("DOTENV_PATH")

load_dotenv(dotenv_path)

# TEST
DB_BALAM_NAME = os.environ.get("DB_BALAM_TEST_NAME", "")
DB_BALAM_HOST = os.environ.get("DB_BALAM_TEST_HOST", "")
DB_BALAM_PASSWORD = os.environ.get("DB_BALAM_TEST_PASSWORD", "")
DB_BALAM_USER = os.environ.get("DB_BALAM_TEST_USER", "")
DB_BALAM_PORT = os.environ.get("DB_BALAM_TEST_PORT", "")


# PROD
# DB_BALAM_NAME = os.environ.get("DB_BALAM_NAME", "")
# DB_BALAM_HOST = os.environ.get("DB_BALAM_HOST", "")
# DB_BALAM_PASSWORD = os.environ.get("DB_BALAM_PASSWORD", "")
# DB_BALAM_USER = os.environ.get("DB_BALAM_USER", "")
# DB_BALAM_PORT = os.environ.get("DB_BALAM_PORT", "")

database = PostgresqlDatabase(DB_BALAM_NAME, **{'host': DB_BALAM_HOST, 'port': int(
    DB_BALAM_PORT), 'user': DB_BALAM_USER, 'password': DB_BALAM_PASSWORD})


class UnknownField(object):
    def __init__(self, *_, **__): pass


class BaseModel(Model):
    class Meta:
        database = database


class FileMetadataSchemas(BaseModel):
    created_at = DateTimeField()
    files_mime_type = CharField()
    id = UUIDField(primary_key=True)
    name = CharField(index=True)
    schema = BinaryJSONField()
    updated_at = DateTimeField()

    class Meta:
        table_name = 'FileMetadataSchemas'


class Projects(BaseModel):
    contacts = CharField(null=True)
    countries = CharField()
    created_at = DateTimeField()
    description = TextField()
    duration = CharField(null=True)
    id = UUIDField(primary_key=True)
    project_configuration = IntegerField()
    project_hash = CharField(index=True)
    sequence_interval = IntegerField(null=True)
    shortname = CharField(index=True)
    temporality = CharField(null=True)
    title = CharField()
    updated_at = DateTimeField()

    class Meta:
        table_name = 'Projects'
        indexes = (
            (('title', 'shortname', 'project_hash'), True),
        )


class Ecosystems(BaseModel):
    created_at = DateTimeField()
    id = UUIDField(primary_key=True)
    name = CharField(index=True)
    updated_at = DateTimeField()

    class Meta:
        table_name = 'Ecosystems'


class Sites(BaseModel):
    created_at = DateTimeField()
    description = TextField(null=True)
    ecosystem = ForeignKeyField(column_name='ecosystem_id', field='id',
                                model=Ecosystems, null=True)
    geometry = UnknownField(index=True, null=True)  # USER-DEFINED
    id = UUIDField(primary_key=True)
    identifier = CharField(index=True)
    metadata = BinaryJSONField(null=True)
    project = ForeignKeyField(column_name='project_id', field='id', model=Projects, null=True)
    updated_at = DateTimeField()

    class Meta:
        table_name = 'Sites'


class SamplingAreas(BaseModel):
    center_of_area = UnknownField(index=True, null=True)  # USER-DEFINED
    created_at = DateTimeField()
    description = TextField(null=True)
    ecosystem = ForeignKeyField(column_name='ecosystem_id', field='id',
                                model=Ecosystems, null=True)
    id = UUIDField(primary_key=True)
    identifier = CharField(null=True)
    metadata = BinaryJSONField(null=True)
    project = ForeignKeyField(column_name='project_id', field='id', model=Projects, null=True)
    radius_of_area = DoubleField(null=True)
    site = ForeignKeyField(column_name='site_id', field='id', model=Sites)
    updated_at = DateTimeField()

    class Meta:
        table_name = 'SamplingAreas'


class Users(BaseModel):
    can_login = BooleanField()
    date_joined = DateTimeField()
    email = CharField()
    first_name = CharField()
    id = UUIDField(primary_key=True)
    is_active = BooleanField()
    is_curator = BooleanField()
    is_staff = BooleanField()
    is_superuser = BooleanField()
    last_login = DateTimeField(null=True)
    last_name = CharField()
    observation_count = IntegerField()
    observation_score = DoubleField()
    password = CharField()
    updated_at = DateTimeField()
    username = CharField(index=True)

    class Meta:
        table_name = 'Users'


class CalendarActivities(BaseModel):
    activity_type = CharField()
    color = CharField()
    created_at = DateTimeField()
    description = TextField(null=True)
    end_date = DateTimeField()
    id = UUIDField(primary_key=True)
    project = ForeignKeyField(column_name='project_id', field='id', model=Projects, null=True)
    sampling_area = ForeignKeyField(column_name='sampling_area_id',
                                    field='id', model=SamplingAreas, null=True)
    site = ForeignKeyField(column_name='site_id', field='id', model=Sites, null=True)
    start_date = DateTimeField()
    title = CharField()
    updated_at = DateTimeField()
    user = ForeignKeyField(column_name='user_id', field='id', model=Users, null=True)

    class Meta:
        table_name = 'CalendarActivities'


class DeviceConfigs(BaseModel):
    config = BinaryJSONField()
    config_type = CharField()
    created_at = DateTimeField()
    id = UUIDField(primary_key=True)
    name = CharField()
    updated_at = DateTimeField()

    class Meta:
        table_name = 'DeviceConfigs'


class Devices(BaseModel):
    brand = CharField()
    created_at = DateTimeField()
    id = UUIDField(primary_key=True)
    serial_number = CharField(index=True)
    updated_at = DateTimeField()

    class Meta:
        table_name = 'Devices'


class ProjectDevices(BaseModel):
    created_at = DateTimeField()
    device_config = ForeignKeyField(column_name='device_config_id',
                                    field='id', model=DeviceConfigs, null=True)
    device = ForeignKeyField(column_name='device_id', field='id', model=Devices, null=True)
    device_type = CharField()
    id = UUIDField(primary_key=True)
    project = ForeignKeyField(column_name='project_id', field='id', model=Projects)
    project_serial_number = CharField()
    status = CharField()
    updated_at = DateTimeField()

    class Meta:
        table_name = 'ProjectDevices'
        indexes = (
            (('project', 'project_serial_number'), True),
        )


class SamplingPoints(BaseModel):
    activity = ForeignKeyField(column_name='activity_id', field='id',
                               model=CalendarActivities, null=True)
    additional_identifier = CharField(null=True)
    altitude = DoubleField(null=True)
    created_at = DateTimeField()
    date_collected = DateTimeField(null=True)
    date_deployment = DateTimeField(null=True)
    description = TextField(null=True)
    device_config = CharField(null=True)
    device = ForeignKeyField(column_name='device_id', field='id', model=ProjectDevices, null=True)
    ecosystem = ForeignKeyField(column_name='ecosystem_id', field='id',
                                model=Ecosystems, null=True)
    id = UUIDField(primary_key=True)
    identifier = CharField(null=True)
    location = UnknownField(index=True, null=True)  # USER-DEFINED
    metadata = BinaryJSONField(null=True)
    project = ForeignKeyField(column_name='project_id', field='id', model=Projects, null=True)
    sampling_area = ForeignKeyField(column_name='sampling_area_id',
                                    field='id', model=SamplingAreas, null=True)
    site = ForeignKeyField(column_name='site_id', field='id', model=Sites, null=True)
    updated_at = DateTimeField()

    class Meta:
        table_name = 'SamplingPoints'


class Files(BaseModel):
    created_at = DateTimeField()
    file_metadata = BinaryJSONField(null=True)
    id = UUIDField(primary_key=True)
    metadata_schema = ForeignKeyField(
        column_name='metadata_schema_id', field='id', model=FileMetadataSchemas, null=True)
    mime_type = CharField()
    name = CharField(index=True)
    project = ForeignKeyField(column_name='project_id', field='id', model=Projects)
    sampling_point = ForeignKeyField(column_name='sampling_point_id',
                                     field='id', model=SamplingPoints, null=True)
    updated_at = DateTimeField()
    url = CharField(index=True)

    class Meta:
        table_name = 'Files'


class AcousticIndicesMetadata(BaseModel):
    created_at = DateTimeField()
    description = TextField(null=True)
    file = ForeignKeyField(column_name='file_id', field='id', model=Files)
    files_acoustic_index = BinaryJSONField(null=True)
    files_acoustic_index_per_bin = BinaryJSONField(null=True)
    id = UUIDField(primary_key=True)
    identifier = CharField(null=True)
    updated_at = DateTimeField()

    class Meta:
        table_name = 'AcousticIndicesMetadata'


class Indices(BaseModel):
    created_at = DateTimeField()
    id = UUIDField(primary_key=True)
    name = CharField(index=True)
    type = CharField(index=True)
    updated_at = DateTimeField()

    class Meta:
        table_name = 'Indices'


class AcousticIndicesMetadataIndices(BaseModel):
    acousticindicesmetadata = ForeignKeyField(
        column_name='acousticindicesmetadata_id', field='id', model=AcousticIndicesMetadata)
    id = BigAutoField()
    indices = ForeignKeyField(column_name='indices_id', field='id', model=Indices)

    class Meta:
        table_name = 'AcousticIndicesMetadata_indices'
        indexes = (
            (('acousticindicesmetadata', 'indices'), True),
        )


class ActivityRoutes(BaseModel):
    activity = ForeignKeyField(column_name='activity_id', field='id', model=CalendarActivities)
    comments_route = CharField(null=True)
    created_at = DateTimeField()
    id = UUIDField(primary_key=True)
    rotation = CharField(null=True)
    route = UnknownField(index=True, null=True)  # USER-DEFINED
    route_type = CharField(null=True)
    updated_at = DateTimeField()

    class Meta:
        table_name = 'ActivityRoutes'


class CalendarActivitiesDevices(BaseModel):
    calendaractivities = ForeignKeyField(
        column_name='calendaractivities_id', field='id', model=CalendarActivities)
    id = BigAutoField()
    projectdevice = ForeignKeyField(column_name='projectdevice_id',
                                    field='id', model=ProjectDevices)

    class Meta:
        table_name = 'CalendarActivities_devices'
        indexes = (
            (('calendaractivities', 'projectdevice'), True),
        )


class Monitors(BaseModel):
    created_at = DateTimeField()
    email = CharField(null=True)
    first_name = CharField()
    id = UUIDField(primary_key=True)
    last_name = CharField(null=True)
    nationality = CharField(null=True)
    phone_number = CharField(null=True)
    role = CharField()
    updated_at = DateTimeField()

    class Meta:
        table_name = 'Monitors'


class CalendarActivitiesMonitors(BaseModel):
    calendaractivities = ForeignKeyField(
        column_name='calendaractivities_id', field='id', model=CalendarActivities)
    id = BigAutoField()
    monitor = ForeignKeyField(column_name='monitor_id', field='id', model=Monitors)

    class Meta:
        table_name = 'CalendarActivities_monitors'
        indexes = (
            (('calendaractivities', 'monitor'), True),
        )


class DeviceConfigsProject(BaseModel):
    deviceconfig = ForeignKeyField(column_name='deviceconfig_id', field='id', model=DeviceConfigs)
    id = BigAutoField()
    project = ForeignKeyField(column_name='project_id', field='id', model=Projects)

    class Meta:
        table_name = 'DeviceConfigs_project'
        indexes = (
            (('deviceconfig', 'project'), True),
        )


class Events(BaseModel):
    created_at = DateTimeField()
    event_type = CharField()
    id = UUIDField(primary_key=True)
    identifier = CharField()
    updated_at = DateTimeField()

    class Meta:
        table_name = 'Events'


class EventsFiles(BaseModel):
    event = ForeignKeyField(column_name='event_id', field='id', model=Events)
    file = ForeignKeyField(column_name='file_id', field='id', model=Files)
    id = BigAutoField()

    class Meta:
        table_name = 'Events_files'
        indexes = (
            (('event', 'file'), True),
        )


class AuthGroup(BaseModel):
    name = CharField(index=True)

    class Meta:
        table_name = 'auth_group'


class GroupProfiles(BaseModel):
    created_at = DateTimeField()
    description = TextField()
    group = ForeignKeyField(column_name='group_id', field='id', model=AuthGroup, unique=True)
    id = UUIDField(primary_key=True)
    slug = CharField(index=True)
    updated_at = DateTimeField()

    class Meta:
        table_name = 'GroupProfiles'


class GroupProfilesProjects(BaseModel):
    groupprofile = ForeignKeyField(column_name='groupprofile_id', field='id', model=GroupProfiles)
    id = BigAutoField()
    project = ForeignKeyField(column_name='project_id', field='id', model=Projects)

    class Meta:
        table_name = 'GroupProfiles_projects'
        indexes = (
            (('groupprofile', 'project'), True),
        )


class ObservationGeom(BaseModel):
    bbox = CharField()
    created_at = DateTimeField()
    id = UUIDField(primary_key=True)
    updated_at = DateTimeField()
    video_frame_num = IntegerField(null=True)

    class Meta:
        table_name = 'ObservationGeom'


class ObservationMethod(BaseModel):
    created_at = DateTimeField()
    description = TextField(null=True)
    id = UUIDField(primary_key=True)
    name = CharField()
    updated_at = DateTimeField()

    class Meta:
        table_name = 'ObservationMethod'


class PipelineInfo(BaseModel):
    comments = CharField()
    created_at = DateTimeField()
    execution_params = BinaryJSONField()
    id = UUIDField(primary_key=True)
    last_execution = DateTimeField(null=True)
    name = CharField()
    updated_at = DateTimeField()
    url_repo_model = CharField()
    version = CharField()

    class Meta:
        table_name = 'PipelineInfo'


class Observations(BaseModel):
    behaviour = CharField(null=True)
    comments = CharField(null=True)
    confidence = DoubleField(null=True)
    count = IntegerField(null=True)
    created_at = DateTimeField()
    end_time = DateTimeField(null=True)
    file = ForeignKeyField(column_name='file_id', field='id', model=Files)
    geom = ForeignKeyField(column_name='geom_id', field='id',
                           model=ObservationGeom, null=True, unique=True)
    high_frequency = DoubleField(null=True)
    id = UUIDField(primary_key=True)
    individual_id = CharField(null=True)
    is_setup_or_pickup = BooleanField(null=True)
    life_stage = CharField(null=True)
    low_frequency = DoubleField(null=True)
    observation_method = ForeignKeyField(
        column_name='observation_method_id', field='id', model=ObservationMethod, null=True)
    observation_tag = BinaryJSONField(null=True)
    observation_type = CharField(null=True)
    pipeline = ForeignKeyField(column_name='pipeline_id', field='id',
                               model=PipelineInfo, null=True)
    project = ForeignKeyField(column_name='project_id', field='id', model=Projects)
    score = DoubleField(null=True)
    sex = CharField(null=True)
    start_time = DateTimeField(null=True)
    taxon_id = CharField(null=True)
    updated_at = DateTimeField()
    user = ForeignKeyField(column_name='user_id', field='id', model=Users)
    votes = IntegerField(null=True)

    class Meta:
        table_name = 'Observations'


class ObservationVotes(BaseModel):
    created_at = DateTimeField()
    id = UUIDField(primary_key=True)
    observation = ForeignKeyField(column_name='observation_id', field='id', model=Observations)
    score = IntegerField()
    updated_at = DateTimeField()
    user = ForeignKeyField(column_name='user_id', field='id', model=Users)
    vote_weight = IntegerField()

    class Meta:
        table_name = 'ObservationVotes'


class ProcessedFiles(BaseModel):
    created_at = DateTimeField()
    file = ForeignKeyField(column_name='file_id', field='id', model=Files)
    id = UUIDField(primary_key=True)
    pipeline = ForeignKeyField(column_name='pipeline_id', field='id', model=PipelineInfo)
    updated_at = DateTimeField()

    class Meta:
        table_name = 'ProcessedFiles'


class ProjectManagementLastdeviceserialnumber(BaseModel):
    id = BigAutoField()
    last_serial_number = IntegerField()
    project = ForeignKeyField(column_name='project_id', field='id', model=Projects, unique=True)

    class Meta:
        table_name = 'ProjectManagement_lastdeviceserialnumber'


class ProjectsGroups(BaseModel):
    groupprofile = ForeignKeyField(column_name='groupprofile_id', field='id', model=GroupProfiles)
    id = BigAutoField()
    project = ForeignKeyField(column_name='project_id', field='id', model=Projects)

    class Meta:
        table_name = 'Projects_groups'
        indexes = (
            (('project', 'groupprofile'), True),
        )


class SessionTokens(BaseModel):
    activity = ForeignKeyField(column_name='activity_id', field='id', model=CalendarActivities)
    created_at = DateTimeField()
    expiration_date = DateTimeField()
    id = UUIDField(primary_key=True)
    last_used = DateField(null=True)
    monitor = ForeignKeyField(column_name='monitor_id', field='id', model=Monitors)
    project = ForeignKeyField(column_name='project_id', field='id', model=Projects)
    token = CharField(index=True)
    updated_at = DateTimeField()
    used = BooleanField()

    class Meta:
        table_name = 'SessionTokens'


class UsersGroups(BaseModel):
    group = ForeignKeyField(column_name='group_id', field='id', model=AuthGroup)
    id = BigAutoField()
    user = ForeignKeyField(column_name='user_id', field='id', model=Users)

    class Meta:
        table_name = 'Users_groups'
        indexes = (
            (('user', 'group'), True),
        )


class DjangoContentType(BaseModel):
    app_label = CharField()
    model = CharField()

    class Meta:
        table_name = 'django_content_type'
        indexes = (
            (('app_label', 'model'), True),
        )


class AuthPermission(BaseModel):
    codename = CharField()
    content_type = ForeignKeyField(column_name='content_type_id',
                                   field='id', model=DjangoContentType)
    name = CharField()

    class Meta:
        table_name = 'auth_permission'
        indexes = (
            (('content_type', 'codename'), True),
        )


class UsersUserPermissions(BaseModel):
    id = BigAutoField()
    permission = ForeignKeyField(column_name='permission_id', field='id', model=AuthPermission)
    user = ForeignKeyField(column_name='user_id', field='id', model=Users)

    class Meta:
        table_name = 'Users_user_permissions'
        indexes = (
            (('user', 'permission'), True),
        )


class AuthGroupPermissions(BaseModel):
    group = ForeignKeyField(column_name='group_id', field='id', model=AuthGroup)
    id = BigAutoField()
    permission = ForeignKeyField(column_name='permission_id', field='id', model=AuthPermission)

    class Meta:
        table_name = 'auth_group_permissions'
        indexes = (
            (('group', 'permission'), True),
        )


class DjangoAdminLog(BaseModel):
    action_flag = SmallIntegerField()
    action_time = DateTimeField()
    change_message = TextField()
    content_type = ForeignKeyField(column_name='content_type_id',
                                   field='id', model=DjangoContentType, null=True)
    object_id = TextField(null=True)
    object_repr = CharField()
    user = ForeignKeyField(column_name='user_id', field='id', model=Users)

    class Meta:
        table_name = 'django_admin_log'


class DjangoMigrations(BaseModel):
    app = CharField()
    applied = DateTimeField()
    id = BigAutoField()
    name = CharField()

    class Meta:
        table_name = 'django_migrations'


class DjangoSession(BaseModel):
    expire_date = DateTimeField(index=True)
    session_data = TextField()
    session_key = CharField(primary_key=True)

    class Meta:
        table_name = 'django_session'


class SpatialRefSys(BaseModel):
    auth_name = CharField(null=True)
    auth_srid = IntegerField(null=True)
    proj4text = CharField(null=True)
    srid = AutoField()
    srtext = CharField(null=True)

    class Meta:
        table_name = 'spatial_ref_sys'
