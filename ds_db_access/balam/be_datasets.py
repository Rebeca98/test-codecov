from __future__ import annotations
import numpy as np
import os
import pandas as pd
import uuid

from be_ml_vision.datasets.media import BeMediaDataset
from be_ml_vision.datasets.images import BeImageDataset, BeImagePredictionDataset
from be_ml_vision.datasets.video import BeVideoDataset, BeVideoPredictionDataset

from ds_db_access.balam.utils_models import get_pipeline_id, get_data_processed
from ds_db_access.balam.utils_models import get_data_to_process
from ds_db_access.balam.utils_models import insert_observations
from ds_db_access.balam.utils_models import get_project_by_title
from ds_db_access.balam.utils_models import get_files_with_no_events
from ds_db_access.balam.utils_models import insert_events_table
from ds_db_access.balam.params_db import S3_PATH

# TODO: change ObservationMediaDataset to BalamMediaDataset


class ObservationMediaDataset(BeMediaDataset):
    class MEDIA_FIELDS(BeMediaDataset.MEDIA_FIELDS):
        SAMPLING_AREA = 'sampling_area'
        SITE = 'site'
        DEVICE = 'device'
        ECOSYSTEM = 'ecosystem'
        URL = 'url'
        PROJECT_TITLE = 'project'

    class ANNOTATIONS_FIELDS(BeMediaDataset.ANNOTATIONS_FIELDS):
        BEHAVIOUR = 'behaviour'
        COUNT = 'count'
        CREATED_AT = 'created_at'
        INDIVIDUAL_ID = 'individual_id'
        IS_SETUP_OR_PICKUP = 'is_setup_or_pickup'
        LIFE_STAGE = 'life_stage'
        OBSERVATION_METHOD = 'observation_method'
        OBSERVATION_TAG = 'observation_tag'
        OBSERVATION_TYPE = 'observation_type'
        SEX = 'sex'

        TYPES = {
            **BeMediaDataset.ANNOTATIONS_FIELDS.TYPES,
            COUNT: int,
            INDIVIDUAL_ID: str,
            IS_SETUP_OR_PICKUP: bool
        }

    @classmethod
    def from_pipeline_results(cls,
                              project_title,
                              site,
                              pipeline_name,
                              pipeline_version,
                              **kwargs) -> ObservationMediaDataset:
        filetype = kwargs.get('filetype')
        if filetype is None:
            raise Exception("You must send the parameter filetype")

        data = get_data_processed(filetype=filetype,
                                  project_title=project_title,
                                  site=site,
                                  pipeline_name=pipeline_name,
                                  pipeline_version=pipeline_version)

        if data is not None and len(data) > 0:
            data = cls.map_fields_from_db_schema(data, filetype)
        else:
            raise Exception("No data found from pipeline results")

        ds_instance = cls(data, **kwargs)

        return ds_instance

    @classmethod
    def from_not_processed_data(cls,
                                project_title: str,
                                site: str,
                                pipeline_name: str,
                                pipeline_version: str,
                                **kwargs) -> ObservationMediaDataset:
        filetype = kwargs.get('filetype')
        if filetype is None:
            raise Exception("You must send the parameter filetype")

        data = get_data_to_process(
            project_title=project_title,
            site=site,
            filetype=filetype,
            pipeline_name=pipeline_name,
            pipeline_version=pipeline_version
        )

        if data is not None and len(data) > 0:
            data = cls.map_fields_from_db_schema(data, filetype)
            return cls(data, **kwargs)
        else:
            raise Exception("No data found for not processed data")

    @classmethod
    def get_new_data(cls,
                     project_title: str,
                     filetype: str,
                     site: str,
                     **kwargs) -> ObservationMediaDataset:
        data = get_files_with_no_events(project_title=project_title,
                                        filetype=filetype,
                                        site_identifier=site)
        if data is not None:
            data = cls.map_fields_from_db_schema(data, filetype)
            return cls(data, **kwargs)
        else:
            raise Exception("No new data found")

    def store_observations(self,
                           project_title: str,
                           pipeline_name: str,
                           pipeline_version: str):
        observations_df = self.as_dataframe()
        media_dir = self.get_media_dir()
        if media_dir is not None and media_dir != '':
            observations_df[self.ANNOTATIONS_FIELDS.ITEM] = (
                observations_df[self.ANNOTATIONS_FIELDS.ITEM]
                .apply(lambda x: os.path.relpath(x, media_dir)))
       # observations_df['observation_type'] = observations_df['label'].apply(
       #     lambda x: 'empty' if x == 'empty' else 'person' if x == 'person' else 'animal')
        observations_df = self.map_fields_to_db_schema(observations_df)

        try:
            project_id = get_project_by_title(project_title)
        except ValueError as e:
            print(f"Error: {e}")
            return

        pipeline_id = None
        observation_method = 'human'
        if pipeline_name is not None and pipeline_version is not None:
            try:
                pipeline_id = get_pipeline_id(pipeline_name=pipeline_name,
                                              pipeline_version=pipeline_version)
            except ValueError as e:
                print(f"Error: {e}")
                return
            observation_method = 'machine'

        insert_observations(observations_df=observations_df,
                            project_id=project_id,
                            pipeline_id=pipeline_id,
                            username='rebe',
                            observation_method=observation_method,
                            s3_path=S3_PATH[project_title])

    def store_events(self, filetype):
        new_data_df = self.as_dataframe()
        new_data_df = self.map_fields_to_db_schema(new_data_df)
        insert_events_table(filetype=filetype, data_df=new_data_df)

    @classmethod
    def map_fields_from_db_schema(cls, data: pd.DataFrame, filetype: str):
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
        if filetype.startswith('image'):
            media_id_field = cls.ANNOTATIONS_FIELDS.MEDIA_ID
        else:
            media_id_field = cls.ANNOTATIONS_FIELDS.MEDIA_ID

        column_mappings = {'file_id': media_id_field,
                           'file_path': cls.ANNOTATIONS_FIELDS.ITEM,
                           'timestamp': cls.MEDIA_FIELDS.DATE_CAPTURED
                           }

        data = data.rename(
            columns=column_mappings)

        if 'observation_type' not in data.columns:
            data['observation_type'] = None

        if 'observation_id' in data.columns:
            data = data.rename(
                columns={'observation_id': cls.ANNOTATIONS_FIELDS.ID})

        data[cls.ANNOTATIONS_FIELDS.LABEL] = (
            data.apply(lambda row: row.get(cls.ANNOTATIONS_FIELDS.TAXON_ID,
                                           row['observation_type']),
                       axis=1))
        if 'device' in data.columns:
            data[cls.MEDIA_FIELDS.LOCATION] = (
                data.apply(
                    lambda row: f"{row.get('site_identifier')}-{row.get('sampling_area')}-{row['device']}",
                    axis=1))

        return data

    @classmethod
    def map_fields_to_db_schema(cls, data: pd.DataFrame):
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
        column_mappings = {
            cls.ANNOTATIONS_FIELDS.ITEM: 'file_path'
        }
        data = data.rename(columns=column_mappings)

        essential_columns = ['video_frame_num',
                             'bbox', 'confidence', 'score', 'taxon_id']

        for column in essential_columns:
            if column not in data.columns:
                data[column] = None

        # if 'observation_type' in data.columns:
        #    if 'observation_tag' not in data.columns:
        if 'label' in data.columns:
            data['observation_type'] = data[cls.ANNOTATIONS_FIELDS.LABEL].apply(
                lambda x: 'empty' if x == 'empty' else 'person' if x == 'person' else 'animal')
            data['observation_tag'] = data.apply(
                lambda row: row.get(cls.ANNOTATIONS_FIELDS.LABEL), axis=1)
        # else:
        # data['observation_type'] = data.apply(
        #    lambda row: row.get(cls.ANNOTATIONS_FIELDS.LABEL), axis=1)
        # data['observation_tag'] = None

        return data


class ObservationsImageDataset(ObservationMediaDataset, BeImageDataset):
    class MEDIA_FIELDS(ObservationMediaDataset.MEDIA_FIELDS, BeImageDataset.MEDIA_FIELDS):
        TYPES = {
            **ObservationMediaDataset.MEDIA_FIELDS.TYPES,
            **BeImageDataset.MEDIA_FIELDS.TYPES
        }

    class ANNOTATIONS_FIELDS(ObservationMediaDataset.ANNOTATIONS_FIELDS,
                             BeImageDataset.ANNOTATIONS_FIELDS):
        TYPES = {
            **ObservationMediaDataset.ANNOTATIONS_FIELDS.TYPES,
            **BeImageDataset.ANNOTATIONS_FIELDS.TYPES
        }

    @classmethod
    def from_pipeline_results(cls,
                              project_title,
                              site,
                              pipeline_name,
                              pipeline_version,
                              **kwargs) -> ObservationsImageDataset:
        return super().from_pipeline_results(project_title=project_title,
                                             site=site,
                                             pipeline_name=pipeline_name,
                                             pipeline_version=pipeline_version,
                                             filetype='image',
                                             **kwargs)

    @classmethod
    def from_not_processed_data(cls,
                                project_title: str,
                                site: str,
                                pipeline_name: str,
                                pipeline_version: str,
                                **kwargs) -> ObservationsImageDataset:
        return super().from_not_processed_data(project_title=project_title,
                                               site=site,
                                               pipeline_name=pipeline_name,
                                               pipeline_version=pipeline_version,
                                               filetype='image',
                                               **kwargs)

    def set_seq_ids(self, min_interval=2):
        df = self.as_dataframe()
        df[self.MEDIA_FIELDS.DATE_CAPTURED] = pd.to_datetime(df[self.MEDIA_FIELDS.DATE_CAPTURED])
        locs = df[self.MEDIA_FIELDS.LOCATION].unique()

        item_to_seq_id = {}

        for loc in locs:
            df_loc = (
                df[df[self.MEDIA_FIELDS.LOCATION] == loc]
                .sort_values(self.MEDIA_FIELDS.DATE_CAPTURED, ascending=True, inplace=False)
            )
            df_loc['secs_diff'] = (
                df_loc[self.MEDIA_FIELDS.DATE_CAPTURED]
                .diff()
                .fillna(pd.Timedelta(seconds=0)) / np.timedelta64(1, 's'))

            last_seq_id = str(uuid.uuid4())
            for _, row in df_loc.iterrows():
                if row['secs_diff'] > min_interval:
                    last_seq_id = str(uuid.uuid4())
                item_to_seq_id[row['item']] = last_seq_id

        self.set_field_values(
            self.MEDIA_FIELDS.SEQ_ID, lambda row: item_to_seq_id[row['item']], inplace=True)


class ObservationsImagePredictionDataset(ObservationsImageDataset, BeImagePredictionDataset):
    class MEDIA_FIELDS(ObservationsImageDataset.MEDIA_FIELDS,
                       BeImagePredictionDataset.MEDIA_FIELDS):
        TYPES = {
            **ObservationsImageDataset.MEDIA_FIELDS.TYPES,
            **BeImagePredictionDataset.MEDIA_FIELDS.TYPES
        }

    class ANNOTATIONS_FIELDS(ObservationsImageDataset.ANNOTATIONS_FIELDS,
                             BeImagePredictionDataset.ANNOTATIONS_FIELDS):
        TYPES = {
            **ObservationsImageDataset.ANNOTATIONS_FIELDS.TYPES,
            **BeImagePredictionDataset.ANNOTATIONS_FIELDS.TYPES
        }


class ObservationsVideoDataset(ObservationMediaDataset, BeVideoDataset):
    class MEDIA_FIELDS(ObservationMediaDataset.MEDIA_FIELDS, BeVideoDataset.MEDIA_FIELDS):
        TYPES = {
            **ObservationMediaDataset.MEDIA_FIELDS.TYPES,
            **BeVideoDataset.MEDIA_FIELDS.TYPES
        }

    class ANNOTATIONS_FIELDS(ObservationMediaDataset.ANNOTATIONS_FIELDS,
                             BeVideoDataset.ANNOTATIONS_FIELDS):
        TYPES = {
            **ObservationMediaDataset.ANNOTATIONS_FIELDS.TYPES,
            **BeVideoDataset.ANNOTATIONS_FIELDS.TYPES
        }

    @classmethod
    def from_pipeline_results(cls,
                              project_title,
                              site,
                              pipeline_name,
                              pipeline_version,
                              **kwargs) -> ObservationsVideoDataset:
        return super().from_pipeline_results(project_title=project_title,
                                             site=site,
                                             pipeline_name=pipeline_name,
                                             pipeline_version=pipeline_version,
                                             filetype='video',
                                             **kwargs)

    @classmethod
    def from_not_processed_data(cls,
                                project_title: str,
                                site: str,
                                pipeline_name: str,
                                pipeline_version: str,
                                **kwargs) -> ObservationsVideoDataset:
        return super().from_not_processed_data(project_title=project_title,
                                               site=site,
                                               pipeline_name=pipeline_name,
                                               pipeline_version=pipeline_version,
                                               filetype='video',
                                               **kwargs)


class ObservationsVideoPredictionDataset(ObservationsVideoDataset, BeVideoPredictionDataset):
    class MEDIA_FIELDS(ObservationsVideoDataset.MEDIA_FIELDS,
                       BeVideoPredictionDataset.MEDIA_FIELDS):
        TYPES = {
            **ObservationsVideoDataset.MEDIA_FIELDS.TYPES,
            **BeVideoPredictionDataset.MEDIA_FIELDS.TYPES
        }

    class ANNOTATIONS_FIELDS(ObservationsVideoDataset.ANNOTATIONS_FIELDS,
                             BeVideoPredictionDataset.ANNOTATIONS_FIELDS):
        TYPES = {
            **ObservationsVideoDataset.ANNOTATIONS_FIELDS.TYPES,
            **BeVideoPredictionDataset.ANNOTATIONS_FIELDS.TYPES
        }
