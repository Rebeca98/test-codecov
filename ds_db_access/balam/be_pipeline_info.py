from dataclasses import dataclass

from be_ml_vision.pipelines_info.pipeline_info import IPipelineInfo
from be_ml_vision.pipelines_info.pipeline_info import IFilteringImagesWithoutWildlifePipelineInfo
from be_ml_vision.pipelines_info.pipeline_info import IFilteringVideosWithoutWildlifePipelineInfo
from be_ml_vision.pipelines_info.pipeline_info import IMegadetectorPipelineInfo
from be_ml_vision.pipelines_info.pipeline_info import ITaxaClassificationAnimalImagesEnsemble
from be_ml_vision.pipelines_info.pipeline_info import ITaxaClassificationAnimalVideosEnsemble

from ds_db_access.balam.utils_models import get_pipeline_exec_params, insert_pipeline_info


class PipelineInfo(IPipelineInfo):

    def store_pipeline_info(self,
                            execution_params: IPipelineInfo.ExecutionParams,
                            url_repo_model: str = '',
                            comments: str = '',
                            update: bool = True):
        # Check if the pipelineInfo already exists, and control the behaviour using the parameter `update`
        insert_pipeline_info(pipeline_name=self.name,
                             pipeline_version=self.version,
                             url_repo_model=url_repo_model,
                             execution_params=execution_params.get_items(),
                             comments=comments)

    @classmethod
    def get_pipeline_info(cls, name, version) -> IPipelineInfo:
        instance = cls(name, version)
        exec_params_res = get_pipeline_exec_params(pipeline_name=name, pipeline_version=version)
        instance.url_repo_model = "https://github.com/biometrioearth/..."  # Obtain this info
        instance.comments = "Comments"  # Obtain this info
        instance.execution_params_dict = exec_params_res[0]

        return instance


class FilteringImagesWithoutWildlifePipelineInfo(PipelineInfo):

    @dataclass
    class ExecutionParams(IFilteringImagesWithoutWildlifePipelineInfo.ExecutionParams):
        pass

    @classmethod
    def get_pipeline_info(cls, name, version) -> IPipelineInfo:
        instance = super().get_pipeline_info(name, version)

        exec_params_dict = instance.execution_params_dict
        instance.execution_params = cls.ExecutionParams(
            threshold=exec_params_dict['threshold'],
            pred_method=exec_params_dict['pred_method'],
            detector_version=exec_params_dict['detector_version'],
            min_score_of_stored_dets=exec_params_dict['min_score_of_stored_dets'])

        return instance


class FilteringVideosWithoutWildlifePipelineInfo(PipelineInfo):

    @dataclass
    class ExecutionParams(IFilteringVideosWithoutWildlifePipelineInfo.ExecutionParams):
        pass

    @classmethod
    def get_pipeline_info(cls, name, version) -> IPipelineInfo:
        instance = super().get_pipeline_info(name, version)

        exec_params_dict = instance.execution_params_dict
        instance.execution_params = cls.ExecutionParams(
            threshold=exec_params_dict['threshold'],
            pred_method=exec_params_dict['pred_method'],
            detector_version=exec_params_dict['detector_version'],
            min_score_of_stored_dets=exec_params_dict['min_score_of_stored_dets'],
            freq_video_sampling=exec_params_dict['freq_video_sampling'])

        return instance


class MegadetectorPipelineInfo(PipelineInfo):

    @dataclass
    class ExecutionParams(IMegadetectorPipelineInfo.ExecutionParams):
        pass

    @classmethod
    def get_pipeline_info(cls, name, version) -> IPipelineInfo:
        instance = super().get_pipeline_info(name, version)

        exec_params_dict = instance.execution_params_dict
        instance.execution_params = cls.ExecutionParams(
            min_score_of_stored_dets=exec_params_dict['min_score_threshold'])

        return instance


class TaxaClassificationAnimalImagesEnsemble(PipelineInfo):

    @dataclass
    class ExecutionParams(ITaxaClassificationAnimalImagesEnsemble.ExecutionParams):
        pass

    @classmethod
    def get_pipeline_info(cls, name, version) -> IPipelineInfo:
        instance = super().get_pipeline_info(name, version)

        exec_params_dict = instance.execution_params_dict
        instance.execution_params = cls.ExecutionParams(
            pred_method=exec_params_dict['pred_method'],
            dets_threshold=exec_params_dict['dets_threshold'],
            models_names=exec_params_dict['models_names'],
            models_weights=exec_params_dict['models_weights'],
            detector_version=exec_params_dict['detector_version'],
            min_score_of_stored_dets=exec_params_dict['min_score_of_stored_dets'],
            obs_level=exec_params_dict['obs_level'])

        return instance


class TaxaClassificationAnimalVideosEnsemble(PipelineInfo):

    @dataclass
    class ExecutionParams(ITaxaClassificationAnimalVideosEnsemble.ExecutionParams):
        pass

    @classmethod
    def get_pipeline_info(cls, name, version) -> IPipelineInfo:
        instance = super().get_pipeline_info(name, version)

        exec_params_dict = instance.execution_params_dict
        instance.execution_params = cls.ExecutionParams(
            pred_method=exec_params_dict['pred_method'],
            dets_threshold=exec_params_dict['dets_threshold'],
            models_names=exec_params_dict['models_names'],
            models_weights=exec_params_dict['models_weights'],
            detector_version=exec_params_dict['detector_version'],
            min_score_of_stored_dets=exec_params_dict['min_score_of_stored_dets'],
            obs_level=exec_params_dict['obs_level'],
            freq_video_sampling=exec_params_dict['freq_video_sampling'])

        return instance

