# -*- coding: utf-8 -*-
from functools import lru_cache
from typing import List, Type, Union

from pydantic import Field

from kiara.exceptions import KiaraException, KiaraProcessingException
from kiara.models.filesystem import FileModel
from kiara.models.module import KiaraModuleConfig
from kiara.models.values.value import ValueMap
from kiara.modules import KiaraModule, ValueMapSchema
from kiara.registries.models import ModelRegistry
from kiara_plugin.onboarding.models import OnboardDataModel


class OnboardFileConfig(KiaraModuleConfig):

    onboard_type: Union[None, str] = Field(
        description="The name of the type of onboarding.", default=None
    )


ONBOARDING_MODEL_NAME_PREFIX = "onboarding.file.from."


class OnboardFileModule(KiaraModule):
    """A generic module that imports a file from one of several possible sources."""

    _module_type_name = "import.file"
    _config_cls = OnboardFileConfig

    def create_inputs_schema(
        self,
    ) -> ValueMapSchema:

        result = {
            "source": {
                "type": "string",
                "doc": "The source uri of the file to be onboarded.",
                "optional": False,
            },
            "file_name": {
                "type": "string",
                "doc": "The file name to use for the onboarded file (defaults to source file name if possible).",
                "optional": True,
            },
        }

        onboard_model_cls = self.get_onboard_model_cls()
        if not onboard_model_cls:

            available = (
                ModelRegistry.instance()
                .get_models_of_type(OnboardDataModel)
                .item_infos.keys()
            )

            if not available:
                raise KiaraException(msg="No onboard models available. This is a bug.")

            idx = len(ONBOARDING_MODEL_NAME_PREFIX)
            allowed = sorted((x[idx:] for x in available))

            result["onboard_type"] = {
                "type": "string",
                "type_config": {"allowed_strings": allowed},
                "doc": "The type of onboarding to use. Allowed: {}".format(
                    ", ".join(allowed)
                ),
                "optional": True,
            }
        elif onboard_model_cls.get_config_fields():
            result = {
                "onboard_config": {
                    "type": "kiara_model",
                    "type_config": {
                        "kiara_model_id": self.get_config_value("onboard_type"),
                    },
                }
            }

        return result

    def create_outputs_schema(
        self,
    ) -> ValueMapSchema:

        result = {"file": {"type": "file", "doc": "The file that was onboarded."}}
        return result

    @lru_cache(maxsize=1)
    def get_onboard_model_cls(self) -> Union[None, Type[OnboardDataModel]]:

        onboard_type: Union[str, None] = self.get_config_value("onboard_type")
        if not onboard_type:
            return None

        model_registry = ModelRegistry.instance()
        model_cls = model_registry.get_model_cls(onboard_type, OnboardDataModel)
        return model_cls  # type: ignore

    def find_matching_onboard_models(self, uri: str) -> List[Type[OnboardDataModel]]:

        model_registry = ModelRegistry.instance()
        onboard_models = model_registry.get_models_of_type(
            OnboardDataModel
        ).item_infos.values()

        result = []
        onboard_model: Type[OnboardDataModel]  # type: ignore
        for onboard_model in onboard_models:

            python_cls: Type[OnboardDataModel] = onboard_model.python_class.get_class()  # type: ignore
            if python_cls.accepts_uri(uri):
                result.append(python_cls)
        return result

    def process(self, inputs: ValueMap, outputs: ValueMap):

        onboard_type = self.get_config_value("onboard_type")

        source: str = inputs.get_value_data("source")
        file_name: Union[str, None] = inputs.get_value_data("file_name")

        if not onboard_type:

            user_input_onboard_type = inputs.get_value_data("onboard_type")
            if not user_input_onboard_type:
                model_clsses = self.find_matching_onboard_models(source)
                if not model_clsses:
                    raise KiaraProcessingException(
                        msg=f"Can't onboard file from '{source}': no onboard models found that accept this source type."
                    )
                if model_clsses:
                    if len(model_clsses) > 1:
                        raise KiaraProcessingException(
                            msg=f"Can't onboard file from '{source}': multiple onboard models found that accept this source type. Please specify the onboard type to use."
                        )

                model_cls: Type[OnboardDataModel] = model_clsses[0]
            else:
                full_onboard_type = (
                    f"{ONBOARDING_MODEL_NAME_PREFIX}{user_input_onboard_type}"
                )
                model_registry = ModelRegistry.instance()
                model_cls = model_registry.get_model_cls(full_onboard_type, OnboardDataModel)  # type: ignore
                if not model_cls.accepts_uri(source):
                    raise KiaraProcessingException(msg=f"Can't onboard file from '{source}' using onboard type '{model_cls._kiara_model_id}'.")  # type: ignore
        else:
            model_cls = self.get_onboard_model_cls()  # type: ignore
            if not model_cls:
                raise KiaraProcessingException(msg=f"Can't onboard file from '{source}' using onboard type '{onboard_type}': no onboard model found with this name.")  # type: ignore
            if not model_cls.accepts_uri(source):
                raise KiaraProcessingException(msg=f"Can't onboard file from '{source}' using onboard type '{model_cls._kiara_model_id}'.")  # type: ignore

        if not model_cls.get_config_fields():
            model = model_cls()
        else:
            raise NotImplementedError()

        result = model.retrieve(uri=source, file_name=file_name)
        if not result:
            raise KiaraProcessingException(msg=f"Can't onboard file from '{source}' using onboard type '{model_cls._kiara_model_id}': no result data retrieved. This is most likely a bug.")  # type: ignore

        if isinstance(result, tuple):
            if len(result) > 2:
                raise KiaraProcessingException(
                    "Can't onboard file: onboard model returned more than two values. This is most likely a bug."
                )

            data = result[0]
            result[1]
        else:
            data = result

        if isinstance(data, str):
            data = FileModel.load_file(data, file_name=file_name)
        elif not isinstance(data, FileModel):
            raise KiaraProcessingException(
                "Can't onboard file: onboard model returned data that is not a file. This is most likely a bug."
            )

        outputs.set_value("file", data)
