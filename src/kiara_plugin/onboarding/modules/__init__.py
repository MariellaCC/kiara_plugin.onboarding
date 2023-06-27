# -*- coding: utf-8 -*-
from typing import List, Union

from pydantic import Field

from kiara.exceptions import KiaraException
from kiara.models.filesystem import FolderImportConfig
from kiara.models.module import KiaraModuleConfig
from kiara.models.values.value import ValueMap
from kiara.modules import KiaraModule, ValueMapSchema
from kiara.registries.models import ModelRegistry
from kiara_plugin.onboarding.models import OnboardDataModel
from kiara_plugin.onboarding.utils.download import (
    get_onboard_model_cls,
    onboard_file,
    onboard_file_bundle,
)


class OnboardFileConfig(KiaraModuleConfig):

    onboard_type: Union[None, str] = Field(
        description="The name of the type of onboarding.", default=None
    )
    attach_metadata: Union[bool, None] = Field(
        description="Whether to attach metadata.", default=None
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

        if self.get_config_value("attach_metadata") is None:
            result["attach_metadata"] = {
                "type": "boolean",
                "doc": "Whether to attach onboarding metadata to the result file.",
                "default": True,
            }

        onboard_type: Union[str, None] = self.get_config_value("onboard_type")
        if not onboard_type:
            onboard_model_cls = None
        else:
            onboard_model_cls = get_onboard_model_cls(onboard_type)

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

    def process(self, inputs: ValueMap, outputs: ValueMap):

        onboard_type = self.get_config_value("onboard_type")

        source: str = inputs.get_value_data("source")
        file_name: Union[str, None] = inputs.get_value_data("file_name")

        if not onboard_type:

            user_input_onboard_type = inputs.get_value_data("onboard_type")
            if user_input_onboard_type:
                onboard_type = (
                    f"{ONBOARDING_MODEL_NAME_PREFIX}{user_input_onboard_type}"
                )

        attach_metadata = self.get_config_value("attach_metadata")
        if attach_metadata is None:
            attach_metadata = inputs.get_value_data("attach_metadata")

        data = onboard_file(
            source=source,
            file_name=file_name,
            onboard_type=onboard_type,
            attach_metadata=attach_metadata,
        )

        outputs.set_value("file", data)


class OnboardFileBundleConfig(KiaraModuleConfig):

    onboard_type: Union[None, str] = Field(
        description="The name of the type of onboarding.", default=None
    )
    attach_metadata: Union[bool, None] = Field(
        description="Whether to attach onboarding metadata.", default=None
    )
    sub_path: Union[None, str] = Field(description="The sub path to use.", default=None)
    include_files: Union[None, List[str]] = Field(
        description="File types to include.", default=None
    )
    exclude_files: Union[None, List[str]] = Field(
        description="File types to include.", default=None
    )
    exclude_dirs: Union[None, List[str]] = Field(
        description="Exclude directories that end with one of those tokens.",
        default=None,
    )


class OnboardFileBundleModule(KiaraModule):
    """A generic module that imports a file from one of several possible sources."""

    _module_type_name = "import.file_bundle"
    _config_cls = OnboardFileBundleConfig

    def create_inputs_schema(
        self,
    ) -> ValueMapSchema:

        result = {
            "source": {
                "type": "string",
                "doc": "The source uri of the file to be onboarded.",
                "optional": False,
            }
        }

        if self.get_config_value("attach_metadata") is None:
            result["attach_metadata"] = {
                "type": "boolean",
                "doc": "Whether to attach onboarding metadata.",
                "default": True,
            }
        if self.get_config_value("sub_path") is None:
            result["sub_path"] = {
                "type": "string",
                "doc": "The sub path to use. If not specified, the root of the source folder will be used.",
                "optional": True,
            }
        if self.get_config_value("include_files") is None:
            result["include_files"] = {
                "type": "list",
                "doc": "Include files that end with one of those tokens. If not specified, all file extensions are included.",
                "optional": True,
            }

        if self.get_config_value("exclude_files") is None:
            result["exclude_files"] = {
                "type": "list",
                "doc": "Exclude files that end with one of those tokens. If not specified, no file extensions are excluded.",
                "optional": True,
            }
        if self.get_config_value("exclude_dirs") is None:
            result["exclude_dirs"] = {
                "type": "list",
                "doc": "Exclude directories that end with one of those tokens. If not specified, no directories are excluded.",
                "optional": True,
            }

        onboard_type: Union[str, None] = self.get_config_value("onboard_type")
        if not onboard_type:
            onboard_model_cls = None
        else:
            onboard_model_cls = get_onboard_model_cls(onboard_type)

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

        result = {
            "file_bundle": {
                "type": "file_bundle",
                "doc": "The file_bundle that was onboarded.",
            }
        }
        return result

    def process(self, inputs: ValueMap, outputs: ValueMap):

        onboard_type = self.get_config_value("onboard_type")
        source: str = inputs.get_value_data("source")

        if onboard_type:
            user_input_onboard_type = inputs.get_value_data("onboard_type")
            if not user_input_onboard_type:
                onboard_type = (
                    f"{ONBOARDING_MODEL_NAME_PREFIX}{user_input_onboard_type}"
                )

        sub_path = self.get_config_value("sub_path")
        if sub_path is None:
            sub_path = inputs.get_value_data("sub_path")

        include = self.get_config_value("include_files")
        if include is None:
            _include = inputs.get_value_data("include_files")
            if _include:
                include = _include.list_data
        exclude = self.get_config_value("exclude_files")
        if exclude is None:
            _exclude = inputs.get_value_data("exclude_files")
            if _exclude:
                exclude = _exclude.list_data
        exclude_dirs = self.get_config_value("exclude_dirs")
        if exclude_dirs is None:
            _exclude_dirs = inputs.get_value_data("exclude_dirs")
            if _exclude_dirs:
                exclude_dirs = _exclude_dirs.list_data

        import_config_data = {
            "sub_path": sub_path,
        }
        if include:
            import_config_data["include_files"] = include
        if exclude:
            import_config_data["exclude_files"] = exclude
        if exclude_dirs:
            import_config_data["exclude_dirs"] = exclude_dirs

        import_config = FolderImportConfig(**import_config_data)
        attach_metadata = self.get_config_value("attach_metadata")
        if attach_metadata is None:
            attach_metadata = inputs.get_value_data("attach_metadata")

        imported_bundle = onboard_file_bundle(
            source=source,
            import_config=import_config,
            onboard_type=onboard_type,
            attach_metadata=attach_metadata,
        )

        outputs.set_value("file_bundle", imported_bundle)
