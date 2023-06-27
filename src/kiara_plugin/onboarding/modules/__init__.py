# -*- coding: utf-8 -*-
from functools import lru_cache
from typing import List, Mapping, Tuple, Type, Union

from pydantic import Field

from kiara.exceptions import KiaraException, KiaraProcessingException
from kiara.models.filesystem import FolderImportConfig, KiaraFile, KiaraFileBundle
from kiara.models.module import KiaraModuleConfig
from kiara.models.values.value import ValueMap
from kiara.modules import KiaraModule, ValueMapSchema
from kiara.registries.models import ModelRegistry
from kiara_plugin.onboarding.models import OnboardDataModel


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

    def find_matching_onboard_models(
        self, uri: str
    ) -> Mapping[Type[OnboardDataModel], Tuple[bool, str]]:

        model_registry = ModelRegistry.instance()
        onboard_models = model_registry.get_models_of_type(
            OnboardDataModel
        ).item_infos.values()

        result = {}
        onboard_model: Type[OnboardDataModel]
        for onboard_model in onboard_models:  # type: ignore

            python_cls: Type[OnboardDataModel] = onboard_model.python_class.get_class()  # type: ignore
            result[python_cls] = python_cls.accepts_uri(uri)

        return result

    def process(self, inputs: ValueMap, outputs: ValueMap):

        onboard_type = self.get_config_value("onboard_type")

        source: str = inputs.get_value_data("source")
        file_name: Union[str, None] = inputs.get_value_data("file_name")

        if not onboard_type:

            user_input_onboard_type = inputs.get_value_data("onboard_type")

            if not user_input_onboard_type:
                model_clsses = self.find_matching_onboard_models(source)
                matches = [k for k, v in model_clsses.items() if v[0]]
                if not matches:
                    raise KiaraProcessingException(
                        msg=f"Can't onboard file from '{source}': no onboard models found that accept this source type."
                    )
                elif len(matches) > 1:
                    msg = "Valid onboarding types for this uri:\n\n"
                    for k, v in model_clsses.items():
                        if not v[0]:
                            continue
                        msg += f"  - {k._kiara_model_id}: {v[1]}\n"
                    raise KiaraProcessingException(
                        msg=f"Can't onboard file from '{source}': multiple onboard models found that accept this source type.\n\n{msg}"
                    )

                model_cls: Type[OnboardDataModel] = matches[0]
            else:
                full_onboard_type = (
                    f"{ONBOARDING_MODEL_NAME_PREFIX}{user_input_onboard_type}"
                )
                model_registry = ModelRegistry.instance()
                model_cls = model_registry.get_model_cls(full_onboard_type, OnboardDataModel)  # type: ignore
                valid, msg = model_cls.accepts_uri(source)
                if not valid:
                    raise KiaraProcessingException(msg=f"Can't onboard file from '{source}' using onboard type '{model_cls._kiara_model_id}': {msg}")  # type: ignore
        else:
            model_cls = self.get_onboard_model_cls()  # type: ignore
            if not model_cls:
                raise KiaraProcessingException(msg=f"Can't onboard file from '{source}' using onboard type '{onboard_type}': no onboard model found with this name.")  # type: ignore

            valid, msg = model_cls.accepts_uri(source)
            if not valid:
                raise KiaraProcessingException(msg=f"Can't onboard file from '{source}' using onboard type '{model_cls._kiara_model_id}': {msg}")  # type: ignore

        if not model_cls.get_config_fields():
            model = model_cls()
        else:
            raise NotImplementedError()

        attach_metadata = self.get_config_value("attach_metadata")
        if attach_metadata is None:
            attach_metadata = inputs.get_value_data("attach_metadata")

        result = model.retrieve(
            uri=source, file_name=file_name, attach_metadata=attach_metadata
        )
        if not result:
            raise KiaraProcessingException(msg=f"Can't onboard file from '{source}' using onboard type '{model_cls._kiara_model_id}': no result data retrieved. This is most likely a bug.")  # type: ignore

        if isinstance(result, str):
            data = KiaraFile.load_file(result, file_name=file_name)
        elif not isinstance(result, KiaraFile):
            raise KiaraProcessingException(
                "Can't onboard file: onboard model returned data that is not a file. This is most likely a bug."
            )
        else:
            data = result

        outputs.set_value("file", data)


class OnboardFileBundleConfig(KiaraModuleConfig):

    onboard_type: Union[None, str] = Field(
        description="The name of the type of onboarding.", default=None
    )
    attach_metadata: Union[bool, None] = Field(
        description="Whether to attach onboarding metadata.", default=None
    )
    sub_path: Union[None, str] = Field(description="The sub path to use.", default=None)
    include_file_types: Union[None, List[str]] = Field(
        description="File types to include.", default=None
    )
    exclude_file_types: Union[None, List[str]] = Field(
        description="File types to include.", default=None
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
        if self.get_config_value("include_file_types") is None:
            result["include_file_types"] = {
                "type": "list",
                "doc": "A list of file extensions to include. If not specified, all file extensions are included.",
                "optional": True,
            }

        if self.get_config_value("exclude_file_types") is None:
            result["exclude_file_types"] = {
                "type": "list",
                "doc": "A list of file extensions to exclude. If not specified, no file extensions are excluded.",
                "optional": True,
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

        result = {
            "file_bundle": {
                "type": "file_bundle",
                "doc": "The file_bundle that was onboarded.",
            }
        }
        return result

    @lru_cache(maxsize=1)
    def get_onboard_model_cls(self) -> Union[None, Type[OnboardDataModel]]:

        onboard_type: Union[str, None] = self.get_config_value("onboard_type")
        if not onboard_type:
            return None

        model_registry = ModelRegistry.instance()
        model_cls = model_registry.get_model_cls(onboard_type, OnboardDataModel)
        return model_cls  # type: ignore

    def find_matching_onboard_models(
        self, uri: str
    ) -> Mapping[Type[OnboardDataModel], Tuple[bool, str]]:

        model_registry = ModelRegistry.instance()
        onboard_models = model_registry.get_models_of_type(
            OnboardDataModel
        ).item_infos.values()

        result = {}
        onboard_model: Type[OnboardDataModel]
        for onboard_model in onboard_models:  # type: ignore

            python_cls: Type[OnboardDataModel] = onboard_model.python_class.get_class()  # type: ignore
            result[python_cls] = python_cls.accepts_bundle_uri(uri)

        return result

    def process(self, inputs: ValueMap, outputs: ValueMap):

        onboard_type = self.get_config_value("onboard_type")

        source: str = inputs.get_value_data("source")

        if not onboard_type:

            user_input_onboard_type = inputs.get_value_data("onboard_type")
            if not user_input_onboard_type:
                model_clsses = self.find_matching_onboard_models(source)
                matches = [k for k, v in model_clsses.items() if v[0]]
                if not matches:
                    raise KiaraProcessingException(
                        msg=f"Can't onboard file from '{source}': no onboard models found that accept this source type."
                    )
                elif len(matches) > 1:
                    msg = "Valid onboarding types for this uri:\n\n"
                    for k, v in model_clsses.items():
                        if not v[0]:
                            continue
                        msg += f"  - {k._kiara_model_id}: {v[1]}\n"
                    raise KiaraProcessingException(
                        msg=f"Can't onboard file from '{source}': multiple onboard models found that accept this source type.\n\n{msg}"
                    )

                model_cls: Type[OnboardDataModel] = matches[0]
            else:
                full_onboard_type = (
                    f"{ONBOARDING_MODEL_NAME_PREFIX}{user_input_onboard_type}"
                )
                model_registry = ModelRegistry.instance()
                model_cls = model_registry.get_model_cls(full_onboard_type, OnboardDataModel)  # type: ignore
                valid, msg = model_cls.accepts_bundle_uri(source)
                if not valid:
                    raise KiaraProcessingException(msg=f"Can't onboard file from '{source}' using onboard type '{model_cls._kiara_model_id}': {msg}")  # type: ignore
        else:
            model_cls = self.get_onboard_model_cls()  # type: ignore
            if not model_cls:
                raise KiaraProcessingException(msg=f"Can't onboard file from '{source}' using onboard type '{onboard_type}': no onboard model found with this name.")  # type: ignore
            valid, msg = model_cls.accepts_bundle_uri(source)
            if not valid:
                raise KiaraProcessingException(msg=f"Can't onboard file from '{source}' using onboard type '{model_cls._kiara_model_id}': {msg}")  # type: ignore

        if not model_cls.get_config_fields():
            model = model_cls()
        else:
            raise NotImplementedError()

        sub_path = self.get_config_value("sub_path")
        if sub_path is None:
            sub_path = inputs.get_value_data("sub_path")

        include = self.get_config_value("include_file_types")
        if include is None:
            include = inputs.get_value_data("include_file_types")
        exclude = self.get_config_value("exclude_file_types")
        if exclude is None:
            exclude = inputs.get_value_data("exclude_file_types")

        import_config = FolderImportConfig(
            sub_path=sub_path, include_files=include, exclude_files=exclude
        )
        attach_metadata = self.get_config_value("attach_metadata")
        if attach_metadata is None:
            attach_metadata = inputs.get_value_data("attach_metadata")

        try:
            result: Union[None, KiaraFileBundle] = model.retrieve_bundle(
                uri=source, import_config=import_config, attach_metadata=attach_metadata
            )

            if not result:
                raise KiaraProcessingException(msg=f"Can't onboard file bundle from '{source}' using onboard type '{model_cls._kiara_model_id}': no result data retrieved. This is most likely a bug.")  # type: ignore

            if isinstance(result, str):
                result = KiaraFileBundle.import_folder(source=result)

        except NotImplementedError:
            result = None

        if not result:
            result_file = model.retrieve(
                uri=source, file_name=None, attach_metadata=attach_metadata
            )
            if not result_file:
                raise KiaraProcessingException(msg=f"Can't onboard file bundle from '{source}' using onboard type '{model_cls._kiara_model_id}': no result data retrieved. This is most likely a bug.")  # type: ignore

            if isinstance(result, str):
                imported_bundle_file = KiaraFile.load_file(result_file)  # type: ignore
            elif not isinstance(result_file, KiaraFile):
                raise KiaraProcessingException(
                    "Can't onboard file: onboard model returned data that is not a file. This is most likely a bug."
                )
            else:
                imported_bundle_file = result_file

            imported_bundle = KiaraFileBundle.from_archive_file(
                imported_bundle_file, import_config=import_config
            )
        else:
            imported_bundle = result

        outputs.set_value("file_bundle", imported_bundle)
