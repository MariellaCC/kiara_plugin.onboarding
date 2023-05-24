# -*- coding: utf-8 -*-
import os
from typing import Any, Dict, Tuple

from pydantic import BaseModel, Field

from kiara.api import KiaraModule, KiaraModuleConfig, ValueMap, ValueMapSchema
from kiara.exceptions import KiaraProcessingException
from kiara.models.filesystem import FileModel


class ImportFileConfig(KiaraModuleConfig):

    import_metadata: bool = Field(
        description="Whether to return the import metadata as well.",
        default=True,
    )


class ImportFileModule(KiaraModule):
    """A generic module to import a file from any local or remote location."""

    _module_type_name = "import.file"
    _config_cls = ImportFileConfig

    def create_inputs_schema(
        self,
    ) -> ValueMapSchema:

        result: Dict[str, Dict[str, Any]] = {
            "location": {
                "type": "string",
                "doc": "The uri (url/path/...) of the file to import.",
            }
        }
        # if not self.get_config_value("import_metadata"):
        #     result["import_metadata"] = {
        #         "type": "dict",
        #         "doc": "Metadata you want to attach to the file.",
        #         "optional": True,
        #     }

        return result

    def create_outputs_schema(
        self,
    ) -> ValueMapSchema:

        result = {
            "file": {
                "type": "file",
                "doc": "The imported file.",
            }
        }
        if self.get_config_value("import_metadata"):
            result["import_metadata"] = {
                "type": "dict",
                "doc": "Metadata about the import and file.",
            }
        return result

    def process(self, inputs: ValueMap, outputs: ValueMap) -> None:

        location = inputs.get_value_data("location")
        import_metadata = self.get_config_value("import_metadata")

        if not location:
            raise KiaraProcessingException("Invalid location input: can't be empty.")

        if os.path.exists(location):
            if os.path.isdir(os.path.realpath(location)):
                raise KiaraProcessingException(
                    f"Invalid location input: {location} is a directory."
                )
            else:
                import_type: str = "local_file"
        elif location.startswith("http"):
            import_type = "http"
        else:
            raise KiaraProcessingException(
                f"Can't determine input type for file location: {location}."
            )

        func_name = f"import_{import_type}"
        func = getattr(self, func_name)

        result_file, metadata = func(location)

        outputs.set_value("file", result_file)
        if import_metadata:
            outputs.set_value("import_metadata", metadata)

    def import_local_file(self, location: str) -> Tuple[FileModel, BaseModel]:
        """Import a file from a local location."""
        raise NotImplementedError()

    def import_http(self, location: str) -> Tuple[FileModel, BaseModel]:
        """Download a file from a http location."""
        raise NotImplementedError()
