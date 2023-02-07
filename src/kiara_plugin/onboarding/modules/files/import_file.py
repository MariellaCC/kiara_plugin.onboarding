# -*- coding: utf-8 -*-
from typing import Any, Dict

from kiara import KiaraModule, KiaraModuleConfig, ValueMap, ValueMapSchema
from pydantic import Field


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
            "uri": {
                "type": "string",
                "doc": "The uri (url/path/...) of the file to import.",
            }
        }
        if self.get_config_value("import_metadata"):
            result["import_metadata"] = {
                "type": "dict",
                "doc": "Metadata you want to attach to the file.",
                "optional": True,
            }

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
        pass
