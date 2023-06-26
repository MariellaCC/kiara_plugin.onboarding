# -*- coding: utf-8 -*-


# class ImportFileBundleConfig(KiaraModuleConfig):
#
#     import_metadata: bool = Field(
#         description="Whether to return the import metadata as well.",
#         default=True,
#     )
#
#
# class ImportFileBundleModule(KiaraModule):
#     """A generic module to import a file bundle from any local or remote location."""
#
#     _module_type_name = "import.file_bundle"
#     _config_cls = ImportFileBundleConfig
#
#     def create_inputs_schema(
#         self,
#     ) -> ValueMapSchema:
#
#         result: Dict[str, Dict[str, Any]] = {
#             "uri": {
#                 "type": "string",
#                 "doc": "The uri (url/path/...) of the file to import.",
#             }
#         }
#         if self.get_config_value("import_metadata"):
#             result["import_metadata"] = {
#                 "type": "dict",
#                 "doc": "Metadata you want to attach to the file bundle.",
#                 "optional": True,
#             }
#
#         return result
#
#     def create_outputs_schema(
#         self,
#     ) -> ValueMapSchema:
#
#         result = {
#             "file_bundle": {
#                 "type": "file_bundle",
#                 "doc": "The imported file bundle.",
#             }
#         }
#         if self.get_config_value("import_metadata"):
#             result["import_metadata"] = {
#                 "type": "dict",
#                 "doc": "Metadata about the import and file bundle.",
#             }
#         return result
#
#     def process(self, inputs: ValueMap, outputs: ValueMap) -> None:
#         pass
