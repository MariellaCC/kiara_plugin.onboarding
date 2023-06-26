# -*- coding: utf-8 -*-
# import atexit
# import os
# import shutil
# import tempfile
# from typing import Any, Dict, Union
#
# from pydantic import Field
#
# from kiara.api import KiaraModule, KiaraModuleConfig, ValueMap, ValueMapSchema
# from kiara.exceptions import KiaraProcessingException
# from kiara.models.filesystem import FileBundle
# from kiara_plugin.onboarding.utils.download import download_file
#
#
# class ImportFileConfig(KiaraModuleConfig):
#
#     import_metadata: bool = Field(
#         description="Whether to return the import metadata as well.",
#         default=True,
#     )
#     source_location: Union[str, None] = Field(
#         description="The location of the source file.", default=None
#     )
#
#
# class DownloadFileConfig(KiaraModuleConfig):
#     attach_metadata: bool = Field(
#         description="Whether to attach the download metadata to the result file.",
#         default=True,
#     )
#
#
# class DownloadFileModule(KiaraModule):
#     """Download a single file from a remote location.
#
#     The result of this operation is a single value of type 'file' (basically an array of raw bytes), which can then be used in other modules to
#     create more meaningful data structures.
#     """
#
#     _module_type_name = "download.file"
#     _config_cls = DownloadFileConfig
#
#     def create_inputs_schema(self) -> ValueMapSchema:
#
#         result: Dict[str, Dict[str, Any]] = {
#             "url": {"type": "string", "doc": "The url of the file to download."},
#             "file_name": {
#                 "type": "string",
#                 "doc": "The file name to use for the downloaded file.",
#                 "optional": True,
#             },
#         }
#         return result
#
#     def create_outputs_schema(
#         self,
#     ) -> ValueMapSchema:
#
#         result: Dict[str, Dict[str, Any]] = {
#             "file": {
#                 "type": "file",
#                 "doc": "The downloaded file.",
#             }
#         }
#
#         return result
#
#     def process(self, inputs: ValueMap, outputs: ValueMap):
#
#         url = inputs.get_value_data("url")
#         file_name = inputs.get_value_data("file_name")
#
#         result_file = download_file(
#             url=url,
#             file_name=file_name,
#             attach_metadata=self.get_config_value("attach_metadata"),
#         )
#
#         outputs.set_value("file", result_file)
#
#
# class DownloadFileBundleModule(KiaraModule):
#     _module_type_name = "download.file_bundle"
#     _config_cls = DownloadFileConfig
#
#     def create_inputs_schema(self) -> ValueMapSchema:
#
#         result: Dict[str, Dict[str, Any]] = {
#             "url": {
#                 "type": "string",
#                 "doc": "The url of an archive/zip file to download.",
#             },
#             "sub_path": {
#                 "type": "string",
#                 "doc": "A relative path to select only a sub-folder from the archive.",
#                 "optional": True,
#             },
#         }
#
#         return result
#
#     def create_outputs_schema(
#         self,
#     ) -> ValueMapSchema:
#
#         result: Dict[str, Dict[str, Any]] = {
#             "file_bundle": {
#                 "type": "file_bundle",
#                 "doc": "The downloaded file bundle.",
#             }
#         }
#
#         return result
#
#     def process(self, inputs: ValueMap, outputs: ValueMap):
#
#         from datetime import datetime
#         from urllib.parse import urlparse
#
#         import httpx
#         import pytz
#
#         url = inputs.get_value_data("url")
#         suffix = None
#         try:
#             parsed_url = urlparse(url)
#             _, suffix = os.path.splitext(parsed_url.path)
#         except Exception:
#             pass
#         if not suffix:
#             suffix = ""
#
#         sub_path: Union[None, str] = inputs.get_value_data("sub_path")
#         tmp_file = tempfile.NamedTemporaryFile(delete=False, suffix=suffix)
#         atexit.register(tmp_file.close)
#
#         history = []
#         datetime.utcnow().replace(tzinfo=pytz.utc)
#         with open(tmp_file.name, "wb") as f:
#             with httpx.stream("GET", url, follow_redirects=True) as r:
#                 history.append(dict(r.headers))
#                 for h in r.history:
#                     history.append(dict(h.headers))
#                 for data in r.iter_bytes():
#                     f.write(data)
#
#         out_dir = tempfile.mkdtemp()
#
#         def del_out_dir():
#             shutil.rmtree(out_dir, ignore_errors=True)
#
#         atexit.register(del_out_dir)
#
#         error = None
#         try:
#             shutil.unpack_archive(tmp_file.name, out_dir)
#         except Exception:
#             # try patool, maybe we're lucky
#             try:
#                 import patoolib
#
#                 patoolib.extract_archive(tmp_file.name, outdir=out_dir)
#             except Exception as e:
#                 error = e
#
#         if error is not None:
#             raise KiaraProcessingException(f"Could not extract archive: {error}.")
#
#         path = out_dir
#         if sub_path:
#             path = os.path.join(out_dir, sub_path)
#         bundle = FileBundle.import_folder(path)
#
#         metadata = {
#             "response_headers": history,
#             "request_time": datetime.utcnow().replace(tzinfo=pytz.utc).isoformat(),
#         }
#         outputs.set_value("download_metadata", metadata)
#         outputs.set_value("file_bundle", bundle)
