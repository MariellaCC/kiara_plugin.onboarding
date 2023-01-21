# -*- coding: utf-8 -*-
import atexit
import os
import shutil
import tempfile
from typing import Any, Dict, Union

from kiara import KiaraModule, KiaraModuleConfig, ValueMap, ValueMapSchema
from kiara.models.filesystem import FileBundle, FileModel
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


class ImportFileBundleConfig(KiaraModuleConfig):

    import_metadata: bool = Field(
        description="Whether to return the import metadata as well.",
        default=True,
    )


class ImportFileBundleModule(KiaraModule):
    """A generic module to import a file bundle from any local or remote location."""

    _module_type_name = "import.file_bundle"
    _config_cls = ImportFileBundleConfig

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
                "doc": "Metadata you want to attach to the file bundle.",
                "optional": True,
            }

        return result

    def create_outputs_schema(
        self,
    ) -> ValueMapSchema:

        result = {
            "file_bundle": {
                "type": "file_bundle",
                "doc": "The imported file bundle.",
            }
        }
        if self.get_config_value("import_metadata"):
            result["import_metadata"] = {
                "type": "dict",
                "doc": "Metadata about the import and file bundle.",
            }
        return result

    def process(self, inputs: ValueMap, outputs: ValueMap) -> None:
        pass


class DownloadFileConfig(KiaraModuleConfig):
    download_metadata: bool = Field(
        description="Whether to return the download metadata as well.",
        default=True,
    )


class DownloadFileModule(KiaraModule):
    """Download a single file from a remote location.

    The result of this operation is a single value of type 'file' (basically an array of raw bytes), which can then be used in other modules to
    create more meaningful data structures.
    """

    _module_type_name = "download.file"
    _config_cls = DownloadFileConfig

    def create_inputs_schema(self) -> ValueMapSchema:

        result: Dict[str, Dict[str, Any]] = {
            "url": {"type": "string", "doc": "The url of the file to download."},
            "file_name": {
                "type": "string",
                "doc": "The file name to use for the downloaded file.",
                "optional": True,
            },
        }
        return result

    def create_outputs_schema(
        self,
    ) -> ValueMapSchema:

        result: Dict[str, Dict[str, Any]] = {
            "file": {
                "type": "file",
                "doc": "The downloaded file.",
            }
        }

        if self.get_config_value("download_metadata"):
            result["download_metadata"] = {
                "type": "dict",
                "doc": "Metadata about the download.",
            }
        return result

    def process(self, inputs: ValueMap, outputs: ValueMap):

        from datetime import datetime

        import httpx
        import pytz

        url = inputs.get_value_data("url")
        file_name = inputs.get_value_data("file_name")
        tmp_file = tempfile.NamedTemporaryFile(delete=False)
        atexit.register(tmp_file.close)

        history = []
        datetime.utcnow().replace(tzinfo=pytz.utc)
        with open(tmp_file.name, "wb") as f:
            with httpx.stream("GET", url, follow_redirects=True) as r:
                history.append(dict(r.headers))
                for h in r.history:
                    history.append(dict(h.headers))
                for data in r.iter_bytes():
                    f.write(data)

        if not file_name:
            # TODO: make this smarter, using content-disposition headers if available
            file_name = url.split("/")[-1]

        result_file = FileModel.load_file(tmp_file.name, file_name)

        metadata = {
            "response_headers": history,
            "request_time": datetime.utcnow().replace(tzinfo=pytz.utc).isoformat(),
        }
        outputs.set_value("download_metadata", metadata)
        outputs.set_value("file", result_file)


class DownloadFileBundleModule(KiaraModule):
    _module_type_name = "download.file_bundle"
    _config_cls = DownloadFileConfig

    def create_inputs_schema(self) -> ValueMapSchema:

        result: Dict[str, Dict[str, Any]] = {
            "url": {
                "type": "string",
                "doc": "The url of an archive/zip file to download.",
            },
            "sub_path": {
                "type": "string",
                "doc": "A relative path to select only a sub-folder from the archive.",
                "optional": True,
            },
        }

        return result

    def create_outputs_schema(
        self,
    ) -> ValueMapSchema:

        result: Dict[str, Dict[str, Any]] = {
            "file_bundle": {
                "type": "file_bundle",
                "doc": "The downloaded file bundle.",
            }
        }

        if self.get_config_value("download_metadata"):
            result["download_metadata"] = {
                "type": "dict",
                "doc": "Metadata about the download.",
            }
        return result

    def process(self, inputs: ValueMap, outputs: ValueMap):

        from datetime import datetime

        import httpx
        import pytz

        url = inputs.get_value_data("url")
        sub_path: Union[None, str] = inputs.get_value_data("sub_path")
        tmp_file = tempfile.NamedTemporaryFile(delete=False)
        atexit.register(tmp_file.close)

        history = []
        datetime.utcnow().replace(tzinfo=pytz.utc)
        with open(tmp_file.name, "wb") as f:
            with httpx.stream("GET", url, follow_redirects=True) as r:
                history.append(dict(r.headers))
                for h in r.history:
                    history.append(dict(h.headers))
                for data in r.iter_bytes():
                    f.write(data)

        import patoolib

        out_dir = tempfile.mkdtemp()

        def del_out_dir():
            shutil.rmtree(out_dir, ignore_errors=True)

        # atexit.register(del_out_dir)

        patoolib.extract_archive(tmp_file.name, outdir=out_dir)

        path = out_dir
        if sub_path:
            path = os.path.join(out_dir, sub_path)
        bundle = FileBundle.import_folder(path)

        metadata = {
            "response_headers": history,
            "request_time": datetime.utcnow().replace(tzinfo=pytz.utc).isoformat(),
        }
        outputs.set_value("download_metadata", metadata)
        outputs.set_value("file_bundle", bundle)
