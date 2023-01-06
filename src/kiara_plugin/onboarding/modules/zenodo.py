# -*- coding: utf-8 -*-
import hashlib
import shutil
from pathlib import Path
from typing import Any, Mapping

import orjson
from kiara import KiaraModule, KiaraModuleConfig, ValueMap, ValueMapSchema
from kiara.exceptions import KiaraProcessingException
from kiara.models.filesystem import FileBundle
from pydantic import Field


class ZenodoDownloadConfig(KiaraModuleConfig):

    metadata_filename: str = Field(
        description="The filename for the zenodo metadata.", default="metadata.json"
    )


class ZenodoDownload(KiaraModule):
    """Download a dataset from zenodo.org."""

    _module_type_name = "onboard.zenodo_record"
    _config_cls = ZenodoDownloadConfig

    def create_inputs_schema(
        self,
    ) -> ValueMapSchema:

        metadata_filename = self.get_config_value("metadata_filename")
        return {
            "doi": {"type": "string", "doc": "The doi of the record"},
            "include_metadata": {
                "type": "boolean",
                "doc": f"Whether to write the record metadata to a file '{metadata_filename}' and include it in the resulting file bundle.",
                "default": True,
            },
        }

    def create_outputs_schema(
        self,
    ) -> ValueMapSchema:

        return {
            "file_bundle": {
                "type": "file_bundle",
            }
        }

    def download_file(self, file_data: Mapping[str, Any], target_path: Path):

        import httpx

        url = file_data["links"]["self"]
        file_name = file_data["key"]
        checksum = file_data["checksum"][4:]

        target_file = target_path / file_name

        if target_file.exists():
            raise KiaraProcessingException(
                f"Can't download file, target path already exists: {target_path.as_posix()}."
            )

        hash_md5 = hashlib.md5()

        with open(target_file, "ab") as file2:
            with httpx.Client() as client:
                with client.stream("GET", url) as resp:
                    for chunk in resp.iter_bytes():
                        hash_md5.update(chunk)
                        file2.write(chunk)

        if checksum != hash_md5.hexdigest():
            raise KiaraProcessingException(
                f"Can't downloda file '{file_name}', invalid checksum: {checksum} != {hash_md5.hexdigest()}"
            )

        return target_file

    def process(self, inputs: ValueMap, outputs: ValueMap):

        import pyzenodo3

        include_metadata = inputs.get_value_data("include_metadata")

        doi = inputs.get_value_data("doi")
        zen = pyzenodo3.Zenodo()

        record = zen.find_record_by_doi(doi)

        path = FileBundle.create_tmp_dir()
        shutil.rmtree(path, ignore_errors=True)
        path.mkdir()
        for file_data in record.data["files"]:
            self.download_file(file_data, path)

        if include_metadata:
            metadata_filename = self.get_config_value("metadata_filename")
            metadata_file = path / metadata_filename
            metadata_file.write_bytes(orjson.dumps(record.data))

        bundle = FileBundle.import_folder(path.as_posix())
        outputs.set_value("file_bundle", bundle)
