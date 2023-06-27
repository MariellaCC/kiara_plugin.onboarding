# -*- coding: utf-8 -*-

"""This module contains the metadata (and other) models that are used in the ``kiara_plugin.onboarding`` package.

Those models are convenience wrappers that make it easier for *kiara* to find, create, manage and version metadata -- but also
other type of models -- that is attached to data, as well as *kiara* modules.

Metadata models must be a sub-class of [kiara.metadata.MetadataModel][kiara.metadata.MetadataModel]. Other models usually
sub-class a pydantic BaseModel or implement custom base classes.
"""
import os.path
from abc import abstractmethod
from typing import List, Tuple, Union

from kiara.exceptions import KiaraException
from kiara.models import KiaraModel
from kiara.models.filesystem import FileBundle, FileModel, FolderImportConfig


class OnboardDataModel(KiaraModel):

    _kiara_model_id: str = None  # type: ignore

    @classmethod
    def get_config_fields(cls) -> List[str]:
        return sorted(cls.__fields__.keys())

    @classmethod
    @abstractmethod
    def accepts_uri(cls, uri: str) -> Tuple[bool, str]:
        pass

    @classmethod
    def accepts_bundle_uri(cls, uri: str) -> Tuple[bool, str]:
        return cls.accepts_uri(uri)

    @abstractmethod
    def retrieve(
        self, uri: str, file_name: Union[None, str], attach_metadata: bool
    ) -> FileModel:
        pass

    def retrieve_bundle(
        self, uri: str, import_config: FolderImportConfig, attach_metadata: bool
    ) -> FileBundle:
        raise NotImplementedError()


class FileFromLocalModel(OnboardDataModel):

    _kiara_model_id: str = "onboarding.file.from.local_file"

    @classmethod
    def accepts_uri(cls, uri: str) -> Tuple[bool, str]:

        if os.path.isfile(os.path.abspath(uri)):
            return True, "local file exists and is file"
        else:
            return False, "local file does not exist or is not a file"

    @classmethod
    def accepts_bundle_uri(cls, uri: str) -> Tuple[bool, str]:

        if os.path.isdir(os.path.abspath(uri)):
            return True, "local folder exists and is folder"
        else:
            return False, "local folder does not exist or is not a folder"

    def retrieve(
        self, uri: str, file_name: Union[None, str], attach_metadata: bool
    ) -> FileModel:

        if not os.path.exists(os.path.abspath(uri)):
            raise KiaraException(
                f"Can't create file from path '{uri}': path does not exist."
            )
        if not os.path.isfile(os.path.abspath(uri)):
            raise KiaraException(
                f"Can't create file from path '{uri}': path is not a file."
            )

        return FileModel.load_file(uri)

    def retrieve_bundle(
        self, uri: str, import_config: FolderImportConfig, attach_metadata: bool
    ) -> FileBundle:

        if not os.path.exists(os.path.abspath(uri)):
            raise KiaraException(
                f"Can't create file from path '{uri}': path does not exist."
            )
        if not os.path.isdir(os.path.abspath(uri)):
            raise KiaraException(
                f"Can't create file from path '{uri}': path is not a directory."
            )

        return FileBundle.import_folder(source=uri, import_config=import_config)


class FileFromRemoteModel(OnboardDataModel):

    _kiara_model_id: str = "onboarding.file.from.url"

    @classmethod
    def accepts_uri(cls, uri: str) -> Tuple[bool, str]:

        accepted_protocols = ["http", "https"]
        for protocol in accepted_protocols:
            if uri.startswith(f"{protocol}://"):
                return True, "url is valid (starts with http or https)"

        return False, "url is not valid (does not start with http or https)"

    def retrieve(
        self, uri: str, file_name: Union[None, str], attach_metadata: bool
    ) -> FileModel:
        from kiara_plugin.onboarding.utils.download import download_file

        result_file: FileModel = download_file(  # type: ignore
            url=uri, file_name=file_name, attach_metadata=attach_metadata
        )
        return result_file

    def retrieve_bundle(
        self, uri: str, import_config: FolderImportConfig, attach_metadata: bool
    ) -> FileBundle:
        from kiara_plugin.onboarding.utils.download import download_file_bundle

        result_bundle = download_file_bundle(
            url=uri, import_config=import_config, attach_metadata=attach_metadata
        )
        return result_bundle


class FileFromZenodoModel(OnboardDataModel):

    _kiara_model_id: str = "onboarding.file.from.zenodo"

    @classmethod
    def accepts_uri(cls, uri: str) -> Tuple[bool, str]:

        if uri.startswith("zenodo:"):
            return True, "url is valid (follows format 'zenodo:<doi>')"

        elif "/zenodo." in uri:
            return True, "url is valid (contains '/zenodo.')"

        return False, "url is not valid (does not follow format 'zenodo:<doi>')"

    def retrieve(
        self, uri: str, file_name: Union[None, str], attach_metadata: bool
    ) -> FileModel:

        import pyzenodo3

        from kiara_plugin.onboarding.utils.download import download_file

        if uri.startswith("zenodo:"):
            doi = uri[len("zenodo:") :]
        elif "/zenodo." in uri:
            doi = uri

        tokens = doi.split("/zenodo.")
        if len(tokens) != 2:
            raise KiaraException(
                msg=f"Can't parse Zenodo DOI from URI for single file download: {doi}"
            )

        path_components = tokens[1].split("/", maxsplit=1)
        if len(path_components) != 2:
            raise KiaraException(
                msg=f"Can't parse Zenodo DOI from URI for single file download: {doi}"
            )

        file_path = path_components[1]
        _doi = f"{tokens[0]}/zenodo.{path_components[0]}"

        zen = pyzenodo3.Zenodo()
        record = zen.find_record_by_doi(_doi)

        match = None
        for _available_file in record.data["files"]:
            if file_path == _available_file["key"]:
                match = _available_file
                break

        if not match:
            msg = "Available files:\n"
            for key in record.data["files"]:
                msg += f"  - {key['key']}\n"
            raise KiaraException(
                msg=f"Can't find file '{file_path}' in Zenodo record. {msg}"
            )

        url = match["links"]["self"]
        checksum = match["checksum"][4:]

        file_name = file_path.split("/")[-1]

        file_model: FileModel
        file_model, md5_digest = download_file(  # type: ignore
            url=url,
            target=None,
            file_name=file_name,
            attach_metadata=attach_metadata,
            return_md5_hash=True,
        )

        if checksum != md5_digest:
            raise KiaraException(
                msg=f"Can't download file '{file_name}', invalid checksum: {checksum} != {md5_digest}"
            )

        if attach_metadata:
            file_model.metadata["zenodo_record_data"] = record.data

        return file_model

    def retrieve_bundle(
        self, uri: str, import_config: FolderImportConfig, attach_metadata: bool
    ) -> FileBundle:

        import shutil

        import pyzenodo3

        from kiara_plugin.onboarding.utils.download import download_file

        if uri.startswith("zenodo:"):
            doi = uri[len("zenodo:") :]
        elif "/zenodo." in uri:
            doi = uri

        tokens = doi.split("/zenodo.")
        if len(tokens) != 2:
            raise KiaraException(
                msg=f"Can't parse Zenodo DOI from URI for single file download: {doi}"
            )

        path_components = tokens[1].split("/", maxsplit=1)

        if len(path_components) == 2:
            zid = path_components[0]
            file_path = path_components[1]
        else:
            zid = path_components[0]
            file_path = None

        _doi = f"{tokens[0]}/zenodo.{zid}"

        if not file_path:

            zen = pyzenodo3.Zenodo()

            record = zen.find_record_by_doi(_doi)

            path = FileBundle.create_tmp_dir()
            shutil.rmtree(path, ignore_errors=True)
            path.mkdir()

            for file_data in record.data["files"]:
                url = file_data["links"]["self"]
                file_name = file_data["key"]
                checksum = file_data["checksum"][4:]

                target = os.path.join(path, file_name)
                file_model: FileModel
                file_model, md5_digest = download_file(  # type: ignore
                    url=url,
                    target=target,
                    file_name=file_name,
                    attach_metadata=attach_metadata,
                    return_md5_hash=True,
                )

                if checksum != md5_digest:
                    raise KiaraException(
                        msg=f"Can't download file '{file_name}', invalid checksum: {checksum} != {md5_digest}"
                    )

            bundle = FileBundle.import_folder(path.as_posix())
            if attach_metadata:
                bundle.metadata["zenodo_record_data"] = record.data

        else:

            zen = pyzenodo3.Zenodo()
            record = zen.find_record_by_doi(_doi)

            match = None
            for _available_file in record.data["files"]:
                if file_path == _available_file["key"]:
                    match = _available_file
                    break

            if not match:
                msg = "Available files:\n"
                for key in record.data["files"]:
                    msg += f"  - {key['key']}\n"
                raise KiaraException(
                    msg=f"Can't find file '{file_path}' in Zenodo record. {msg}"
                )

            url = match["links"]["self"]
            checksum = match["checksum"][4:]

            file_name = file_path.split("/")[-1]

            file_model, md5_digest = download_file(  # type: ignore
                url=url,
                target=None,
                file_name=file_name,
                attach_metadata=attach_metadata,
                return_md5_hash=True,
            )

            if checksum != md5_digest:
                raise KiaraException(
                    msg=f"Can't download file '{file_name}', invalid checksum: {checksum} != {md5_digest}"
                )

            bundle = FileBundle.from_archive_file(
                archive_file=file_model, import_config=import_config
            )
            if attach_metadata:
                bundle.metadata["zenodo_record_data"] = record.data

        return bundle
