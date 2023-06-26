# -*- coding: utf-8 -*-

"""This module contains the metadata (and other) models that are used in the ``kiara_plugin.onboarding`` package.

Those models are convenience wrappers that make it easier for *kiara* to find, create, manage and version metadata -- but also
other type of models -- that is attached to data, as well as *kiara* modules.

Metadata models must be a sub-class of [kiara.metadata.MetadataModel][kiara.metadata.MetadataModel]. Other models usually
sub-class a pydantic BaseModel or implement custom base classes.
"""
import os.path
from abc import abstractmethod
from typing import List, Union

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
    def accepts_uri(cls, uri: str) -> bool:
        pass

    @classmethod
    def accepts_bundle_uri(cls, uri: str) -> bool:
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
    def accepts_uri(cls, uri: str) -> bool:

        return os.path.isfile(os.path.abspath(uri))

    @classmethod
    def accepts_bundle_uri(cls, uri: str) -> bool:

        return os.path.isdir(os.path.abspath(uri))

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
    def accepts_uri(cls, uri: str) -> bool:

        accepted_protocols = ["http", "https"]
        for protocol in accepted_protocols:
            if uri.startswith(f"{protocol}://"):
                return True

        return False

    def retrieve(
        self, uri: str, file_name: Union[None, str], attach_metadata: bool
    ) -> FileModel:
        from kiara_plugin.onboarding.utils.download import download_file

        result_file = download_file(
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
