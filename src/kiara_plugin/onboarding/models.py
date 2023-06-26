# -*- coding: utf-8 -*-

"""This module contains the metadata (and other) models that are used in the ``kiara_plugin.onboarding`` package.

Those models are convenience wrappers that make it easier for *kiara* to find, create, manage and version metadata -- but also
other type of models -- that is attached to data, as well as *kiara* modules.

Metadata models must be a sub-class of [kiara.metadata.MetadataModel][kiara.metadata.MetadataModel]. Other models usually
sub-class a pydantic BaseModel or implement custom base classes.
"""
import os.path
from abc import abstractmethod
from typing import TYPE_CHECKING, List, Union

from kiara.exceptions import KiaraException
from kiara.models import KiaraModel
from kiara.models.filesystem import FileModel

if TYPE_CHECKING:
    pass


class OnboardDataModel(KiaraModel):

    _kiara_model_id: str = None  # type: ignore

    @classmethod
    def get_config_fields(cls) -> List[str]:
        return sorted(cls.__fields__.keys())

    @classmethod
    @abstractmethod
    def accepts_uri(cls, uri: str) -> bool:
        pass

    @abstractmethod
    def retrieve(self, uri: str, file_name: Union[None, str]) -> FileModel:

        pass


class FileFromLocalModel(OnboardDataModel):

    _kiara_model_id: str = "onboarding.file.from.local_file"

    @classmethod
    def accepts_uri(cls, uri: str) -> bool:

        return os.path.isfile(uri)

    def retrieve(self, uri: str, file_name: Union[None, str]) -> FileModel:

        if not os.path.exists(uri):
            raise KiaraException(
                f"Can't create file from path '{uri}': path does not exist."
            )
        if not os.path.isfile(uri):
            raise KiaraException(
                f"Can't create file from path '{uri}': path is not a file."
            )

        return FileModel.load_file(uri)


class FileFromRemoteModel(OnboardDataModel):

    _kiara_model_id: str = "onboarding.file.from.url"

    @classmethod
    def accepts_uri(cls, uri: str) -> bool:

        accepted_protocols = ["http", "https"]
        for protocol in accepted_protocols:
            if uri.startswith(f"{protocol}://"):
                return True

        return False

    def retrieve(self, uri: str, file_name: Union[None, str]) -> FileModel:
        from kiara_plugin.onboarding.utils.download import download_file

        result_file = download_file(url=uri, file_name=file_name, attach_metadata=True)
        return result_file
