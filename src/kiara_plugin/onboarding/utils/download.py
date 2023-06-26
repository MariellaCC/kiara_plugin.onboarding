# -*- coding: utf-8 -*-
import atexit
import os
import tempfile
from datetime import datetime
from typing import Dict, List, Union

from pydantic import BaseModel, Field

from kiara.exceptions import KiaraException
from kiara.models.filesystem import FileBundle, FileModel, FolderImportConfig


class DownloadMetadata(BaseModel):
    url: str = Field(description="The url of the download request.")
    response_headers: List[Dict[str, str]] = Field(
        description="The response headers of the download request."
    )
    request_time: str = Field(description="The time the request was made.")


class DownloadBundleMetadata(DownloadMetadata):
    import_config: FolderImportConfig = Field(
        description="The import configuration that was used to import the files from the source bundle."
    )


def download_file(
    url: str, file_name: Union[str, None] = None, attach_metadata: bool = True
) -> FileModel:

    import httpx
    import pytz

    tmp_file = tempfile.NamedTemporaryFile(delete=False)

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

    if attach_metadata:
        metadata = {
            "url": url,
            "response_headers": history,
            "request_time": datetime.utcnow().replace(tzinfo=pytz.utc).isoformat(),
        }
        _metadata = DownloadMetadata(**metadata)
        result_file.metadata = _metadata.dict()
        result_file.metadata_schema = DownloadMetadata.schema_json()

    return result_file


def download_file_bundle(
    url: str, attach_metadata: bool = True, import_config: FolderImportConfig = None
) -> FileBundle:

    import shutil
    from datetime import datetime
    from urllib.parse import urlparse

    import httpx
    import pytz

    suffix = None
    try:
        parsed_url = urlparse(url)
        _, suffix = os.path.splitext(parsed_url.path)
    except Exception:
        pass
    if not suffix:
        suffix = ""

    tmp_file = tempfile.NamedTemporaryFile(delete=False, suffix=suffix)
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

    out_dir = tempfile.mkdtemp()

    def del_out_dir():
        shutil.rmtree(out_dir, ignore_errors=True)

    atexit.register(del_out_dir)

    error = None
    try:
        shutil.unpack_archive(tmp_file.name, out_dir)
    except Exception:
        # try patool, maybe we're lucky
        try:
            import patoolib

            patoolib.extract_archive(tmp_file.name, outdir=out_dir)
        except Exception as e:
            error = e

    if error is not None:
        raise KiaraException(msg=f"Could not extract archive: {error}.")

    bundle = FileBundle.import_folder(out_dir, import_config=import_config)

    if attach_metadata:
        metadata = {
            "url": url,
            "response_headers": history,
            "request_time": datetime.utcnow().replace(tzinfo=pytz.utc).isoformat(),
            "import_config": import_config.dict(),
        }
        _metadata = DownloadBundleMetadata(**metadata)
        bundle.metadata = _metadata.dict()
        bundle.metadata_schema = DownloadMetadata.schema_json()

    return bundle
