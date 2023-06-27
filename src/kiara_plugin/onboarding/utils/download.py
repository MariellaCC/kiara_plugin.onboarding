# -*- coding: utf-8 -*-
import atexit
import os
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple, Union

from pydantic import BaseModel, Field

from kiara.exceptions import KiaraException
from kiara.models.filesystem import FolderImportConfig, KiaraFile, KiaraFileBundle


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
    url: str,
    target: Union[str, None] = None,
    file_name: Union[str, None] = None,
    attach_metadata: bool = True,
    return_md5_hash: bool = False,
) -> Union[KiaraFile, Tuple[KiaraFile, str]]:

    import hashlib

    import httpx
    import pytz

    if not target:
        tmp_file = tempfile.NamedTemporaryFile(delete=False)
        _target = Path(tmp_file.name)
    else:
        _target = Path(target)
        _target.parent.mkdir(parents=True, exist_ok=True)

    if return_md5_hash:
        hash_md5 = hashlib.md5()  # noqa

    history = []
    datetime.utcnow().replace(tzinfo=pytz.utc)
    with open(_target, "wb") as f:
        with httpx.stream("GET", url, follow_redirects=True) as r:
            history.append(dict(r.headers))
            for h in r.history:
                history.append(dict(h.headers))
            for data in r.iter_bytes():
                if return_md5_hash:
                    hash_md5.update(data)
                f.write(data)

    if not file_name:
        # TODO: make this smarter, using content-disposition headers if available
        file_name = url.split("/")[-1]

    result_file = KiaraFile.load_file(_target.as_posix(), file_name)

    if attach_metadata:
        metadata = {
            "url": url,
            "response_headers": history,
            "request_time": datetime.utcnow().replace(tzinfo=pytz.utc).isoformat(),
        }
        _metadata = DownloadMetadata(**metadata)
        result_file.metadata["download_info"] = _metadata.dict()
        result_file.metadata_schemas["download_info"] = DownloadMetadata.schema_json()

    if return_md5_hash:
        return result_file, hash_md5.hexdigest()
    else:
        return result_file


def download_file_bundle(
    url: str,
    attach_metadata: bool = True,
    import_config: Union[FolderImportConfig, None] = None,
) -> KiaraFileBundle:

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

    bundle = KiaraFileBundle.import_folder(out_dir, import_config=import_config)

    if import_config is None:
        ic_dict = {}
    elif isinstance(import_config, FolderImportConfig):
        ic_dict = import_config.dict()
    else:
        ic_dict = import_config
    if attach_metadata:
        metadata = {
            "url": url,
            "response_headers": history,
            "request_time": datetime.utcnow().replace(tzinfo=pytz.utc).isoformat(),
            "import_config": ic_dict,
        }
        _metadata = DownloadBundleMetadata(**metadata)
        bundle.metadata["download_info"] = _metadata.dict()
        bundle.metadata_schemas["download_info"] = DownloadMetadata.schema_json()

    return bundle
