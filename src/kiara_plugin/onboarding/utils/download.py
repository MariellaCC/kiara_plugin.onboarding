# -*- coding: utf-8 -*-
import tempfile
from datetime import datetime
from typing import Dict, List, Union

import httpx
import pytz
from pydantic import BaseModel, Field

from kiara.models.filesystem import FileModel


class DownloadMetadata(BaseModel):
    url: str = Field(description="The url of the download request.")
    response_headers: List[Dict[str, str]] = Field(
        description="The response headers of the download request."
    )
    request_time: str = Field(description="The time the request was made.")


def download_file(
    url: str, file_name: Union[str, None] = None, attach_metadata: bool = True
) -> FileModel:

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
