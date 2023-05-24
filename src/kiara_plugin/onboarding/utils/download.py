# -*- coding: utf-8 -*-
import tempfile
from datetime import datetime
from typing import Any, Dict, Tuple, Union

import httpx
import pytz

from kiara.models.filesystem import FileModel


def download_file(
    url: str, file_name: Union[str, None] = None
) -> Tuple[FileModel, Dict[str, Any]]:

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

    metadata = {
        "response_headers": history,
        "request_time": datetime.utcnow().replace(tzinfo=pytz.utc).isoformat(),
    }
    return result_file, metadata
