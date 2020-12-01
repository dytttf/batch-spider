# coding:utf8
"""

"""
import tqdm
from requests.models import Response, CONTENT_CHUNK_SIZE


def response_to_file(
    response: Response,
    file_obj,
    show_progress: bool = False,
    progress_config: dict = None,
):
    """

    Args:
        response:
        file_obj:
        show_progress:
        progress_config: 进度条配置项
            若无 则


    Returns:

    """
    if not show_progress:
        file_obj.write(response.content)
    else:
        #
        _progress_config = {
            "desc": "文件下载进度:",
        }
        if progress_config:
            _progress_config.update(progress_config)
        #
        content_length = response.headers["Content-Length"]
        t = tqdm.tqdm(desc=progress_config["desc"], total=int(content_length))
        _iter = response.iter_content(CONTENT_CHUNK_SIZE)
        while 1:
            try:
                _content = next(_iter)
            except StopIteration:
                break
            file_obj.write(_content)
            t.update(len(_content))
        t.close()
    return
