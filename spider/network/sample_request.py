# coding:utf8


class Request(object):
    def __init__(
        self, request, callback=None, meta: dict = None, downloader=None, **kwargs
    ):
        """
        Args:
            request: url 或者  {"url": "", "headers": {}},
            callback:
            meta:
            downloader:
            **kwargs:
        """
        self.request = request
        self.callback = callback
        self.meta = meta
        # 记录重试次数
        self.retry = 0
        # 可单独指定downloader
        self.downloader = downloader

        # 快捷方式
        self.url = request
        self.data = None
        if isinstance(request, dict):
            self.url = request.get("url")
            self.data = request.get("data")
