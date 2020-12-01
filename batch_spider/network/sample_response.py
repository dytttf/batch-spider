# coding:utf8
from typing import Optional

try:
    from requests.models import Response as requests_response
except:
    requests_response = None

from batch_spider.network.sample_request import Request


class Response(object):
    def __init__(
        self,
        response: Optional[requests_response],
        request: Request,
        exception=None,
        **kwargs
    ):
        """
        爬虫下载相应对象
        Args:
            response: requests.models.Response 对象
            request: spider.network.sample_request.Request
            exception: 异常对象
            **kwargs:
        """
        self.request = request
        self.response = response
        self.exception = exception
        self.kwargs = kwargs

    def __del__(self):
        try:
            self.response.close()
        except Exception as e:
            pass
