# coding:utf8
"""
下载器
"""
import io
import os
import time
import random
from urllib.parse import quote as urlquote
from urllib.parse import urljoin
from collections import deque
from typing import List, Optional, Dict, AnyStr, Union

try:
    import pycurl
except:
    pycurl = None

try:
    from spider.network import geventcurl
except:
    geventcurl = None

import requests
import requests_ftp
from hyper.contrib import HTTP20Adapter
from requests.models import Response as requestsResponse
from requests.structures import CaseInsensitiveDict
from requests.utils import requote_uri

# 支持一下FTP的url下载
requests_ftp.monkeypatch_session()

from spider import util
from spider.utils import log
from spider.network import proxy

logger = log.get_logger(__file__)


class UserAgentPool(util.RequestArgsPool):
    """
    1、不同设备可选

    """

    # 设配类型
    pc = "pc"
    #:
    windows = "windows"
    mac = "mac"
    linux = "linux"
    compatible = "compatible"

    mobile = "mobile"
    #:
    android = "android"
    ios = "ios"
    #
    dev_types = [pc, mobile, windows, mac, linux, android, ios]

    def __init__(self, types: List[str] = None, **kwargs):
        """
        Args:
            types: ua设备类型
            **kwargs:
        """
        super().__init__(**kwargs)
        if not types:
            # 默认pc
            types = [self.windows, self.mac]
        self.types = types

        # ua列表
        self.user_agents = []
        # 初始化标志
        self.init_flag = 0
        # ua文件目录
        self.ua_dir = os.path.join(os.path.dirname(__file__), "user_agent_files")

    def init(self):
        """
        初始化默认ua列表
        Returns:

        """
        self.init_flag = 1

        type_func_dict = {
            self.pc: self.pc_user_agents,
            self.mobile: self.mobile_user_agents,
            self.windows: self.windows_user_agents,
            self.mac: self.mac_user_agents,
            self.linux: self.linux_user_agents,
            self.android: self.android_user_agents,
            self.ios: self.ios_user_agents,
        }
        for typ in self.types:
            ua_list = type_func_dict[typ]
            if ua_list:
                self.user_agents.extend(ua_list)
        return

    def get_ua_from_file(self, filename):
        """
            从文件获取ua列表
            get_ua_from_files("windows.txt")
        Args:
            filename:

        Returns:

        """
        with open(os.path.join(self.ua_dir, filename)) as f:
            lines = f.readlines()
        lines = [x.strip() for x in lines if x.strip()]
        return lines

    def get(self):
        """
            随机获取user-agent
        Returns:

        """
        if not self.init_flag:
            self.init()
        return random.choice(self.user_agents) if self.user_agents else ""

    @property
    def pc_user_agents(self):
        return self.windows_user_agents + self.mac_user_agents + self.linux_user_agents

    @property
    def windows_user_agents(self):
        return self.get_ua_from_file("windows.txt")

    @property
    def mac_user_agents(self):
        return self.get_ua_from_file("mac.txt")

    @property
    def compatible_user_agents(self):
        return self.get_ua_from_file("compatible.txt")

    @property
    def linux_user_agents(self):
        return self.get_ua_from_file("linux.txt")

    @property
    def mobile_user_agents(self):
        return self.android_user_agents + self.ios_user_agents

    @property
    def android_user_agents(self):
        return self.get_ua_from_file("android.txt")

    @property
    def ios_user_agents(self):
        return self.get_ua_from_file("ios.txt")


class CookiePool(util.RequestArgsPool):
    def __init__(self, **kwargs):
        """Cookie池"""
        super().__init__(**kwargs)

        self.cookie_list = deque(maxlen=10000)

    def get(self):
        try:
            r = self.cookie_list.pop()
        except Exception:
            r = None
        return r

    def add(self, cookies):
        self.cookie_list.append(cookies)
        return

    def __len__(self):
        return len(self.cookie_list)


class RefererPool(util.RequestArgsPool):
    def __init__(self, **kwargs):
        """Referer池"""
        super().__init__(**kwargs)


default_user_agent_pool = UserAgentPool()
default_cookie_pool = CookiePool()
default_referer_pool = RefererPool()


class Downloader(object):
    """
    定制
        代理池
        cookie池
        ua池
        referer池
    """

    def __init__(
        self,
        proxy_enable: bool = True,
        timeout: int = 20,
        proxy_pool=proxy.default_proxy_pool,
        cookie_pool=None,
        user_agent_pool=default_user_agent_pool,
        referer_pool=None,
        show_error_log: bool = False,
        show_fail_log: bool = True,
        use_session: bool = True,
        with_exception: bool = False,
        stream: bool = False,
        h2: bool = False,
        use_pycurl: bool = False,
        use_gevent_pycurl: bool = False,
        use_default_headers: bool = True,
        format_headers: bool = True,
        **kwargs
    ):
        """
        下载器
        Args:
            proxy_enable: 是否使用代理 默认 True
            timeout: http请求超时时间 默认 20s
            proxy_pool: 代理池 默认代理池
            cookie_pool: cookie池 默认 None
            user_agent_pool: User-Agent池 默认使用PC的UA池
            referer_pool: Referer池 默认 None
            show_error_log: 是否输出下载异常详细日志 默认False
            show_fail_log: 是否输出下载失败日志 默认True
            use_session: 是否使用session 默认True
            with_exception: 是否返回异常对象 默认False
            stream: 参见 requests.get 的参数 stream 默认False
            h2: 是否使用http/2协议
            use_pycurl: 是否使用pycurl下载 仅支持多线程 不支持协程
            use_gevent_pycurl: 是否使用gevent pycurl下载 仅支持多线程 不支持协程
            use_default_headers: 是否使用default_headers
            format_headers: 是否自动格式化header 默认 True
            **kwargs:
        """
        super().__init__()

        #
        session = requests.Session()
        adapter = requests.adapters.HTTPAdapter(
            pool_connections=1000, pool_maxsize=1000, max_retries=0
        )
        if h2:
            adapter = HTTP20Adapter()
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        self.session = session

        #
        # TODO  hyper的h2支持不好  访问google搜索会被ban
        self.h2 = h2
        self.use_pycurl = use_pycurl
        self.use_gevent_pycurl = use_gevent_pycurl

        # http请求超时时间
        self.timeout = timeout
        # 是否使用代理
        self.proxy_enable = proxy_enable
        # proxy 池
        self.proxy_pool: proxy.ProxyPool = proxy_pool
        # ua 池
        self.user_agent_pool = user_agent_pool
        # cookie 池
        self.cookie_pool: CookiePool = cookie_pool
        # referer 池
        self.referer_pool = referer_pool

        # 是否输出下载异常详细日志
        self.show_error_log = show_error_log
        # 是否数据下载失败日志 # TODO 优化  目前仅针对京东临时处理
        self.show_fail_log = show_fail_log
        # 是否使用session
        self.use_session = use_session
        # 是否返回异常信息  例如重定向次数超限这样的异常
        self.with_exception = with_exception
        # stream
        self.stream = stream
        # use_default_headers
        self.use_default_headers = use_default_headers
        # 是否格式化headers
        self.format_headers = format_headers
        # 多余参数
        self.kwargs = kwargs

        # requests 支持的参数列表
        self.requests_module_kwargs = [
            "params",
            "data",
            "json",
            "headers",
            "cookies",
            "files",
            "auth",
            "timeout",
            "allow_redirects",
            "proxies",
            "verify",
            "stream",
            "cert",
        ]

    def close(self):
        if self.cookie_pool:
            self.cookie_pool.close()
        if self.referer_pool:
            self.referer_pool.close()
        if self.user_agent_pool:
            self.user_agent_pool.close()

    @property
    def default_headers(self):
        headers = {
            "User-Agent": self.user_agent_pool.get() if self.user_agent_pool else "",
            "Accept": "*/*",
            "Accept-Encoding": "gzip, deflate",
            "Connection": "keep-alive",
        }
        return headers

    def convert_http_protocol(
        self, request: Union[Dict, AnyStr]
    ) -> Union[Dict, AnyStr]:
        """
            https 和 http 互转
        Args:
            request: url 或者 {"url": ""}

        Returns:

        """
        # 支持 request 为字典格式
        if isinstance(request, dict):
            url = request.get("url")
        else:
            url = request
        if url.startswith("https"):
            url = "http" + url[5:]
        elif url.startswith("http:"):
            url = "https" + url[4:]
        if isinstance(request, dict):
            request["url"] = url
        else:
            request = url
        return request

    def prepare_request(self, request, **kwargs):
        """
            预处理下载参数
        Args:
            request:
            **kwargs:

        Returns:

        """
        # 支持 request 为字典格式
        url = request
        if isinstance(request, dict):
            url = request.get("url")
            kwargs.update(request)

        # 默认方法get
        method = kwargs.get("method", "GET")

        # 处理headers
        default_headers = self.default_headers if self.use_default_headers else {}
        # 处理cookie
        _cookie = ""
        _cookies = {}
        if not self.cookie_pool is None:
            _cookie = self.cookie_pool.get()
            if _cookie:
                if isinstance(_cookie, dict):
                    _cookies = _cookie
                else:
                    default_headers["Cookie"] = _cookie
        # 处理referer
        _referer = ""
        if self.referer_pool:
            _referer = self.referer_pool.get()
            default_headers["Referer"] = _referer

        # 用户调用 优先级最高
        headers = kwargs.pop("headers", {})
        if headers:
            default_headers.update(headers)
        kwargs["headers"] = (
            util.format_headers(default_headers)
            if self.format_headers
            else default_headers
        )

        # 处理https验证
        if "verify" not in kwargs:
            kwargs["verify"] = False
        # 处理代理
        if "proxies" not in kwargs:
            if self.proxy_enable:
                kwargs["proxies"] = self.proxy_pool.get()
                if not kwargs["proxies"]:
                    raise Exception("no valid proxy")
        if "stream" not in kwargs:
            kwargs["stream"] = self.stream
        if "timeout" not in kwargs:
            kwargs["timeout"] = self.timeout

        # session自定义
        _session = kwargs.pop("session", None)

        # 处理 cookie 统一改为使用cookies参数 将headers和session.headers中的cookie都拿到cookies里
        # 之所以这么做  是因为requests库在处理redirect的时候 把headers里的Cookie参数给删除了  然后某些网站就坑了 比如微博跳转
        if "cookies" not in kwargs:
            cookies_str = kwargs["headers"].get("Cookie", "")
            cookies_str += ";" + (_session or self.session).headers.get("Cookie", "")
            kwargs["cookies"] = dict(
                [x.split("=", maxsplit=1) for x in cookies_str.split(";") if x.strip()]
            )
            kwargs["cookies"].update(_cookies)
        #

        # 处理多余参数
        for key in list(kwargs.keys()):
            if key not in self.requests_module_kwargs:
                kwargs.pop(key)

        # 下载
        if ("data" in kwargs or "json" in kwargs) and method == "GET":
            method = "POST"
        return _session, method, url, kwargs

    def _download_by_requests(self, method, url, session=None, **kwargs):
        """
            使用requests下载
        Args:
            method:
            url:
            session:
            **kwargs:

        Returns:

        """
        #
        if session:
            response = session.request(method, url, **kwargs)
        else:
            if url.startswith("ftp"):
                ftp_session = requests.Session()
                response = ftp_session.get(url, **kwargs)
                if not kwargs["stream"]:
                    ftp_session.close()
            else:
                if self.use_session:
                    response = self.session.request(method, url, **kwargs)
                else:
                    response = requests.request(method, url, **kwargs)
        return response

    def _download_by_pycurl(self, method, url, **kwargs) -> requestsResponse:
        """
            使用pycurl下载
        Args:
            method:
            url:
            **kwargs:

        Returns:

        """
        _pycurl = pycurl
        if self.use_gevent_pycurl:
            _pycurl = geventcurl
        #

        c = _pycurl.Curl()
        # 设置下载参数
        c.setopt(_pycurl.CUSTOMREQUEST, method)
        c.setopt(_pycurl.URL, requote_uri(url))
        # headers
        headers = kwargs.get("headers", {})
        h = []
        for k, v in headers.items():
            if v is None:
                continue
            if k.lower() == "referer":
                # 扩展safe 不然referer里的http://中的:会被编码  导致某些网站挂掉...
                v = urlquote(v, safe=";/?:@&=+$,")
            h.append("%s: %s" % (k, v))
        c.setopt(_pycurl.HTTPHEADER, h)
        # 设置accept_encoding
        accept_encoding = headers.get("Accept-Encoding", "")
        if accept_encoding:
            c.setopt(_pycurl.ACCEPT_ENCODING, accept_encoding)
        if not kwargs["verify"]:
            # 关闭 SSL 检查
            c.setopt(_pycurl.SSL_VERIFYPEER, 0)
            c.setopt(_pycurl.SSL_VERIFYHOST, 0)
        #
        c.setopt(_pycurl.TIMEOUT, kwargs["timeout"])
        # 解决无法自动加载重定向中的set-cookie选项问题 可能不仅仅是重定向
        # https://stackoverflow.com/questions/1458683/how-do-i-pass-cookies-on-a-curl-redirect
        c.setopt(_pycurl.COOKIEFILE, "")
        # 自动重定向
        if kwargs.get("allow_redirects", True):
            c.setopt(_pycurl.FOLLOWLOCATION, 1)
            # 设置最大重定向次数 拍脑袋定的10
            c.setopt(_pycurl.MAXREDIRS, 10)
        else:
            c.setopt(_pycurl.FOLLOWLOCATION, 0)
        # 代理
        if kwargs.get("proxies"):
            proxy_args = proxy.ProxyItem.parse_proxies(kwargs["proxies"])
            if proxy_args["user"]:
                c.setopt(
                    _pycurl.PROXY,
                    "http://{user}:{password}@{ip}:{port}".format(**proxy_args),
                )
            else:
                c.setopt(_pycurl.PROXY, "http://{ip}:{port}".format(**proxy_args))
        # buffer response headers
        response_headers_buffer = io.BytesIO()
        c.setopt(_pycurl.HEADERFUNCTION, response_headers_buffer.write)
        # buffer内容
        buffer = io.BytesIO()
        c.setopt(_pycurl.WRITEDATA, buffer)
        # http/2支持
        if self.h2:
            c.setopt(_pycurl.HTTP_VERSION, _pycurl.CURL_HTTP_VERSION_2_0)

        # TODO 支持stream

        # 发送下载请求
        c.perform()

        # 构造requests的Response对象
        r = requestsResponse()
        r.status_code = c.getinfo(_pycurl.RESPONSE_CODE)
        r._content = buffer.getvalue()
        r.raw = buffer
        # 解析headers
        r_headers = {}
        for line in (
            response_headers_buffer.getvalue().decode("iso-8859-1").split("\r\n")
        ):
            if ":" not in line:
                continue
            name, value = line.split(":", 1)
            r_headers[name.strip()] = value.strip()
        r.headers = CaseInsensitiveDict(r_headers)
        if "location" in r.headers:
            r.url = urljoin(url, r.headers["location"])
        else:
            r.url = url
        c.close()
        return r

    @util.retry_decorator(Exception)
    def _download(self, request, **kwargs) -> requestsResponse:
        """
            下载
        Args:
            request:
            **kwargs:

        Returns:

        """
        # 整理参数
        _session, method, url, kwargs = self.prepare_request(request, **kwargs)
        # 下载
        _download_start = time.time()
        if self.use_pycurl or self.use_gevent_pycurl:
            response = self._download_by_pycurl(method, url, **kwargs)
        else:
            response = self._download_by_requests(method, url, _session, **kwargs)
        _download_end = time.time()

        # 记录使用的属性
        response.meta = {
            "proxies": kwargs.get("proxies", None),
            "headers": kwargs["headers"].copy(),
            "cookies": kwargs["cookies"].copy(),
            "time": {
                "start": _download_start,
                "end": _download_end,
                "use": _download_end - _download_start,
            },
        }
        # 兼容
        response.proxies = kwargs.get("proxies", None)
        if not kwargs["stream"]:
            response.close()
        return response

    def download(self, request, **kwargs):
        response = None
        exception = None
        is_converted = False
        for i in range(2):
            try:
                response = self._download(request, **kwargs)
                if response is not None:
                    if not response and self.show_fail_log:
                        logger.error(
                            "download failed: {} {}".format(
                                response.status_code, response.url
                            )
                        )
                break
            except Exception as e:
                if not is_converted:
                    exception = e
                    request = self.convert_http_protocol(request)
                    is_converted = True
                else:
                    if self.show_error_log:
                        logger.exception(exception, exc_info=exception)
                    else:
                        logger.error("download exception: {}".format(exception))
        if self.with_exception:
            return response, exception
        return response


if __name__ == "__main__":
    pass
    # 用法示例
    downloader = Downloader()
    resp = downloader.download(
        "http://httpbin.org/headers", proxies=None, headers={"Accept-Encoding": None}
    )
    print(type(resp))
    print(resp)
    print(resp.url)
    print(resp.text)
    print(resp.headers)
    print(resp.meta)

    # resp = downloader.download("ftp://127.0.0.1:2121/a.txt")
    # print(resp)
    # print(resp.content.decode("gbk"))

    # print(downloader.convert_http_protocol("https://www.baidu.com"))
