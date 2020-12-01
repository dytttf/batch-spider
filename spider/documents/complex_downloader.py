# coding:utf8
from spider.network.downloader import Downloader, UserAgentPool, CookiePool, RefererPool
from spider.network import proxy
from spider import setting

# 构造下载器对象
downloader = Downloader()
# 不使用代理
if 1:
    downloader = Downloader(proxy_enable=False)
    # downloader = Downloader()
    # downloader.proxy_enable = False
# 自定义代理池
if 1:
    downloader = Downloader(proxy_pool=proxy.default_proxy_pool)
    # downloader = Downloader()
    # downloader.proxy_pool = proxy.ProxyPool(setting.get_proxy_uri("proxy_all.txt"))
# 自定义headers
if 1:
    headers = {}
    url = "http://www.baidu.com"
    # 方式1 其他任何requests模块的request方法支持的参数都可以使用
    response = downloader.download(url, headers=headers)

    # 方式2
    request = {"url": url, "headers": headers, "data": {}}
    response = downloader.download(request)
# 定制User-Agent一般类型
if 1:
    ua_pool = UserAgentPool(types=["mobile"])
    downloader = Downloader(user_agent_pool=ua_pool)
# 定制Cookie池  Referer池
if 1:
    import random

    # 继承CookiePool  必须存在get方法
    class MyCookiePool(CookiePool):
        def __init__(self, **kwargs):
            super().__init__(**kwargs)

            self.cookie_list = [1, 2, 3, 4]  # 仅仅举个栗子

        def get(self):
            return "".format(random.choice(self.cookie_list))
            # Referer池和Cookie池用法一样


# 获取下载成功使用的代理和headers
if 1:
    response = downloader.download("http://www.baidu.com")
    print(response.meta)
