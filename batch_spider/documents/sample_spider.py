# coding:utf8
"""
最简爬虫代码示例
"""

# 导入依赖
from batch_spider.spiders import Spider, Request, Response

from bs4 import BeautifulSoup


# 编写自己的爬虫 名字根据项目需求而定
class MySpider(Spider):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        # 代理开关  默认使用代理
        self.downloader.proxy_enable = False

    def start_requests(self):
        """入口函数"""
        for i in range(1000):
            yield Request("https://httpbin.org/ip")

    def parse(self, response: Response):
        # 获取请求体
        request = response.request
        print(request)
        # 获取原生requests模块的Response
        _response = response.response
        print(_response.json())
        # 调用BeautifulSoup解析
        _response.encoding = "utf8"
        soup = BeautifulSoup(_response.text, "html.parser")
        print(soup.title)
        return


# 启动
if __name__ == "__main__":
    with MySpider(pool_size=1) as my_spider:
        my_spider.run()
