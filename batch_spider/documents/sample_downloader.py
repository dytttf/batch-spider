# coding:utf8
from batch_spider.network.downloader import Downloader, UserAgentPool, CookiePool, RefererPool

# 构造下载器对象
downloader = Downloader()

response = downloader.download("http://www.baidu.com")
print(response)
