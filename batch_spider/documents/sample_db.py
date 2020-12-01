# coding:utf8
from batch_spider import setting
from batch_spider.db import DB

# mysql连接
# setting.spider_mysql_1_uri
# 格式  mysql://username:password@host:port/db?charset=utf8mb4
db = DB().create(setting.default_mysql_uri)
db.cursor.execute("show tables;")
db.close()
