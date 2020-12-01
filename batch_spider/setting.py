# coding:utf8
"""配置文件
"""
import os

if 1:
    # 数据库配置
    def get_mysql_uri(x, protocol="mysql"):
        return (
            "{protocol}://{user}:{passwd}@{host}:{port}/{db}?charset={charset}".format(
                protocol=protocol, **x
            )
        )

    # mysql链接配置
    mysql_base_setting_dict = {
        "port": 3306,
        "user": os.getenv("SPIDER_MYSQL_USER"),
        "passwd": os.getenv("SPIDER_MYSQL_PASSWD"),
        "charset": "utf8mb4",
        "db": "spider_data",
    }

    # 兼容
    default_mysql_setting_dict = mysql_base_setting_dict.copy()
    default_mysql_setting_dict.update({"host": "localhost"})
    default_mysql_uri = get_mysql_uri(default_mysql_setting_dict)
    #

if 1:
    # redis 配置
    def get_redis_uri(db, host, password=""):
        if password:
            return "redis://:{}@{}/{}".format(password, host, db)
        else:
            return "redis://{}/{}".format(host, db)

    # 协作redis 1
    redis_spider_cooperation_1_host = "localhost:6379"
    spider_redis_cooperation_1_uri = get_redis_uri(0, redis_spider_cooperation_1_host)
    default_redis_uri = spider_redis_cooperation_1_uri

    # 用于redis锁
    redis_spider_util_cluster_dict = {
        0: spider_redis_cooperation_1_uri,
        1: spider_redis_cooperation_1_uri,
        2: spider_redis_cooperation_1_uri,
        3: spider_redis_cooperation_1_uri,
    }

if 1:
    # 代理文件别名
    proxy_name_default = "proxy.txt"  # 默认
    # 代理本地文件缓存时间
    local_proxy_file_cache_timeout = {}
    # 代理服务地址
    proxy_server = "http://host:port"

    def get_proxy_uri(proxy_name=proxy_name_default):
        """获取代理完整地址"""
        return "{}/{}".format(proxy_server, proxy_name)

    def get_local_proxy_file_cache_timeout(proxy_name):
        """获取代理本地缓存超时时间"""
        return local_proxy_file_cache_timeout.get(proxy_name, 60)


############## 导入用户自定义的setting #############
try:
    from setting import *
except:
    pass
