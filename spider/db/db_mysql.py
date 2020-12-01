# coding:utf8
""""""
from spider.utils import log
from spider.share.db.db_mysql import MySQLOpt as _MySQLOpt

logger = log.get_logger(__file__)


class MySQLOpt(_MySQLOpt):
    def __init__(self, setting_dict, **kwargs):
        """
        Args:
            setting_dict: mysql连接参数字典
            **kwargs: is_pool: 是否使用连接池模式
        """
        if "logger" not in kwargs:
            kwargs["logger"] = logger
        super().__init__(setting_dict, **kwargs)
