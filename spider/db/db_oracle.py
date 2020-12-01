# coding:utf8
""""""
from spider.utils import log
from spider.share.db.db_oracle import OracleOpt as _OracleOpt

logger = log.get_logger(__file__)


class OracleOpt(_OracleOpt):
    def __init__(self, setting_dict, **kwargs):
        """
        Args:
            setting_dict:  oracle 连接参数字典
            **kwargs: is_pool: 是否使用连接池模式
        """
        super().__init__(setting_dict, logger=logger, **kwargs)
