# coding:utf8
"""
"""
import time
import hashlib
from urllib import parse


try:
    import MySQLdb
except ImportError:
    MySQLdb = None


class DB(object):
    """
    数据库工厂
        db = DB().create("mysql://root:123456@localhost:3306/test")
        db = DB().create("mysql+pymysql://root:123456@localhost:3306/test")
        db = DB().create("mysql+mysqldb://root:123456@localhost:3306/test")
        mysql:
            mysql://root:123456@localhost:3306/test
        oracle:
            oracle://username:password@ip:port/db?charset=utf8
            oracle://sys:chang_on_install@127.0.0.1:1521/orcl?charset=utf8
    """

    def __init__(self, **kwargs):
        pass

    def create(self, conn_str, *, is_pool=True, **kwargs):
        """
        创建一个数据库连接
        Args:
            conn_str: protocol://user:password@host:port/params
            is_pool: 是否使用链接池模式
            **kwargs:

        Returns:

        """
        if conn_str is None:
            raise Exception("参数为空")

        # 收集参数
        kwargs["is_pool"] = is_pool

        # 处理#号密码bug  使用随机字符串替换#
        #
        random_replace_str = ""
        if "#" in conn_str:
            random_replace_str = hashlib.md5(
                str(time.time() * 1000).encode()
            ).hexdigest()
            conn_str = conn_str.replace("#", random_replace_str)

        db_setting_uri = parse.urlparse(conn_str)

        setting_dict = {}

        setting_dict["type"] = db_setting_uri.scheme.strip()
        setting_dict["host"] = db_setting_uri.hostname.strip()
        setting_dict["port"] = db_setting_uri.port
        setting_dict["username"] = db_setting_uri.username.strip()
        setting_dict["password"] = db_setting_uri.password.strip()
        setting_dict["db"] = db_setting_uri.path.strip("/").strip()
        setting_dict["params"] = parse.parse_qs(db_setting_uri.query)

        # 恢复密码中的#
        if random_replace_str:
            setting_dict["password"] = setting_dict["password"].replace(
                random_replace_str, "#"
            )

        if setting_dict["type"].startswith("mysql"):
            # 默认使用 mysqldb
            if setting_dict["type"] == "mysql":
                if MySQLdb:
                    setting_dict["type"] = "mysql+mysqldb"
                else:
                    setting_dict["type"] = "mysql+pymysql"
            try:
                from . import db_mysql
            except ImportError:
                import db_mysql
            self.db_opt = db_mysql.MySQLOpt(setting_dict, **kwargs)
        elif setting_dict["type"].startswith("oracle"):
            try:
                from . import db_oracle
            except:
                import db_oracle
            self.db_opt = db_oracle.OracleOpt(setting_dict, **kwargs)
        else:
            raise Exception("未知的协议：%s" % setting_dict["type"])
        return self.db_opt
