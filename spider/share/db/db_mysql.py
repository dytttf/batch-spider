# coding:utf8
"""
用于MySQL数据库相关的一些方法
兼容 pymysql  MySQLdb

使用pymysql 在gevent下偶尔会出现
    RuntimeError: reentrant call inside <_io.BufferedReader name=8>
MySQLdb 好像不会 但是有机器环境依赖......
"""
import json
import copy
import time
import datetime
from collections import OrderedDict, defaultdict
from typing import List, Tuple

import pymysql

try:
    import MySQLdb
except ImportError:
    MySQLdb = None

from spider.share.utils import log

# 定义需要捕获的异常
catch_error = (pymysql.OperationalError, pymysql.ProgrammingError)

if MySQLdb:
    catch_error = (
        pymysql.OperationalError,
        MySQLdb.OperationalError,
        pymysql.ProgrammingError,
        MySQLdb.ProgrammingError,
    )


def get_mysql_conn(setting_dict, **kwargs):
    """
    建立mysql连接
    Args:
        setting_dict:
        **kwargs:

    Returns:

    """
    db_user = setting_dict["username"]
    db_passwd = setting_dict["password"]
    db_host = setting_dict["host"]
    db_port = setting_dict["port"]
    if db_port is None:
        db_port = 3306
    db_default = setting_dict["db"]
    params = setting_dict["params"]
    db_charset = params.get("charset", ["utf8mb4"])[0]

    if "pymysql" in setting_dict["type"]:
        conn = pymysql.connect(
            host=db_host,
            port=db_port,
            user=db_user,
            passwd=db_passwd,
            db=db_default,
            charset=db_charset,
            **kwargs,
        )
        conn.set_charset(db_charset)
    elif "mysqldb" in setting_dict["type"]:
        if not MySQLdb:
            # 抛出异常
            raise ImportError("can not import MySQLdb")
        conn = MySQLdb.connect(
            host=db_host,
            port=db_port,
            user=db_user,
            passwd=db_passwd,
            db=db_default,
            charset=db_charset,
            **kwargs,
        )
        conn.set_character_set(db_charset)
    else:
        raise ValueError("unknown mysql type: {}".format(setting_dict["type"]))
    conn.autocommit(True)
    return conn


class Cursor(object):
    """
    重新封装cursor使得可以自动捕获mysql连接超时错误并重新连接
    """

    def __init__(self, mysql_conn, setting_dict, logger=None, **kwargs):

        self.cursor = mysql_conn.cursor()
        self.conn = mysql_conn

        self.kwargs = kwargs
        self.setting_dict = setting_dict

        self.logger = self.kwargs.pop("logger", logger) or log.get_logger(__file__)

        self._init()

    def __getattr__(self, name):
        """不存在的属性调用原cursor"""
        return getattr(self.cursor, name)

    def _init(self):
        """
        初始化一些东西：
            1、设置session级别的变量 range_optimizer_max_mem_size=83886080  比默认值8M大10倍 优化update速度

        Returns:

        """
        # 设置 range_optimizer_max_mem_size 最小为 83886080
        if 1:
            try:
                # 查询原来的设置
                sql = "show variables like '%range_optimizer_max_mem_size%';"
                self.cursor.execute(sql)
                _r = self.cursor.fetchall()
                if _r:
                    # polordb 没有这个参数
                    old_size = int(_r[0][1])
                    if old_size < 83_886_080:
                        self.cursor.execute(
                            "set session range_optimizer_max_mem_size=83886080;"
                        )
            except Exception as e:
                self.logger.exception("set range_optimizer_max_mem_size failed")

        return

    def execute(self, sql, args=None, retry=0):
        try:
            result = self.cursor.execute(sql, args=args)
        except catch_error as e:
            # 捕获超时异常 关闭异常
            if retry < 20 and e.args[0] in (
                2006,
                2013,
                "cursor closed",
                "Cursor closed",
            ):
                self.logger.debug("mysql连接错误:{}  重连中...".format(e))
                time.sleep(retry * 5)
                # 重连
                mysql_conn = get_mysql_conn(self.setting_dict, **self.kwargs)
                self.cursor = mysql_conn.cursor()
                self.conn = mysql_conn
                return self.execute(sql, args=args, retry=retry + 1)
            raise e
        # except RuntimeError as e:
        #     # 处理 RuntimeError: reentrant call inside <_io.BufferedReader name=8>  gevent引发的错误
        #     # http://k.sina.com.cn/article_1708729084_65d922fc034002ecf.html
        #     # https://coveralls.io/builds/5985537/source?filename=src%2Fgevent%2Fsubprocess.py
        #     # 已知 pymysql==0.7.11 没问题
        #     # 而 pymysql==0.9.3 会在发现未知异常 比如 RuntimeError: reentrant call inside 强制关闭mysql连接
        #     # 所以不要升级
        #     # if "reentrant" in e.args[0] and retry < 3:
        #     #     self.logger.debug("mysql连接错误:{}".format(e))
        #     #     return self.execute(sql, args=args, retry=retry + 1)
        #     raise e
        return result


class MySQLOpt(object):
    # 连接池 共用
    connection_list = {}

    def __init__(self, setting_dict, **kwargs):
        """
        Args:
            setting_dict: mysql连接参数字典
            **kwargs:
                is_pool: 是否使用连接池模式
        """
        self.is_pool = kwargs.pop("is_pool", True)

        # 额外参数
        self.kwargs = kwargs

        self.setting_dict = setting_dict
        self.key = repr(setting_dict)

        self.logger = self.kwargs.pop("logger", None) or log.get_logger(__file__)

        self.table_name = ""

        # 私有变量
        self._conn = None
        self._cursor = None

    def __del__(self):
        try:
            self.close()
        except Exception as e:
            pass

    @property
    def conn(self):
        """
            获取mysql连接
        Returns:

        """
        self._conn = self.cursor.conn
        return self._conn

    @property
    def cursor(self):
        """
            获取游标
        Returns:

        """
        if not self._cursor:
            if not self.is_pool or self.key not in MySQLOpt.connection_list.keys():
                self._cursor = self.get_cursor()
                if self.is_pool:
                    MySQLOpt.connection_list[self.key] = self._cursor
            else:
                self._cursor = MySQLOpt.connection_list[self.key]
        return self._cursor

    def get_cursor(self, connection=None):
        """
            mysql获取cursor  重新封装过的
        Args:
            connection:

        Returns:

        """
        connection = connection or get_mysql_conn(self.setting_dict, **self.kwargs)
        cursor = Cursor(
            connection, self.setting_dict, logger=self.logger, **self.kwargs
        )
        return cursor

    @staticmethod
    def handle_values(data, keys=None, strip=True) -> OrderedDict:
        """
            处理字典中的值 转换为mysql接受的格式
        Args:
            data:
            keys: 使用给定的key顺序
            strip: 是否对字符串类型的值进行strip操作

        Returns:

        """
        if not keys:
            keys = list(data.keys())
        handle_k_list = []
        handle_v_list = []
        for k in keys:
            v = data[k]
            handle_k_list.append("`{}`".format(k))
            if isinstance(v, str):
                if strip:
                    v = v.strip()
                handle_v_list.append("'{}'".format(pymysql.escape_string(v)))
            elif isinstance(v, (int, float)):
                # np.nan is float
                handle_v_list.append("{}".format(v))
            elif isinstance(v, (datetime.date, datetime.time)):
                handle_v_list.append("'{}'".format(v))
            elif v is None:
                handle_v_list.append("null")
            else:
                v = json.dumps(v, ensure_ascii=False)
                handle_v_list.append("'{}'".format(pymysql.escape_string(v)))
        data = OrderedDict(zip(handle_k_list, handle_v_list))
        return data

    @staticmethod
    def group_data_by_keys(data: list):
        """
            根据key分组数据
        Args:
            data:

        Returns:

        """
        data = copy.deepcopy(data)
        group_data_dict = defaultdict(list)
        for item in data:
            keys = list(item.keys())
            keys.sort()
            keys_flag = " ".join(keys)
            group_data_dict[keys_flag].append(item)
        return list(group_data_dict.values())

    def add(
        self,
        data: dict,
        *,
        table_name: str = "",
        ignore_duplicate: bool = True,
        **kwargs,
    ):
        """
            保存数据
        Args:
            data: 数据
            table_name: 表名
            ignore_duplicate: 忽略重复错误
            **kwargs:

        Returns:
            0写入成功  1重复
        """
        data = copy.deepcopy(data)
        table_name = table_name or self.table_name
        if not table_name:
            raise ValueError("table name {}".format(table_name))
        if not data:
            raise ValueError("data is {}".format(data))
        sql = "insert into {table_name} ({keys}) values({values});"
        # 拼接sql
        order_data = self.handle_values(data)
        sql = sql.format(
            **{
                "keys": ",".join(order_data.keys()),
                "values": ",".join(order_data.values()),
                "table_name": table_name,
            }
        )
        try:
            self.cursor.execute(sql)
            resp = 0
        except Exception as e:
            if ignore_duplicate and "Duplicate entry" in str(e):
                self.logger.error(e)
                resp = 1
            else:
                raise e
        return resp

    def add_many(
        self,
        data: list,
        *,
        table_name: str = "",
        batch=100,
        group_by_keys: bool = False,
        ignore_unknown_column: bool = False,
        ignore_duplicate: bool = True,
        **kwargs,
    ):
        """
            保存数据  无视错误
        Args:
            data: 数据
            table_name: 表名
            batch: 分批入库数量
            group_by_keys: 是否按照keys分组处理 默认False  则如果遇到数据中存在key不一致的情况 会抛出异常
            ignore_unknown_column: 忽略表结构不一致错误 并且返回错误数据
            ignore_duplicate: 忽略重复错误
            **kwargs:

        Returns:

        """
        data = copy.deepcopy(data)
        table_name = table_name or self.table_name
        if not table_name:
            raise ValueError("table name {}".format(table_name))
        if not data:
            raise ValueError("data is {}".format(data))
        if not isinstance(data, (list, tuple)):
            data = [data]

        # 数据分组 按照keys是否相同
        # 防止由于key个数不一致导致的出错
        data_group_list = self.group_data_by_keys(data)
        if not group_by_keys:
            assert len(data_group_list) == 1, "数据结构不一致"

        error_data = []

        for data in data_group_list:
            # 固定keys 取第一个数据
            # 如果不固定 则当给定数据的key顺序不固定时会出现入库错乱
            first_data_keys = list(data[0].keys())
            resp = 0
            while data:
                _data = data[:batch]
                data = data[batch:]
                # 每次100
                if ignore_duplicate:
                    sql = "insert ignore into {table_name} ({keys}) values{values};"
                else:
                    sql = "insert into {table_name} ({keys}) values{values};"
                # 拼接sql
                values_list = []
                for item in _data:
                    order_data = self.handle_values(item, keys=first_data_keys)
                    keys = ",".join(order_data.keys())
                    values = ",".join(order_data.values())
                    values_list.append("({})".format(values))

                sql = sql.format(
                    **{
                        "keys": keys,
                        "values": ",".join(values_list),
                        "table_name": table_name,
                    }
                )
                try:
                    rows = self.cursor.execute(sql)
                    self.logger.debug("insert rows {}".format(rows))
                    resp = 0
                except Exception as e:
                    if "Unknown column" in str(e):
                        self.logger.debug("错误数据: {}".format(_data[0]))
                        if ignore_unknown_column:
                            error_data.extend(_data)
                            continue
                    raise e
        if ignore_unknown_column:
            return resp, error_data
        return resp

    def update(
        self,
        data,
        *,
        condition: dict = None,
        table_name: str = "",
        where_sql: str = "",
        **kwargs,
    ):
        """
            更新数据
        Args:
            data: 待更新字段 dict
            condition: 条件字段 dict 默认无序直接and
            table_name: 待更新表名
            where_sql: 若存在此字段则忽略 condition 适用于需要排序或者有除了and之外的where字句
            **kwargs:

        Returns:

        """
        data = copy.deepcopy(data)
        table_name = table_name or self.table_name
        if not table_name:
            raise ValueError("table name {}".format(table_name))
        if not condition:
            condition = {}
        sql = "update {table_name} set {update_data} where {where_sql};"
        order_data = self.handle_values(data)
        # 组合待更新数据
        update_data = ["{}={}".format(k, v) for k, v in order_data.items()]
        update_data = ",".join(update_data)

        # 组合条件语句 默认使用and连接
        order_condition = self.handle_values(condition, strip=False)
        order_condition = [f"{k}={v}" for k, v in order_condition.items()]
        order_condition = " and ".join(order_condition)

        # where_sql 优先
        where_sql = where_sql or order_condition

        sql = sql.format(
            **{
                "table_name": table_name,
                "update_data": update_data,
                "where_sql": where_sql,
            }
        )
        return self.cursor.execute(sql)

    def delete(
        self,
        *,
        condition: dict = None,
        table_name: str = "",
        where_sql: str = "",
        **kwargs,
    ):
        """
        根据条件删除内容
        Args:
            condition: 条件字段 dict 默认无序直接and
            table_name:
            where_sql: 若存在此字段则忽略 condition 适用于需要排序或者有除了and之外的where字句
            **kwargs:

        Returns:
            受影响行数
        """

        table_name = table_name or self.table_name
        if not table_name:
            raise ValueError("table name {}".format(table_name))
        if not condition:
            condition = {}
        sql = "delete from  {table_name}  where {where_sql};"

        # 组合条件语句 默认使用and连接
        order_condition = self.handle_values(condition)
        order_condition = ["{}={}".format(k, v) for k, v in order_condition.items()]
        order_condition = " and ".join(order_condition)

        # where_sql 优先
        where_sql = where_sql or order_condition

        sql = sql.format(**{"table_name": table_name, "where_sql": where_sql})
        return self.cursor.execute(sql)

    def query_all(self, sql, *, args=None) -> List[Tuple]:
        """
            cursor.execute + fetchall
        Args:
            sql:
            args:

        Returns:

        """
        _cursor = self.get_cursor(connection=self.conn)
        _cursor.execute(sql, args=args)
        result = _cursor.fetchall()
        _cursor.close()
        return result

    def query_single_attr(self, sql, *, args=None) -> List:
        """
            查询单个字段使用
        Args:
            sql: 查询sql
            args: list [1,2,3,4,5]

        Returns:

        """
        return [item[0] for item in self.query_all(sql, args=args)]

    def close(self):
        """
            关闭数据库连接
        Returns:

        """
        try:
            if self._cursor:
                self._cursor.close()
        except:
            pass
        try:
            if self._conn:
                self._conn.close()
            self.logger.debug("mysql连接关闭成功")
        except:
            pass
        # 删除链接池中的链接
        self._cursor = None
        self._conn = None
        MySQLOpt.connection_list.pop(self.key, "")

    def copy(self, *, protocol="", **kwargs):
        """
            复制一个新连接 可以修改有限的一些参数
        Args:
            protocol: mysql连接协议  mysql+pymysql   mysql+mysqldb
            **kwargs:

        Returns:

        """
        _kwargs = copy.deepcopy(self.kwargs)
        _kwargs.update(kwargs)
        _kwargs.update({"is_pool": False})
        #
        if protocol:
            assert protocol in ["mysql+pymysql", "mysql+mysqldb"]
            self.setting_dict["type"] = protocol
        #
        db_opt = MySQLOpt(self.setting_dict, **_kwargs)
        return db_opt
