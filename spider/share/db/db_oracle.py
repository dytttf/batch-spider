# coding:utf8
"""
Oracle操作

# dsn = cx_Oracle.makedsn("10.40.34.248", 1521, "oracledb")
# print(dsn)
# db=cx_Oracle.connect('scott','tiger',dsn)


"""
import os

os.environ["NLS_LANG"] = "SIMPLIFIED CHINESE_CHINA.UTF8"

import copy
import time
import json
import datetime
from collections import OrderedDict, defaultdict

import cx_Oracle

from spider.share.utils import log

catch_error = (cx_Oracle.DatabaseError,)


def escape_string(value):
    value = value.replace("'", "''")
    return value


def get_oracle_connect(setting_dict, **kwargs):
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
        db_port = 1521
    db_default = setting_dict["db"]
    params = setting_dict["params"]
    db_charset = params.get("charset", ["utf8"])[0]
    dsn = cx_Oracle.makedsn(db_host, db_port, db_default)
    conn = cx_Oracle.connect(
        user=db_user,
        password=db_passwd,
        dsn=dsn,
        encoding=db_charset,
        threaded=True,
        events=True,
        **kwargs
    )
    conn.autocommit = 1
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

    def __getattr__(self, name):
        """
            不存在的属性调用原cursor
        Args:
            name:

        Returns:

        """
        return getattr(self.cursor, name)

    def execute(self, sql, parameters=None, retry=0):
        try:
            parameters = parameters or {}
            result = self.cursor.execute(sql, **parameters)
        except catch_error as e:
            # 捕获超时异常 关闭异常
            error_string = str(e)
            if retry < 20 and (
                "超出最大空闲时间" in error_string
                or "重新连接" in error_string
                or "您将被注销" in error_string
                or "没有登录" in error_string
            ):
                # cx_Oracle.DatabaseError: ORA-02396: 超出最大空闲时间, 请重新连接
                self.logger.error("oracle 连接错误:{}  重连中...".format(error_string))
                time.sleep(retry * 5)
                # 重连
                oracle_connect = get_oracle_connect(self.setting_dict, **self.kwargs)
                self.cursor = oracle_connect.cursor()
                self.conn = oracle_connect
                return self.execute(sql, parameters=parameters, retry=retry + 1)
            raise e
        return result


class OracleOpt(object):
    # 连接池 共用
    connection_list = {}

    def __init__(self, setting_dict, logger=None, **kwargs):
        self.is_pool = kwargs.pop("is_pool", True)
        # 额外参数
        self.kwargs = kwargs

        self.setting_dict = setting_dict
        self.key = repr(setting_dict)

        self.logger = self.kwargs.pop("logger", logger) or log.get_logger(__file__)

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
            if not self.is_pool or self.key not in OracleOpt.connection_list.keys():
                self._cursor = self.get_cursor()
                if self.is_pool:
                    OracleOpt.connection_list[self.key] = self._cursor
            else:
                self._cursor = OracleOpt.connection_list[self.key]
        return self._cursor

    def get_cursor(self, connection=None):
        """
            mysql获取cursor  重新封装过的
        Args:
            connection:

        Returns:

        """
        connection = connection or get_oracle_connect(self.setting_dict, **self.kwargs)
        cursor = Cursor(
            connection, self.setting_dict, logger=self.logger, **self.kwargs
        )
        return cursor

    def handle_values(self, data, keys=None) -> OrderedDict:
        """
            处理字典中的值 转换为mysql接受的格式
        Args:
            data:
            keys: 使用给定的key顺序

        Returns:

        """
        if not keys:
            keys = list(data.keys())
        handle_k_list = []
        handle_v_list = []
        for k in keys:
            v = data[k]
            handle_k_list.append("{}".format(k))
            if isinstance(v, str):
                v = v.strip()
                # 处理clob
                if len(v) > 4000:
                    v_list = []
                    _step = 2000
                    for i in range(0, len(v), _step):
                        v_list.append(v[i : i + _step])
                    v_list = ["to_clob('{}')".format(escape_string(x)) for x in v_list]
                    v = "||".join(v_list)
                    handle_v_list.append("{}".format(v))
                else:
                    handle_v_list.append("'{}'".format(escape_string(v)))
            elif isinstance(v, (int, float)):
                handle_v_list.append("{}".format(v))
            elif isinstance(v, (datetime.date, datetime.time)):
                handle_v_list.append("'{}'".format(v))
            elif v is None:
                handle_v_list.append("null")
            else:
                v = json.dumps(v, ensure_ascii=False)
                handle_v_list.append("'{}'".format(escape_string(v)))
        data = OrderedDict(zip(handle_k_list, handle_v_list))
        return data

    def _group_data_by_keys(self, data: list):
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
        self, data: dict, *, table_name: str = "", ignore_duplicate: int = 1, **kwargs
    ) -> int:
        """
            保存数据
        Args:
            data: 数据
            table_name:
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
        sql = 'INSERT INTO "{table_name}" ("{keys}") VALUES({values})'
        # 拼接sql
        order_data = self.handle_values(data)
        sql = sql.format(
            **{
                "keys": '","'.join(order_data.keys()),
                "values": ",".join(order_data.values()),
                "table_name": table_name,
            }
        )
        _cursor = self.get_cursor(self.conn)
        try:
            _cursor.execute(sql)
            resp = 0
        except Exception as e:
            if ignore_duplicate and (
                "unique constraint" in str(e) or "违反唯一约束条件" in str(e)
            ):
                self.logger.error(e)
                resp = 1
            else:
                raise e
        finally:
            _cursor.close()
        return resp

    def add_many(
        self,
        data: list,
        *,
        table_name: str = "",
        batch=100,
        group_by_keys: bool = False,
        ignore_unknown_column: bool = False,
        ignore_unique_key: str = None,
        **kwargs
    ):
        """
            保存数据  无视错误
        Args:
            data: 数据
            table_name: 表名（区分大小写）
            batch: 分批入库数量
            group_by_keys: 是否按照keys分组处理 默认False  则如果遇到数据中存在key不一致的情况 会抛出异常
            ignore_unknown_column: 忽略表结构不一致错误 并且返回错误数据
            ignore_unique_key: 忽略的唯一索引名称（区分大小写）
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
        data_group_list = self._group_data_by_keys(data)
        if not group_by_keys:
            assert len(data_group_list) == 1, "数据结构不一致"

        error_data = []
        dup_index_hint = ""
        if ignore_unique_key:
            dup_index_hint = '/*+IGNORE_ROW_ON_DUPKEY_INDEX("{}","{}")*/'.format(
                table_name, ignore_unique_key
            )

        _cursor = self.get_cursor(self.conn)

        for data in data_group_list:
            # 固定keys 取第一个数据
            # 如果不固定 则当给定数据的key顺序不固定时会出现入库错乱
            first_data_keys = list(data[0].keys())
            resp = 0
            while data:
                _data = data[:batch]
                data = data[batch:]
                # 每次100
                sql = 'INSERT {hint} INTO "{table_name}"("{keys}") {values}'
                # 拼接sql
                values_list = []
                for item in _data:
                    order_data = self.handle_values(item, keys=first_data_keys)
                    keys = '","'.join(order_data.keys())
                    values = ",".join(order_data.values())
                    values_list.append(values)

                value_sql_list = ["select {} from dual".format(values_list[0])]
                if len(values_list) > 1:
                    value_sql_list.extend(
                        "union all select {} from dual".format(x)
                        for x in values_list[1:]
                    )
                sql = sql.format(
                    **{
                        "hint": dup_index_hint,
                        "table_name": table_name,
                        "keys": keys,
                        "values": " ".join(value_sql_list),
                    }
                )
                try:
                    _cursor.execute(sql)
                    self.logger.debug("insert rows {}".format(_cursor.rowcount))
                    resp = 0
                except Exception as e:
                    if "Unknown column" in str(e):
                        self.logger.debug("错误数据: {}".format(_data[0]))
                        if ignore_unknown_column:
                            error_data.extend(_data)
                            continue
                    raise e
        _cursor.close()
        if ignore_unknown_column:
            return resp, error_data
        return resp

    def query_all(self, sql, *, parameters: dict = None):
        """

        Args:
            sql:
            parameters:

        Returns:

        """
        _cursor = self.get_cursor(connection=self.conn)
        r = _cursor.execute(sql, parameters=parameters)
        # 仅当sql是一个query时 才会有返回值 否则是None
        # https://cx-oracle.readthedocs.io/en/latest/cursor.html#Cursor.execute
        if r:
            result = _cursor.fetchall()
        else:
            result = []
        _cursor.close()
        return result

    def get_unique_indexs(self, table_name: str):
        """
            获取唯一索引列表
        Args:
            table_name:

        Returns:

        """
        sql = "SELECT INDEX_NAME FROM USER_INDEXES WHERE UNIQUENESS='UNIQUE' AND TABLE_NAME='{}'".format(
            table_name
        )
        indexes = self.query_all(sql)
        return [x[0] for x in indexes]

    def close(self):
        """
            关闭数据库连接
        Returns:

        """
        try:
            self.cursor.close()
        except:
            pass
        try:
            self.conn.close()
            self.logger.debug("oracle连接关闭成功")
        except:
            pass
        # 删除链接池中的链接
        self._cursor = None
        self._conn = None
        OracleOpt.connection_list.pop(self.key, "")

    def copy(self, **kwargs):
        """
            复制者肯定是想要一个不一样的  要不然还复制干嘛
        Args:
            **kwargs:

        Returns:

        """
        _kwargs = copy.deepcopy(self.kwargs)
        _kwargs.update(kwargs)
        _kwargs.update({"is_pool": False})
        db_opt = OracleOpt(self.setting_dict, **_kwargs)
        return db_opt
