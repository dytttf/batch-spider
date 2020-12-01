# coding:utf8
import datetime
import html
import json
import os
import platform
import re
import tempfile
import time
from typing import List, Tuple, Callable
from urllib import parse

import urllib3
from urllib3.exceptions import InsecureRequestWarning

from batch_spider import setting
from batch_spider.share.util import RedisLock as _RedisLock
from batch_spider.share.util import key2num, remove_control_characters
from batch_spider.utils import log


urllib3.disable_warnings(InsecureRequestWarning)


logger = log.get_logger(__file__)

cur_path = os.path.dirname(os.path.abspath(__file__))

temp_dir = tempfile.mkdtemp()

# redis锁使用的redis连接池
global_redis_lock_connection_pool_cache = {}


def retry_decorator(ExceptionToCheck, retry=2, delay=1, delay_ratio=1):
    """
    函数重试装饰器
    Args:
        ExceptionToCheck: 需要重试的异常
        retry: 重试次数
        delay: 重试间隔 = delay * (delay_ratio ** retry（当前第几次重试）)
        delay_ratio: 重试间隔增加比例

    Returns:

    """

    def deco_retry(f):
        def f_retry(self, *args, **kwargs):
            real_delay = delay
            lastException = None
            for mretry in range(retry):
                try:
                    return f(self, *args, **kwargs)
                except ExceptionToCheck as e:
                    # logger.error("function {} retring {} ...".format(f, mretry))
                    time.sleep(real_delay)
                    real_delay = delay * (delay_ratio ** mretry)
                    lastException = e
            if lastException is not None:
                # logger.exception(lastException)
                raise lastException

        return f_retry

    return deco_retry


def format_headers(headers: dict, url: str = "") -> dict:
    """
    规范一下headers
    Args:
        headers:
        url:

    Returns:

    """
    # headers统一转换为大写开头
    _headers = {}
    for k, v in headers.items():
        k = "-".join([x.capitalize() for x in k.split("-")])
        _headers[k] = v
    headers = _headers
    if not headers.get("Referer") and url:
        # 默认补充本身url为Referer
        headers["Referer"] = url
    return headers


def handle_html(text: str) -> str:
    """
        HTML文本统一处理
    Args:
        text:

    Returns:

    """
    text = html.unescape(text).strip()
    text = text.replace("\xa0", "")
    return text


def get_json(text, flag="{}", _eval=False, _globals: dict = None, _locals: dict = None):
    """
    将文本转换为json 支持eval操作
    Args:
        text:
        flag: 在jsonp中用于标识json开头的字符 一般为{}
        _eval: 是否使用eval解析
        _globals: 使用eval时可指定全局命名空间  例如: 将true解析为 True 可传递 _globals = {"true": True}
        _locals: 类似 _globals

    Returns:

    """
    l, r = text.find(flag[0]), text.rfind(flag[1])
    if _eval:
        if not _globals:
            _globals = globals()
        if not _locals:
            _locals = locals()
        return eval(text[l : r + 1], _globals, _locals)
    return json.loads(text[l : r + 1])


def find_groups(text: str, flag: str = "()", error: str = "strict") -> List[Tuple]:
    """
    >>> find_groups("(1)(2)")
    [(0, 2), (3, 5)]
    >>> find_groups("((1)(2))")
    [(0, 7), (1, 3), (4, 6)]
    >>> find_groups("'1'2'3'", flag="''")
    [(0, 2), (4, 6)]

    Args:
        text:
        flag:
        error:
            error等级  strict  or other
                    strict: 对text有要求  必须保证是合法的
                    other: 遇到不合法则停止寻找  返回已经找到的

    Returns:

    """
    brackets = []
    _brackets = []
    # TODO  support """1'2'34"5"6'7'7"""
    if flag[0] == flag[1]:
        for _idx, s in enumerate(text):
            if s == flag[0] and not _brackets:
                _brackets.append(_idx)
            elif s == flag[0]:
                try:
                    brackets.append((_brackets.pop(-1), _idx))
                except Exception as e:
                    if error == "strict":
                        raise e
                    break
    else:
        for _idx, s in enumerate(text):
            if s == flag[0]:
                _brackets.append(_idx)
            elif s == flag[1]:
                try:
                    brackets.append((_brackets.pop(-1), _idx))
                except Exception as e:
                    if error == "strict":
                        raise e
                    break
    brackets.sort(key=lambda x: x[0])
    return brackets


def local_datetime(data):
    """
        把data转换为日期时间，时区为东八区北京时间，能够识别：今天、昨天、5分钟前等等，如果不能成功识别，则返回datetime.datetime.now()
    Args:
        data:

    Returns:

    """
    dt = datetime.datetime.now()
    # html实体字符转义
    data = html.unescape(data)
    data = data.strip()
    try:
        if isinstance(data, bytes):
            data = data.decode()
    except Exception as e:
        logger.error("local_datetime() error: data is not utf8 or unicode : %s" % data)

    # 归一化
    data = (
        data.replace("年", "-")
        .replace("月", "-")
        .replace("日", " ")
        .replace("/", "-")
        .strip()
    )
    data = re.sub("\s+", " ", data)

    year = dt.year

    regex_format_list = [
        # 2013年8月15日 22:46:21
        ("(\w+ \w+ \d+ \d+:\d+:\d+ \+\d+ \d+)", "%a %b %d %H:%M:%S +0800 %Y", ""),
        # Wed Sep  5 12:37:25 2018
        ("(\w+ \w+ \d+ \d+:\d+:\d+ \d+)", "%a %b %d %H:%M:%S %Y", ""),
        # 2013年8月15日 22:46:21
        ("(\d{4}-\d{1,2}-\d{1,2} \d{1,2}:\d{1,2}:\d{1,2})", "%Y-%m-%d %H:%M:%S", ""),
        # "2013年8月15日 22:46"
        ("(\d{4}-\d{1,2}-\d{1,2} \d{1,2}:\d{1,2})", "%Y-%m-%d %H:%M", ""),
        # "2014年5月11日"
        ("(\d{4}-\d{1,2}-\d{1,2})", "%Y-%m-%d", ""),
        # "2014年5月"
        ("(\d{4}-\d{1,2})", "%Y-%m", ""),
        # "13年8月15日 22:46:21",
        ("(\d{2}-\d{1,2}-\d{1,2} \d{1,2}:\d{1,2}:\d{1,2})", "%y-%m-%d %H:%M:%S", ""),
        # "13年8月15日 22:46",
        ("(\d{2}-\d{1,2}-\d{1,2} \d{1,2}:\d{1,2})", "%y-%m-%d %H:%M", ""),
        # "8月15日 22:46:21",
        ("(\d{1,2}-\d{1,2} \d{1,2}:\d{1,2}:\d{1,2})", "%Y-%m-%d %H:%M:%S", "+year"),
        # "8月15日 22:46",
        ("(\d{1,2}-\d{1,2} \d{1,2}:\d{1,2})", "%Y-%m-%d %H:%M", "+year"),
        # "8月15日",
        ("(\d{1,2}-\d{1,2})", "%Y-%m-%d", "+year"),
        # "3 秒前",
        ("(\d+)\s*秒前", "", "-seconds"),
        # "3 秒前",
        ("(\d+)\s*分钟前", "", "-minutes"),
        # "3 小时前",
        ("(\d+)\s*小时前", "", "-hours"),
        # "3 秒前",
        ("(\d+)\s*天前", "", "-days"),
        # 今天 15:42:21
        ("今天\s*(\d{1,2}:\d{1,2}:\d{1,2})", "%Y-%m-%d %H:%M:%S", "date-0"),
        # 昨天 15:42:21
        ("昨天\s*(\d{1,2}:\d{1,2}:\d{1,2})", "%Y-%m-%d %H:%M:%S", "date-1"),
        # 前天 15:42:21
        ("前天\s*(\d{1,2}:\d{1,2}:\d{1,2})", "%Y-%m-%d %H:%M:%S", "date-2"),
        # 今天 15:42
        ("今天\s*(\d{1,2}:\d{1,2})", "%Y-%m-%d %H:%M", "date-0"),
        # 昨天 15:42
        ("昨天\s*(\d{1,2}:\d{1,2})", "%Y-%m-%d %H:%M", "date-1"),
        # 前天 15:42
        ("前天\s*(\d{1,2}:\d{1,2})", "%Y-%m-%d %H:%M", "date-2"),
    ]

    for regex, dt_format, flag in regex_format_list:
        m = re.search(regex, data)
        if m:
            if not flag:
                dt = datetime.datetime.strptime(m.group(1), dt_format)
            elif flag == "+year":
                # 需要增加年份
                dt = datetime.datetime.strptime("%s-%s" % (year, m.group(1)), dt_format)
            elif flag in ("-seconds", "-minutes", "-hours", "-days"):
                # 减秒
                flag = flag.strip("-")
                delta = eval("datetime.timedelta(%s=int(m.group(1)))" % flag)
                dt = dt - delta
            elif flag.startswith("date"):
                del_days = int(flag.split("-")[1])
                _date = dt.date() - datetime.timedelta(days=del_days)
                _date = _date.strftime("%Y-%m-%d")
                dt = datetime.datetime.strptime(
                    "%s %s" % (_date, m.group(1)), dt_format
                )
            return dt
    else:
        logger.error("unknow datetime format: %s" % data)
        dt = None
    return dt


def oom_killed_exit():
    """
        使用oom kill 退出码
    Returns:

    """
    import sys

    sys.exit(137)
    return


# 容器相关
class ContainerInfo(object):
    """
    容器相关信息获取
        tips:
            由于使用了os.popen新开一个进程去获取内存信息 新建进程需要占用一定的内存 所以在系统内存剩余很少的情况下 会抛出内存不够异常
    """

    @classmethod
    def _get_cgroup_mem_info(cls, name):
        with os.popen("cat /sys/fs/cgroup/memory/{}".format(name)) as f:
            value = f.read()
        return value

    @classmethod
    def _node_memory_info(cls):
        """
            宿主机内存信息
        Returns:

        """
        info = {}
        with os.popen("cat /proc/meminfo") as f:
            meminfo = dict([x.strip().split(":") for x in f.readlines() if x.strip()])
        info["total_bytes"] = int(meminfo["MemTotal"].replace("kB", "").strip()) * 1024
        info["available_bytes"] = (
            int(meminfo["MemAvailable"].replace("kB", "").strip()) * 1024
        )
        info["memory_utilization"] = 1.0 - (
            info["available_bytes"] * 1.0 / info["total_bytes"]
        )
        return info

    @classmethod
    def _container_memory_info(cls):
        """
            容器内存信息
        Returns:

        """
        info = {}
        info["limit_in_bytes"] = int(cls._get_cgroup_mem_info("memory.limit_in_bytes"))
        info["usage_in_bytes"] = int(cls._get_cgroup_mem_info("memory.usage_in_bytes"))
        info["memory_utilization"] = (
            info["usage_in_bytes"] * 1.0 / info["limit_in_bytes"]
        )
        return info

    @classmethod
    def memory_info(cls):
        """
            获取内存信息
        Returns:

        """
        if platform.system() in ["Linux"]:
            info = {
                "node": cls._node_memory_info(),  # 宿主机
                "container": cls._container_memory_info(),  # 容器
            }
        else:
            info = {}
        return info


def send_message(*args, **kwargs):
    # raise NotImplementedError
    logger.error("send_message need Implement")


class RequestArgsPool(object):
    """用于下载参数池定义"""

    def __init__(self, **kwargs):
        pass

    def get(self):
        raise NotImplementedError

    def close(self):
        pass

    def add(self, *args, **kwargs):
        raise NotImplementedError

    def delete(self, *args, **kwargs):
        raise NotImplementedError


class UrlHandler(object):
    def __init__(self, **kwargs):
        """url操作"""
        self.url = kwargs.get("url", "")

    @classmethod
    def parse_qs(
        cls,
        qs,
        keep_blank_values=False,
        strict_parsing=False,
        encoding="utf-8",
        errors="replace",
    ):
        parsed_result = {}
        pairs = cls.parse_qsl(
            qs, keep_blank_values, strict_parsing, encoding=encoding, errors=errors
        )
        for name, value in pairs:
            if name in parsed_result:
                parsed_result[name].append(value)
            else:
                parsed_result[name] = [value]
        return parsed_result

    @classmethod
    def parse_qsl(
        cls,
        qs,
        keep_blank_values=False,
        strict_parsing=False,
        encoding="utf-8",
        errors="replace",
    ):
        qs, _coerce_result = parse._coerce_args(qs)
        pairs = [s2 for s1 in qs.split("&") for s2 in s1.split(";")]
        r = []
        for name_value in pairs:
            if not name_value and not strict_parsing:
                continue
            nv = name_value.split("=", 1)
            if len(nv) != 2:
                if strict_parsing:
                    raise ValueError("bad query field: %r" % (name_value,))
                # Handle case of a control-name with no equal sign
                if keep_blank_values:
                    nv.append("")
                else:
                    continue
            if len(nv[1]) or keep_blank_values:
                name = nv[0].replace("+", " ")
                # 区别在这里
                # name = unquote(name, encoding=encoding, errors=errors)
                name = _coerce_result(name)
                value = nv[1].replace("+", " ")
                # value = unquote(value, encoding=encoding, errors=errors)
                value = _coerce_result(value)
                r.append((name, value))
        return r

    @classmethod
    def remove_query(cls, url="", is_delete=lambda x: x, keep_fragment=False, **kwargs):
        """
            从URL中移除指定参数
        Args:
            url:
            is_delete: 使用此函数判断是否删除指定名称的query参数
            keep_fragment: 是否保留 fragment (#)
            **kwargs:
                encoding: url参数编码  默认utf8
        Returns:

        """
        url = url or cls.url
        # 获取参数
        p = parse.urlparse(url)
        qs = cls.parse_qs(p.query)
        #
        query = []
        for key, value in qs.items():
            if is_delete(key):
                continue
            for v in value:
                query.append("{}={}".format(key, v))
        query = "&".join(query)
        # scheme, netloc, url, params, query, fragment
        fragment = p.fragment
        if not keep_fragment:
            fragment = ""
        url = parse.urlunparse((p.scheme, p.netloc, p.path, p.params, query, fragment))
        return url


class RedisLock(_RedisLock):
    def __init__(
        self,
        key,
        timeout=300,
        wait_timeout=8 * 3600,
        break_wait=None,
        redis_uri=None,
        auto_release=True,
        connection_pool=None,
    ):
        """
        redis超时锁
            用法示例:
            with RedisLock(key="test", timeout=10, wait_timeout=100, redis_uri="") as _lock:
                if _lock.locked:
                    # 用来判断是否加上了锁
                    # do somethings
        Args:
            key: 关键字  不同项目区分
            timeout: 锁超时时间
            wait_timeout: 等待加锁超时时间 默认8小时  防止多线程竞争时可能出现的 某个线程无限等待
                            <=0 则不等待 直接加锁失败
            break_wait: 可自定义函数 灵活控制 wait_timeout 时间 当此函数返回True时 不再wait
            redis_uri:
            connection_pool:
            auto_release: 是否自动释放锁 with语法下生效 默认True
        """
        redis_index = -1
        if not connection_pool and not redis_uri:
            # 使用key获取一个redis连接
            redis_index = key2num(key)
            redis_uri = setting.redis_spider_util_cluster_dict[redis_index]
        super().__init__(
            key,
            timeout=timeout,
            wait_timeout=wait_timeout,
            break_wait=break_wait,
            redis_uri=redis_uri,
            connection_pool=connection_pool,
            auto_release=auto_release,
            logger=logger,
        )
        self.redis_index = redis_index


def cron_exec(
    func: Callable,
    cron_expr: str,
    max_times=-1,
    default_utc=False,
    countdown=True,
    countdown_desc="waiting for next execute",
    ignore_exception=False,
):
    """

    Args:
        func: 待执行函数
        cron_expr: 定时器表达式
        max_times: 最大执行次数 默认一直执行
        default_utc: 是否使用UTC
        countdown: 是否输出倒计时
        countdown_desc: 自定义倒计时描述
        ignore_exception: 自定义倒计时描述

    Returns:

    """
    from crontab import CronTab

    #
    times = 0
    cron = CronTab(cron_expr)
    #
    exc = None
    while 1:
        if max_times >= times:
            break
        _next = cron.next(default_utc=default_utc)
        for i in range(int(_next)):
            time.sleep(1)
            _next -= 1
            if countdown:
                print(f"\r{countdown_desc}: {int(_next)} s", end="")
        print()
        print("starting ...")
        try:
            func()
        except Exception as e:
            exc = e
            if not ignore_exception:
                raise exc
            else:
                logger.exception(e)

        print("finished ...")
        time.sleep(1)
        times += 1

    #

    return exc


if __name__ == "__main__":
    pass

    # def a():
    #     raise 111
    #     return
    #
    # # cron_exec(a, "* * * * *")
    # cron_exec(a, "* * * * *", countdown=True, ignore_exception=True)
