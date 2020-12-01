# coding:utf8
"""
代理池
"""
import os
import time
import json
import random
import socket
import datetime
from urllib import parse
from collections import deque
from typing import Union, List, Dict

import redis
import requests

from spider.share.utils import log

# 建立本地缓存代理文件夹
proxy_path = os.path.join(os.path.dirname(__file__), "proxy_file")
if not os.path.exists(proxy_path):
    os.mkdir(proxy_path)


# 代理类型定义
class LimitProxy(object):
    """
        提取次数有限制的代理
    """

    pass


def get_proxy_from_url(**kwargs):
    """
        获取指定url的代理
    Args:
        **kwargs:

    Returns:

    """
    proxy_source_url = kwargs.get("proxy_source_url", [])
    if not isinstance(proxy_source_url, list):
        proxy_source_url = [proxy_source_url]
        proxy_source_url = [x for x in proxy_source_url if x]
    if not proxy_source_url:
        raise ValueError("no specify proxy_source_url: {}".format(proxy_source_url))
    kwargs = kwargs.copy()
    kwargs.pop("proxy_source_url")
    proxies_list = []
    for url in proxy_source_url:
        if url.startswith("http"):
            proxies_list.extend(get_proxy_from_http(url, **kwargs))
        elif url.startswith("redis"):
            proxies_list.extend(get_proxy_from_redis(url, **kwargs))

    if proxies_list:
        # 顺序打乱
        random.shuffle(proxies_list)

    return proxies_list


def get_proxy_from_http(proxy_source_url, **kwargs):
    """
        从指定 http 地址获取代理
    Args:
        proxy_source_url:
        **kwargs:

    Returns:

    """
    filename = proxy_source_url.split("/")[-1]
    abs_filename = os.path.join(proxy_path, filename)
    update_interval = kwargs.get("local_proxy_file_cache_timeout", 60)
    update_flag = 0
    if not update_interval:
        # 强制更新
        update_flag = 1
    elif not os.path.exists(abs_filename):
        # 文件不存在则更新
        update_flag = 1
    elif time.time() - os.stat(abs_filename).st_mtime > update_interval:
        # 超过更新间隔
        update_flag = 1
    if update_flag:
        response = requests.get(proxy_source_url, timeout=20)
        with open(os.path.join(proxy_path, filename), "w") as f:
            f.write(response.text)
    return get_proxy_from_file(filename)


def get_proxy_from_file(filename, **kwargs):
    """
        从指定本地文件获取代理
            文件格式
            ip:port:https
            ip:port:http
            ip:port
    Args:
        filename:
        **kwargs:

    Returns:

    """
    proxies_list = []
    with open(os.path.join(proxy_path, filename), "r") as f:
        lines = f.readlines()

    for line in lines:
        line = line.strip()
        if not line:
            continue
        # 解析
        auth = ""
        if "@" in line:
            auth, line = line.split("@")
        #
        items = line.split(":")
        if len(items) < 2:
            continue

        ip, port, *protocol = items
        if not all([port, ip]):
            continue
        if auth:
            ip = "{}@{}".format(auth, ip)
        if not protocol:
            proxies = {
                "https": "https://%s:%s" % (ip, port),
                "http": "http://%s:%s" % (ip, port),
            }
        else:
            proxies = {protocol[0]: "%s://%s:%s" % (protocol[0], ip, port)}
        proxies_list.append(proxies)

    return proxies_list


def get_proxy_from_redis(proxy_source_url, **kwargs):
    """
        从指定 redis 地址获取代理
            redis 存储结构 zset
                ip:port ts
    Args:
        proxy_source_url:
            redis://:passwd@host:ip/db

        **kwargs:
            redis_proxies_key: "xxx"

    Returns:
        [{'http':'http://xxx.xxx.xxx:xxx', 'https':'https://xxx.xxx.xxx.xxx:xxx'}]
    """

    redis_conn = redis.StrictRedis.from_url(proxy_source_url)
    key = kwargs.get("redis_proxies_key")
    assert key, "从redis中获取代理 需要指定 redis_proxies_key"
    proxies = redis_conn.zrange(key, 0, -1)
    proxies_list = []
    for proxy in proxies:
        proxy = proxy.decode()
        proxies_list.append(
            {"https": "https://%s" % proxy, "http": "http://%s" % proxy}
        )
    return proxies_list


def check_proxy(
    ip="",
    port="",
    proxies=None,
    type=0,
    timeout=5,
    logger=None,
    show_error_log=False,
    **kwargs,
):
    """
        代理有效性检查
    Args:
        ip:
        port:
        proxies:
        type:
        timeout:
        logger:
        show_error_log:
        **kwargs:

    Returns:

    """
    if not logger:
        logger = log.get_logger(__file__)
    ok = 0
    if type == 0 and ip and port:
        # socket检测成功 不代表代理一定可用 Connection closed by foreign host. 这种情况就不行
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sk:
            sk.settimeout(timeout)
            try:
                # 必须检测 否则代理永远不刷新
                sk.connect((ip, int(port)))
                ok = 1
            except Exception as e:
                if show_error_log:
                    logger.debug("check proxy failed: {} {}:{}".format(e, ip, port))
            sk.close()
    else:
        if not proxies:
            proxies = {
                "http": "http://{}:{}".format(ip, port),
                "https": "https://{}:{}".format(ip, port),
            }
        target_url = random.choice(
            [
                "http://www.baidu.com",
                "http://www.baidu.com",
                "http://www.baidu.com",
                # "http://httpbin.org/ip",
            ]
        )
        try:
            r = requests.get(
                target_url,
                headers={
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/76.0.3809.100 Safari/537.36"
                },
                proxies=proxies,
                timeout=timeout,
                stream=True,
            )
            ok = 1
            r.close()
        except Exception as e:
            if show_error_log:
                logger.debug(
                    "check proxy failed: {} {}:{} {}".format(e, ip, port, proxies)
                )
    return ok


class ProxyItem(object):
    """单个代理对象"""

    # 代理标记
    proxy_tag_list = (-1, 0, 1)

    def __init__(
        self,
        proxies=None,
        valid_timeout=20,
        check_interval=180,
        max_proxy_use_num=10000,
        delay=30,
        use_interval=None,
        logger=None,
        **kwargs,
    ):
        """
        Args:
            proxies:
            valid_timeout: 代理检测超时时间 默认-1    20181008  默认不再监测有效性
            check_interval:
            max_proxy_use_num:
            delay:
            use_interval: 使用间隔 单位秒 默认不限制
            logger: 日志处理器 默认 log.get_logger()
            **kwargs:
        """
        # {"http": ..., "https": ...}
        self.proxies = proxies
        # 检测超时时间 秒
        self.valid_timeout = valid_timeout
        # 检测间隔 秒
        self.check_interval = check_interval

        # 标记  0:正常 -1:丢弃  1: 待会再用 ...
        self._flag = 0
        # 上次状态变化时间
        self._flag_ts = 0
        # 上次更新时间 有效时间
        self.update_ts = 0
        # 最大被使用次数
        self.max_proxy_use_num = max_proxy_use_num
        # 被使用次数记录
        self.use_num = 0
        # 延迟使用时间
        self.delay = delay
        # 使用间隔 单位秒
        self.use_interval = use_interval
        # 使用时间
        self._use_ts = 0

        self.proxy_args = self.parse_proxies(self.proxies)
        self.proxy_ip = self.proxy_args["ip"]
        self.proxy_port = self.proxy_args["port"]
        self.proxy_ip_port = "{}:{}".format(self.proxy_ip, self.proxy_port)
        if self.proxy_args["user"]:
            self.proxy_id = "{user}:{password}@{ip}:{port}".format(**self.proxy_args)
        else:
            self.proxy_id = self.proxy_ip_port

        # 日志处理器
        self.logger = logger or log.get_logger(__file__)

    @property
    def use_ts(self):
        return self._use_ts

    @use_ts.setter
    def use_ts(self, value):
        self._use_ts = value

    @property
    def flag(self):
        return self._flag

    @flag.setter
    def flag(self, value):
        self._flag = value

    @property
    def flag_ts(self):
        return self._flag_ts

    @flag_ts.setter
    def flag_ts(self, value):
        self._flag_ts = value

    def get_proxies(self):
        self.use_num += 1
        return self.proxies

    def is_delay(self):
        return self.flag == 1

    def is_valid(self, force=0, type=0):
        """
        检测代理是否有效
            1 有效
            2 延时使用
            0 无效 直接在代理池删除
        Args:
            force:
            type:

        Returns:

        """
        if self.use_num > self.max_proxy_use_num > 0:
            self.logger.debug("代理达到最大使用次数: {} {}".format(self.use_num, self.proxies))
            return 0
        if self.flag == -1:
            self.logger.debug("代理被标记 -1 丢弃 %s" % self.proxies)
            return 0
        if self.delay > 0 and self.flag == 1:
            _delay = int(time.time() - self.flag_ts)
            if _delay < self.delay:
                self.logger.debug("代理被标记 1 延迟 {} {}".format(_delay, self.proxy_id))
                return 2
            else:
                self.flag = 0
                self.logger.debug("延迟代理释放: {}".format(self.proxies))
        if self.use_interval:
            if time.time() - self.use_ts < self.use_interval:
                return 2
        if not force:
            if time.time() - self.update_ts < self.check_interval:
                return 1
        if self.valid_timeout > 0:
            ok = check_proxy(
                proxies=self.proxies,
                type=type,
                timeout=self.valid_timeout,
                logger=self.logger,
            )
        else:
            ok = 1
        self.update_ts = time.time()
        return ok

    @classmethod
    def parse_proxies(self, proxies):
        """
            分解代理组成部分
        Args:
            proxies:

        Returns:

        """
        if not proxies:
            return {}
        if isinstance(proxies, (str, bytes)):
            proxies = json.loads(proxies)
        protocol = list(proxies.keys())
        if not protocol:
            return {}
        _url = proxies.get(protocol[0])
        if not _url.startswith("http"):
            _url = "http://" + _url
        _url_parse = parse.urlparse(_url)
        netloc = _url_parse.netloc
        if "@" in netloc:
            netloc_auth, netloc_host = netloc.split("@")
        else:
            netloc_auth, netloc_host = "", netloc
        ip, *port = netloc_host.split(":")
        port = port[0] if port else "80"
        user, *password = netloc_auth.split(":")
        password = password[0] if password else ""
        return {
            "protocol": protocol,
            "ip": ip,
            "port": port,
            "user": user,
            "password": password,
            "ip_port": "{}:{}".format(ip, port),
        }


class RedisProxyItem(ProxyItem):
    """单个代理对象"""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        #
        #
        self.redis_conn: redis.StrictRedis = kwargs["redis_conn"]
        self.key_namespace = kwargs["namespace"]
        # 使用时间记录
        self.use_time_key = f"{self.key_namespace}:proxy_use_time"
        self.flag_key = f"{self.key_namespace}:proxy_flag"
        self.flag_ts_key = f"{self.key_namespace}:proxy_flag_ts"

    @property
    def use_ts(self):
        r = self.redis_conn.hget(self.use_time_key, self.proxy_id)
        r = float(r) if r else 0
        return r

    @use_ts.setter
    def use_ts(self, value):
        value = float(value)
        self.redis_conn.hset(self.use_time_key, self.proxy_id, value)

    @property
    def flag(self):
        r = self.redis_conn.hget(self.flag_key, self.proxy_id)
        r = int(r) if r else 0
        return r

    @flag.setter
    def flag(self, value):
        value = int(value)
        self.redis_conn.hset(self.flag_key, self.proxy_id, value)

    @property
    def flag_ts(self):
        r = self.redis_conn.hget(self.flag_ts_key, self.proxy_id)
        r = float(r) if r else 0
        return r

    @flag_ts.setter
    def flag_ts(self, value):
        value = float(value)
        self.redis_conn.hset(self.flag_ts_key, self.proxy_id, value)


class ProxyPoolBase(object):
    def __init__(self, *args, **kwargs):
        pass

    def get(self, *args, **kwargs):
        raise NotImplementedError


class ProxyPool(ProxyPoolBase):
    """代理池"""

    def __init__(self, **kwargs):
        """
        Args:
            size: 代理池大小  -1 为不限制
            proxy_source_url: 代理文件地址 支持列表
            proxy_instance:  提供代理的实例
            reset_interval:  代理池重置间隔 最小间隔
            reset_interval_max:  代理池重置间隔 最大间隔 默认2分钟
            check_valid: 是否在获取代理时进行检测有效性
            local_proxy_file_cache_timeout: 本地缓存的代理文件超时时间
            logger: 日志处理器 默认 log.get_logger()
            **kwargs: 其他的参数
        """
        super().__init__(**kwargs)
        # 队列最大长度
        self.max_queue_size = kwargs.get("size", -1)
        # 实际代理数量
        self.real_max_proxy_count = 1000
        # 代理可用最大次数
        # 代理获取地址 http://localhost/proxy.txt
        self.proxy_source_url = kwargs.get("proxy_source_url", [])
        if not isinstance(self.proxy_source_url, list):
            self.proxy_source_url = [self.proxy_source_url]
            self.proxy_source_url = [x for x in self.proxy_source_url if x]
            self.proxy_source_url = list(set(self.proxy_source_url))
            kwargs.update({"proxy_source_url": self.proxy_source_url})
        # 处理日志
        self.logger = kwargs.get("logger") or log.get_logger(__file__)
        kwargs["logger"] = self.logger
        if not self.proxy_source_url:
            self.logger.warn("need set proxy_source_url or proxy_instance")

        # 代理池重置间隔
        self.reset_interval = kwargs.get("reset_interval", 5)
        # 强制重置一下代理 添加新的代理进来 防止一直使用旧的被封的代理
        self.reset_interval_max = kwargs.get("reset_interval_max", 180)
        # 是否监测代理有效性
        self.check_valid = kwargs.get("check_valid", True)

        # 代理队列
        self.proxy_queue = None
        # {代理id: ProxyItem, ...}
        self.proxy_dict = {}
        # 失效代理队列
        self.invalid_proxy_dict = {}
        #
        self.kwargs = kwargs

        # 临时缓存起来的代理 一般用于快速复用 会优先从此队列中获取 用完即弃 因此需要用户不断向里面添加
        self.proxy_temp_cache_list = deque()

        # 重置代理池锁
        self.reset_lock = None
        # 重置时间
        self.last_reset_time = 0
        # 重置的太快了  计数
        self.reset_fast_count = 0
        # 计数 获取代理重试3次仍然失败 次数
        self.no_valid_proxy_times = 0

        # 上次获取代理时间
        self.last_get_ts = time.time()

        # 记录ProxyItem的update_ts 防止由于重置太快导致重复检测有效性
        self.proxy_item_update_ts_dict = {}

        # 警告
        self.warn_flag = False

        # 是否使用redis介入
        self.redis_conn = kwargs.get("redis_conn", "")
        self.namespace = kwargs.get("namespace", "")
        if self.redis_conn:
            self.proxy_item_class = RedisProxyItem
            self.kwargs["redis_conn"] = self.redis_conn
            self.kwargs["namespace"] = self.namespace
            # TODO 清理
        else:
            self.proxy_item_class = ProxyItem

    def warn(self):
        if not self.warn_flag:
            for url in self.proxy_source_url:
                if "zhima" in url:
                    continue
            self.warn_flag = True
        return

    @property
    def queue_size(self):
        """
            当前代理池中代理数量
        Returns:

        """
        return self.proxy_queue.qsize() if self.proxy_queue is not None else 0

    def clear(self):
        """
            清空自己
        Returns:

        """
        self.proxy_queue = None
        # {代理ip: ProxyItem, ...}
        self.proxy_dict = {}
        # 清理失效代理集合
        _limit = datetime.datetime.now() - datetime.timedelta(minutes=10)
        self.invalid_proxy_dict = {
            k: v for k, v in self.invalid_proxy_dict.items() if v > _limit
        }
        # 清理超时的update_ts记录
        _limit = time.time() - 600
        self.proxy_item_update_ts_dict = {
            k: v for k, v in self.proxy_item_update_ts_dict.items() if v > _limit
        }
        return

    def get(self, retry: int = 0) -> dict:
        """
            从代理池中获取代理
        Args:
            retry:

        Returns:

        """
        retry += 1
        if retry > 3:
            self.no_valid_proxy_times += 1
            return None
        # 获取临时代理
        try:
            return self.proxy_temp_cache_list.pop()
        except:
            pass
        #
        if time.time() - self.last_get_ts > 3 * 60:
            # 3分钟没有获取过 重置一下
            try:
                self.reset_proxy_pool()
            except Exception as e:
                self.logger.exception(e)
        # 记录获取时间
        self.last_get_ts = time.time()
        #
        self.warn()
        proxy_item = self.get_random_proxy()
        if proxy_item:
            # 不检测
            if not self.check_valid:
                # 塞回去
                proxies = proxy_item.get_proxies()
                self.put_proxy_item(proxy_item)
                return proxies
            else:
                is_valid = proxy_item.is_valid()
                if is_valid:
                    # 记录update_ts
                    self.proxy_item_update_ts_dict[
                        proxy_item.proxy_id
                    ] = proxy_item.update_ts
                    # 塞回去
                    proxies = proxy_item.get_proxies()
                    self.put_proxy_item(proxy_item)
                    if is_valid == 1:
                        if proxy_item.use_interval:
                            proxy_item.use_ts = time.time()
                        return proxies
                else:
                    # 处理失效代理
                    self.proxy_dict.pop(proxy_item.proxy_id, "")
                    self.invalid_proxy_dict[
                        proxy_item.proxy_id
                    ] = datetime.datetime.now()
        else:
            try:
                self.reset_proxy_pool()
            except Exception as e:
                self.logger.exception(e)
        if self.no_valid_proxy_times >= 5:
            # 解决bug: 当爬虫仅剩一个任务时 由于只有一个线程检测代理 而不可用代理又刚好很多（时间越长越多） 可能出现一直获取不到代理的情况
            # 导致爬虫烂尾
            try:
                self.reset_proxy_pool()
            except Exception as e:
                self.logger.exception(e)
        return self.get(retry)

    get_proxy = get

    def get_random_proxy(self) -> ProxyItem:
        """
            随机获取代理
        Returns:

        """
        if self.proxy_queue is not None:
            if random.random() < 0.5:
                # 一半概率检查 这是个高频操作 优化一下
                if time.time() - self.last_reset_time > self.reset_interval_max:
                    self.reset_proxy_pool(force=True)
                else:
                    min_q_size = (
                        min(self.max_queue_size / 2, self.real_max_proxy_count / 2)
                        if self.max_queue_size > 0
                        else self.real_max_proxy_count / 2
                    )
                    if self.proxy_queue.qsize() < min_q_size:
                        self.reset_proxy_pool()
            try:
                return self.proxy_queue.get_nowait()
            except Exception:
                pass
        return None

    def append_proxies(self, proxies_list: list) -> int:
        """
            添加代理到代理池
        Args:
            proxies_list:

        Returns:

        """
        count = 0
        if not isinstance(proxies_list, list):
            proxies_list = [proxies_list]
        for proxies in proxies_list:
            if proxies:
                proxy_item = self.proxy_item_class(proxies=proxies, **self.kwargs)
                # 增加失效判断 2018/12/18
                if proxy_item.proxy_id in self.invalid_proxy_dict:
                    continue
                if proxy_item.proxy_id not in self.proxy_dict:
                    # 补充update_ts
                    if not proxy_item.update_ts:
                        proxy_item.update_ts = self.proxy_item_update_ts_dict.get(
                            proxy_item.proxy_id, 0
                        )
                    self.put_proxy_item(proxy_item)
                    self.proxy_dict[proxy_item.proxy_id] = proxy_item
                    count += 1
        return count

    def put_proxy_item(self, proxy_item: ProxyItem):
        """
            添加 ProxyItem 到代理池
        Args:
            proxy_item:

        Returns:

        """
        return self.proxy_queue.put_nowait(proxy_item)

    def reset_proxy_pool(self, force: bool = False):
        """
            重置代理池
        Args:
            force: 是否强制重置代理池

        Returns:

        """
        if not self.reset_lock:
            # 必须用时调用 否则 可能存在 gevent patch前 threading就已经被导入 导致的Rlock patch失效
            import threading

            self.reset_lock = threading.RLock()
        with self.reset_lock:
            if (
                force
                or self.proxy_queue is None
                or (
                    self.max_queue_size > 0
                    and self.proxy_queue.qsize() < self.max_queue_size / 2
                )
                or (
                    self.max_queue_size < 0
                    and self.proxy_queue.qsize() < self.real_max_proxy_count / 2
                )
                or self.no_valid_proxy_times >= 5
            ):
                if time.time() - self.last_reset_time < self.reset_interval:
                    self.reset_fast_count += 1
                    if self.reset_fast_count % 10 == 0:
                        self.logger.debug(
                            "代理池重置的太快了:) {}".format(self.reset_fast_count)
                        )
                        time.sleep(1)
                else:
                    self.clear()
                    if self.proxy_queue is None:
                        import queue

                        self.proxy_queue = queue.Queue()
                    # TODO 这里获取到的可能重复
                    proxies_list = get_proxy_from_url(**self.kwargs)
                    self.real_max_proxy_count = len(proxies_list)
                    if 0 < self.max_queue_size < self.real_max_proxy_count:
                        proxies_list = random.sample(proxies_list, self.max_queue_size)
                    _valid_count = self.append_proxies(proxies_list)
                    self.last_reset_time = time.time()
                    self.no_valid_proxy_times = 0
                    self.logger.debug(
                        "重置代理池成功: 获取{}, 成功添加{}, 失效{},  当前代理数{},".format(
                            len(proxies_list),
                            _valid_count,
                            len(self.invalid_proxy_dict),
                            len(self.proxy_dict),
                        )
                    )
        return

    def tag_proxy(
        self, proxies_list: Union[List, Dict], flag: int, *, delay=30
    ) -> bool:
        """
            对代理进行标记
        Args:
            proxies_list:
            flag:
                -1  废弃
                1 延迟使用
            delay:

        Returns:

        """
        if int(flag) not in self.proxy_item_class.proxy_tag_list or not proxies_list:
            return False
        if not isinstance(proxies_list, list):
            proxies_list = [proxies_list]
        for proxies in proxies_list:
            if not proxies:
                continue
            proxy_id = self.proxy_item_class(proxies, **self.kwargs).proxy_id
            if proxy_id not in self.proxy_dict:
                continue
            self.proxy_dict[proxy_id].flag = flag
            self.proxy_dict[proxy_id].flag_ts = time.time()
            self.proxy_dict[proxy_id].delay = delay
            if flag == -1:
                # 处理失效代理
                self.proxy_dict.pop(proxy_id, "")
                self.invalid_proxy_dict[proxy_id] = datetime.datetime.now()

        return True

    def get_proxy_item(self, proxy_id="", proxies=None):
        """
        获取代理对象
        Args:
            proxy_id:
            proxies:

        Returns:

        """
        if proxy_id:
            return self.proxy_dict.get(proxy_id)
        if proxies:
            proxy_id = self.proxy_item_class(proxies, **self.kwargs).proxy_id
            return self.proxy_dict.get(proxy_id)
        return

    def copy(self):
        return ProxyPool(**self.kwargs)

    def all(self) -> list:
        """
            获取当前代理池中的全部代理
        Returns:

        """
        return get_proxy_from_url(**self.kwargs)

    def add_temp_proxy(self, proxies=None):
        """
        添加临时代理
        Args:
            proxies:

        Returns:

        """
        if not proxies:
            return
        if not isinstance(proxies, list):
            proxies = [proxies]
        self.proxy_temp_cache_list.extend(proxies)
        return True


# 代理隧道
class ProxyTunnelPool(ProxyPool):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        # 特殊处理
        proxies = self.get()
        proxy_obj = ProxyItem(proxies=proxies, **self.kwargs)
        proxy_ip_port = ProxyItem.parse_proxies(proxies)["ip_port"]
        self.proxy_queue.append(proxy_obj)
        self.proxy_dict[proxy_ip_port] = proxy_obj

    def get(self, retry=0):
        raise NotImplementedError


if __name__ == "__main__":
    pass
