# coding:utf8
"""
工具库
    仅逻辑相关 不依赖任务具体实例 比如mysql、redis等
"""
import hashlib
import re
import tempfile
import time

import redis

from batch_spider.share.utils import log

IS_REDIS_3 = redis.__version__ > "3"

#
temp_dir = tempfile.gettempdir()

# redis锁使用的redis连接池
global_redis_lock_connection_pool_cache = {}


def delta_month(date, months: int, operate=""):
    """
    月份加减 不保证天数正确
    Args:
        date: datetime.date or datetime.datetime
        months: 月数
        operate: 操作  "+" or "-"

    Returns:

    """
    if not operate:
        raise ValueError("must special operate: {}".format(operate))
    if months < 0:
        raise ValueError("months must a positive number: {}".format(months))
    if months == 0:
        return date
    if operate == "-":
        _month_delta = date.month - months
        if _month_delta < 1:
            date = date.replace(
                year=date.year - int(abs(_month_delta) // 12 + 1),
                month=12 - abs(_month_delta) % 12,
            )
        else:
            date = date.replace(month=date.month - months)
    elif operate == "+":
        _month_delta = date.month + months
        if _month_delta > 12:
            date = date.replace(
                year=date.year + int((_month_delta - 1) // 12),
                month=(_month_delta - 1) % 12 + 1,
            )
        else:
            date = date.replace(month=date.month + months)
    return date


def get_proxies_by_host(host, port):
    proxy_id = "{}:{}".format(host, port)
    return get_proxies_by_id(proxy_id)


def get_proxies_by_id(proxy_id):
    proxies = {
        "http": "http://{}".format(proxy_id),
        "https": "https://{}".format(proxy_id),
    }
    return proxies


def key2num(key: str, number_range: list = [0, 1, 2, 3]) -> int:
    """
        将key 字符串 转换为一个确定的数字
            用于分组 尽量平均
    Args:
        key:
        number_range: 数字可选范围由 number_range 确定

    Returns:

    """
    if not number_range:
        raise ValueError("number_range is empty")
    if not isinstance(key, bytes):
        key = key.encode()
    hex_key = hashlib.md5(key).hexdigest()
    _sum = sum([ord(x) for x in hex_key])
    return number_range[_sum % len(number_range)]


def remove_control_characters(text: str):
    """
        移除控制字符
            全部字符列表
                https://zh.wikipedia.org/wiki/%E6%8E%A7%E5%88%B6%E5%AD%97%E7%AC%A6
            忽略其中的 10进制
                9 \t
                10 \n
                13 \r
    Args:
        text:
    Returns:

    """
    text = re.sub("[\x00-\x08\x0B\x0C\x0E-\x1F\x7F-\x9F]", "", text)
    return text


class RedisLock(object):
    def __init__(
        self,
        key,
        timeout=300,
        wait_timeout=8 * 3600,
        break_wait=None,
        redis_uri=None,
        connection_pool=None,
        auto_release=True,
        logger=None,
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
            logger:
        """
        self.redis_index = -1
        if not key:
            raise Exception("lock key is empty")
        if connection_pool:
            self.redis_conn = redis.StrictRedis(connection_pool=connection_pool)
        else:
            self.redis_conn = self.get_redis_conn(redis_uri)

        self.logger = logger or log.get_logger(__file__)

        self.lock_key = "redis_lock:{}".format(key)
        # 锁超时时间
        self.timeout = timeout
        # 等待加锁时间
        self.wait_timeout = wait_timeout
        # wait中断函数
        self.break_wait = break_wait
        if self.break_wait is None:
            self.break_wait = lambda: False
        if not callable(self.break_wait):
            raise TypeError(
                "break_wait must be function or None, but: {}".format(
                    type(self.break_wait)
                )
            )

        self.locked = False
        self.auto_release = auto_release

    def __enter__(self):
        if not self.locked:
            self.acquire()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.auto_release:
            self.release()

    def __repr__(self):
        return "<RedisLock: {} index: {}>".format(self.lock_key, self.redis_index)

    def get_redis_conn(self, redis_uri):
        if redis_uri not in global_redis_lock_connection_pool_cache:
            connection_pool = redis.BlockingConnectionPool.from_url(
                redis_uri, max_connections=100, timeout=60
            )
            global_redis_lock_connection_pool_cache[redis_uri] = connection_pool
        return redis.StrictRedis(
            connection_pool=global_redis_lock_connection_pool_cache[redis_uri]
        )

    def acquire(self):
        start = time.time()
        self.logger.debug("准备获取锁{} ...".format(self))
        while 1:
            # 尝试加锁
            if self.redis_conn.setnx(self.lock_key, time.time()):
                self.redis_conn.expire(self.lock_key, self.timeout)
                self.locked = True
                self.logger.debug("加锁成功: {}".format(self))
                break
            else:
                # 修复bug： 当加锁时被干掉 导致没有设置expire成功 锁无限存在
                _ttl = self.redis_conn.ttl(self.lock_key)
                if _ttl < 0:
                    self.redis_conn.delete(self.lock_key)
                elif _ttl > self.timeout:
                    self.redis_conn.expire(self.lock_key, self.timeout)

            if self.wait_timeout > 0:
                if time.time() - start > self.wait_timeout:
                    break
            else:
                # 不等待
                break
            if self.break_wait():
                self.logger.debug("break_wait 生效 不再等待加锁")
                break
            self.logger.debug("等待加锁: {} wait:{}".format(self, time.time() - start))
            if self.wait_timeout > 10:
                time.sleep(5)
            else:
                time.sleep(1)
        if not self.locked:
            self.logger.debug("加锁失败: {}".format(self))
        return

    def release(self, force=False):
        """
            释放锁
        Args:
            force: 是否强制释放
                    主要使用场景为某个锁超时设置失败导致无限锁

        Returns:

        """
        if self.locked or force:
            self.redis_conn.delete(self.lock_key)
            self.locked = False
        return

    def prolong_life(self, life_time: int) -> int:
        """
            延长这个锁的超时时间
        Args:
            life_time: 延长时间

        Returns:

        """
        expire = self.redis_conn.ttl(self.lock_key)
        if expire < 0:
            return expire
        expire += life_time
        self.redis_conn.expire(self.lock_key, expire)
        return self.redis_conn.ttl(self.lock_key)

    @property
    def ttl(self):
        """
            获取当前锁剩余生命长度
        Returns:

        """
        expire = self.redis_conn.ttl(self.lock_key)
        return expire


if __name__ == "__main__":
    assert remove_control_characters("a\nasdf\x82") == "a\nasdf"
