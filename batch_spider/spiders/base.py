# coding:utf8
"""

isort:skip_file
"""
# 貌似这里有内存泄漏问题
from gevent import monkey  # isort:skip

# tips  注意 如果 patch了subprocess 会导致 os.popen特别慢...
monkey.patch_all(os=False, subprocess=False, signal=False)  # isort:skip

import warnings  # isort:skip

warnings.filterwarnings("ignore")  # isort:skip

#
import threading
import time
from queue import Empty, Queue
from typing import Callable, List, Optional, Tuple, Union, Dict

from batch_spider import util
from batch_spider.network import downloader
from batch_spider.spiders import Request, Response
from batch_spider.utils import log

logger = log.get_logger(__file__)


class Spider(object):
    def __init__(self, **kwargs):
        super().__init__()
        # 类名属性
        try:
            self.name = self.__class__.__name__
        except:
            self.name = ""
        # 下载器
        self.downloader = kwargs.get("downloader", downloader.Downloader())
        # mysql入库
        self.db = None
        # oss 入库
        self.oss_db = None
        # 线程池
        self.pool_size = kwargs.get("pool_size", 100)
        self.event_exit = threading.Event()
        self.request_queue = Queue()

        # 线程状态记录 是否正在运行中
        self._thread_status = {}
        # request重试次数限制
        self.max_request_retrys = 9999

        # 内存使用上限 比例 默认0.9 超过0.8则主动被kill
        self.memory_utilization_limit = 0.8
        self._killed = False
        self._last_check_memory_utilization_ts = 0

        # break_spider
        self.break_spider_check_interval = 5
        self._last_check_break_spider_ts = 0
        #

        # 注意回调函数仅在 run方法里运行
        # 在start前执行的一系列函数
        self._before_start_callbacks = [self.before_start]
        # 在close前执行的一系列函数
        self._before_stop_callbacks = [self.before_stop]

        self._closed = False
        #
        self._close_reason = ""

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            if not self._closed:
                self._close()
                self._closed = True
        except:
            pass

    def _close(self, **kwargs):
        # 调用自定义的 close
        try:
            self.close()
        except Exception as e:
            logger.exception(e)
        # 关闭默认的一些连接
        for _instince in [self.db, self.oss_db, self.downloader]:
            if _instince:
                try:
                    _instince.close()
                except Exception as e:
                    logger.exception(e)
        return

    def before_start(self, **kwargs):
        """爬虫开始前调用 处理可能需要的一些事情 用户自定义"""
        pass

    def before_stop(self, **kwargs):
        """爬虫结束前调用 处理可能需要的一些事情 用户自定义"""
        pass

    def break_spider(self, **kwargs) -> Optional[int]:
        """
            爬虫中断函数 在迭代start_requests函数过程中调用
        Returns:
            1 停止迭代 start_requests
            other 忽略
        """
        pass

    def _break_spider(self) -> Optional[int]:
        """
            减少调用break_spider的次数 默认10秒一次
        Returns:

        """
        if self.break_spider_check_interval <= 0:
            return self.break_spider()
        if (
            time.time() - self._last_check_break_spider_ts
        ) > self.break_spider_check_interval:
            self._last_check_break_spider_ts = time.time()
            return self.break_spider()
        return 0

    def close(self, **kwargs):
        pass

    @property
    def container_memory_utilization(self):
        """
            获取容器内存使用百分比
        Returns:

        """
        try:
            memory_info = util.ContainerInfo.memory_info()
        except Exception as e:
            if "memory" in str(e):
                logger.debug("系统内存不足 获取信息失败")
                return 1
            raise e
        if not memory_info:
            return 0
        return memory_info["container"]["memory_utilization"]

    def download(
        self,
        *args,
        downloader: downloader.Downloader = None,
        request_obj: Request = None,
        **kwargs
    ):
        """
            下载函数接口
        Args:
            *args:
            downloader: 指定下载器
            request_obj:
            **kwargs:

        Returns:

        """
        if not downloader:
            downloader = self.downloader
        return downloader.download(*args, **kwargs)

    def handle_request(self, thread_num: int):
        while 1:
            try:
                request_obj = self.request_queue.get(True, 1)
                if not request_obj:
                    continue
                # 重试次数限制
                if request_obj.retry > self.max_request_retrys:
                    put_retry = getattr(self.request_queue, "put_retry", None)
                    if put_retry:
                        # 重试置为0 然后丢入重试队列  只对RedisQueue有效
                        request_obj.retry = 0
                        put_retry(request_obj)
                    # 重试次数超出限制 丢弃任务
                    logger.warn(
                        "retry times over limit {}, task delete: {}".format(
                            self.max_request_retrys, request_obj
                        )
                    )
                    self.request_queue.task_done()
                    continue
            except Exception as e:
                if not isinstance(e, Empty):
                    logger.exception(e)
                if self.event_exit.is_set():
                    break
                self._thread_status[thread_num] = 0
                continue
            self._thread_status[thread_num] = 1
            if not isinstance(request_obj, Request):
                # todo 处理
                continue

            _request = request_obj.request
            if _request:
                try:
                    _response = self.download(
                        _request,
                        downloader=request_obj.downloader,
                        request_obj=request_obj,
                    )
                except Exception as e:
                    logger.exception(e)
                    _response = None
            else:
                _response = None
            # 记录下载次数
            request_obj.retry += 1

            # response
            if isinstance(_response, tuple):
                response = Response(_response[0], request_obj, exception=_response[1])
            elif isinstance(_response, Response):
                response = _response
            else:
                response = Response(_response, request_obj)

            try:
                _callback = request_obj.callback
                if not _callback:
                    _callback = self.parse
                if isinstance(_callback, (str, bytes)):
                    _callback = getattr(self, _callback)
                result = _callback(response)
                if result is not None:
                    # 迭代
                    for item in result:
                        if isinstance(item, Request):
                            self.request_queue.put(item)
                        # todo  其他类型
                        pass
            except Exception as e:
                logger.exception(e)
            self.request_queue.task_done()
        return

    def make_request(self, *args, **kwargs) -> Optional[Request]:
        """
            定义如何生成request
        Args:
            *args:
            **kwargs:

        Returns:

        """
        pass

    def register_before_start(self, function: Callable):
        """
            注册爬虫启动前回调函数 可注册多个 顺序调用
        Args:
            function:

        Returns:

        """
        if not callable(function):
            raise TypeError("must be callable: {}".format(function))
        self._before_start_callbacks.append(function)
        logger.debug("注册启动前回调函数成功: {}".format(function))
        return True

    def register_before_stop(self, function: Callable):
        """
            注册爬虫结束前回调函数 可注册多个 顺序调用
        Args:
            function:

        Returns:

        """
        if not callable(function):
            raise TypeError("must be callable: {}".format(function))
        self._before_stop_callbacks.append(function)
        logger.debug("注册结束前回调函数成功: {}".format(function))
        return True

    def run(self, **kwargs):
        # 开启处理线程池
        thread_list = []
        for i in range(self.pool_size):
            t = threading.Thread(target=self.handle_request, args=(i,))
            t.start()
            thread_list.append(t)

        max_queue_size = min(100, self.pool_size)

        logger.debug("Spider start")
        # 执行自定义的call_back
        for _callback in self._before_start_callbacks:
            try:
                _callback_name = getattr(_callback, "__name__", _callback)
                logger.debug("开始执行爬虫启动前回调函数: {}".format(_callback_name))
                _callback()
                logger.debug("回调函数 {} 执行成功".format(_callback_name))
            except Exception as e:
                logger.exception(e)

        # 记录start_requests中发生的异常
        raise_exception = None
        # 是否已经break_spider
        spider_break = 0
        # 此处若不捕获异常 可能卡死爬虫
        try:
            # 减少日志量
            _last_show_qsize_ts = 0
            if self.break_spider() != 1 and not self._killed:
                for item in self.start_requests():
                    if isinstance(item, Request):
                        self.request_queue.put(item)
                    else:
                        if item is None:
                            logger.warning("Got a None Request")
                            continue
                        # todo
                        logger.error("Not a Request Object: {}".format(item))
                    # 检查是否需要中断
                    if self._break_spider() == 1 or self._killed:
                        spider_break = 1
                        if not self._close_reason:
                            self._close_reason = "Break Spider"
                        break
                    # 检查队列长度
                    for i in range(1, 1000):
                        qsize = self.request_queue.qsize()
                        if qsize < max_queue_size:
                            break
                        _max_wait_ts = 3
                        # 动态调整等待时间
                        _wait_ts = min(0.1 * i, _max_wait_ts)
                        time.sleep(_wait_ts)
                        if _wait_ts == _max_wait_ts:
                            # 检查是否需要中断
                            if self.break_spider() == 1 or self._killed:
                                spider_break = 1
                                break
                        # 固定10秒打一次
                        _t = time.time()
                        if _t - _last_show_qsize_ts > 10:
                            logger.debug("wait Request count: {} ...".format(qsize))
                            _last_show_qsize_ts = _t
                    if self.should_oom_killed() == 1:
                        logger.debug("内存使用量即将达到最大值")
                        spider_break = 1
                        self.suicide()
                        break
            else:
                logger.debug("break spider")
        except Exception as e:
            raise_exception = e
        while 1:
            if self.request_queue.qsize() <= 0:
                # 当所有线程都无任务时 停止爬虫
                if sum(self._thread_status.values()) == 0:
                    self.event_exit.set()
                    break
            if spider_break:
                logger.debug(
                    "爬虫主线程已被终止...  等待子线程结束中... 剩余任务: {} 活动线程数: {} 终止原因: {}".format(
                        self.request_queue.qsize(),
                        sum(self._thread_status.values()),
                        self._close_reason,
                    )
                )
            time.sleep(5)

        # 2018/06/08 上边这几行代码 可以代替join  应该是这样的  出了问题再说啊
        for t in thread_list:
            t.join()
        logger.debug("子线程关闭成功")

        # 执行自定义的call_back
        for _callback in self._before_stop_callbacks:
            try:
                _callback_name = getattr(_callback, "__name__", _callback)
                logger.debug("开始执行结束前回调函数: {}".format(_callback_name))
                _callback()
                logger.debug("回调函数 {} 执行成功".format(_callback_name))
            except Exception as e:
                logger.exception(e)

        # 关闭各种连接
        try:
            self._close()
            self._closed = True
            logger.debug("爬虫回调函数 close 执行成功")
        except Exception as e:
            logger.exception(e)

        # 爬虫结束后 抛出记录的异常
        if raise_exception:
            raise raise_exception
        if self._killed:
            logger.debug("Spider done: Killed")
            util.oom_killed_exit()
        logger.debug("Spider done: {}".format(self._close_reason))
        return

    def should_oom_killed(self):
        """
            当内存超限时是否主动kill
        Returns:

        """
        # todo 优化执行次数
        if time.time() > self._last_check_memory_utilization_ts:
            if self.container_memory_utilization > self.memory_utilization_limit:
                return 1
            else:
                self._last_check_memory_utilization_ts = time.time() + 60
        return 0

    def suicide(self):
        """
            自杀
        Returns:

        """
        logger.debug("please kill me ......")
        self._killed = True
        self._close_reason = "Killed(suicide)"
        return

    def store_data(
        self,
        data: Union[List, Dict],
        *,
        table_name: str = "",
        mysql: bool = True,
        mysql_db=None,
        **kwargs
    ):
        """
        数据存储
        Args:
            data: 待保存数据 list or dict
            table_name:
            mysql: 是否保存到mysql
            mysql_db: 是否使用指定的mysql入库连接  可能存在入库mysql与self.db不一致的情况
            **kwargs:

        Returns:

        """
        resp = None
        # mysql入库
        if mysql:
            mysql_db = mysql_db or self.db
            if mysql_db:
                if not isinstance(data, list):
                    resp = mysql_db.add(data, table_name=table_name, **kwargs)
                else:
                    resp = mysql_db.add_many(data, table_name=table_name, **kwargs)
            else:
                raise ValueError("mysql入库失败: mysql_db is None")
        return resp

    def start_requests(self):
        """
        example:
            def start_requests(self):
                while 1:
                    yiele Request("http://www.baidu.com")
        Returns:

        """
        raise NotImplementedError

    def parse(self, response: Response):
        """

        Args:
            response:

        Returns:

        """
        raise NotImplementedError


if __name__ == "__main__":
    spider = Spider()
    spider.run()
