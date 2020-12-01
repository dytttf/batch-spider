# coding:utf8
import logging
import os
import sys
import platform
import tempfile
import logging.handlers
from better_exceptions import format_exception


default_log_format = (
    "[%(asctime)s] [%(levelname)s] [%(filename)s] [%(lineno)d] - %(message)s"
)


def detect_default_log_dir():
    """
        *unix: /var/log/batch_spider_log/
        mac: /tmp/batch_spider_log
        windows: tempfile.gettempdir()

    Returns:

    """
    system = platform.system()
    if system == "Windows":
        log_dir = tempfile.gettempdir()
    elif system == "Darwin":
        log_dir = "/tmp/"
    else:
        log_dir = "/var/log/"

    log_dir = os.path.join(log_dir, "batch_spider_log")
    os.makedirs(log_dir, exist_ok=True)
    return log_dir


def get_logger(
    name, log_level: str = "DEBUG", to_local: bool = False, local_log_dir: str = None
):
    """

    Args:
        name:
        log_level: 日志等级
        to_local: 是否保存到本地
        local_log_dir: 自定义日志目录

    Returns:

    """
    name = name.split(os.sep)[-1].split(".")[0]  # 取文件名

    _logger = logging.getLogger(name)
    _logger.setLevel(log_level)

    # 定制标准输出日志格式
    formatter = logging.Formatter(default_log_format)
    formatter.formatException = lambda exc_info: format_exception(*exc_info)

    stream_handler = logging.StreamHandler(stream=sys.stdout)
    stream_handler.setFormatter(formatter)
    # 检查是否已存在重复的handler
    handle_exists = 0
    for _handler in _logger.handlers:
        if (
            isinstance(_handler, logging.StreamHandler)
            and _handler.stream == sys.stdout
        ):
            handle_exists = 1
    if not handle_exists:
        _logger.addHandler(stream_handler)

    #
    if to_local:
        local_log_dir = local_log_dir or detect_default_log_dir()
        rotating_file_handler = logging.handlers.RotatingFileHandler(
            os.path.join(local_log_dir, f"{name}.log"),
            maxBytes=100 * 1024 * 1024,
            backupCount=10,
            encoding="utf-8",
        )
        rotating_file_handler.setFormatter(formatter)
        _logger.addHandler(rotating_file_handler)

    return _logger


if __name__ == "__main__":
    logger = get_logger("xxx", to_local=True)
    logger.debug("test")

    try:
        a = 1
        b = 0
        c = a / b
    except Exception as e:
        logger.exception(e)
