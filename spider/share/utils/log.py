# coding:utf8
import logging
import os
import sys
from better_exceptions import format_exception


def get_logger(name, log_level="DEBUG"):
    """

    Args:
        name:
        log_level:

    Returns:

    """
    name = name.split(os.sep)[-1].split(".")[0]  # 取文件名

    _logger = logging.getLogger(name)
    _logger.setLevel(log_level)

    # 定制标准输出日志格式
    formatter = logging.Formatter(
        "[%(asctime)s] [%(levelname)s] [%(filename)s] [%(lineno)d] - %(message)s"
    )
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
    return _logger


if __name__ == "__main__":
    logger = get_logger(__file__)
    logger.debug("test")

    try:
        a = 1
        b = 0
        c = a / b
    except Exception as e:
        logger.exception(e)
