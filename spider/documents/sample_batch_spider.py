# coding:utf8
from spider.spiders import BatchSpider, Request, Response
from spider.utils import log

logger = log.get_logger(__file__)


class MySpider(BatchSpider):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        # 以下会保留与父类重复的属性定义 方便复制粘贴

        # 必填字段
        self.task_key = "task:temp"  # 需修改
        # 取任务表 必填
        self.task_table_name = "test_task_table"
        # 任务批次记录表 必填
        self.task_batch_table_name = "test_batch_record"

        # 非必填 但一般应该是需要设置的
        # 任务字段列表 建的表中必须有id字段 用于修改状态
        self.task_field_list = ["url"]
        # 批次间隔
        self.batch_interval = 7
        # 任务标识 用于日志 或 报警中的任务名字
        self.task_tag_name = "test"
        # 消息通知人
        self.message_recipients = ["xxx"]

    def start_requests(self):
        while 1:
            task_obj = self.get_task(obj=True)
            if not task_obj:
                logger.debug("没有任务")
                break
            url = task_obj.url
            # 下载
            req = Request({"url": url}, meta={"task": task_obj}, callback=self.parse)
            yield req
        return

    def parse(self, response: Response):
        request = response.request
        task_obj = request.meta["task"]
        _response = response.response
        try:
            batch_date = self.batch_date
            if _response:
                url = _response.url
                # todo 解析代码
                # 更新完成标志
                self.set_task_state(state=1, condition={"url": url})
            else:
                if _response is not None:
                    if _response.status_code == 404:
                        url = _response.url
                        # todo 解析代码
                        # 更新完成标志
                        self.set_task_state(state=-1, condition={"url": url})
                        return
                raise Exception
        except Exception as e:
            logger.exception(e)
            self.put_task(task_obj)
        return
