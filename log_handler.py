import logging


class MemoryAndScreenHandler(logging.StreamHandler):
    def __init__(self, stream=None):
        super().__init__(stream)
        self.logs = []

    def emit(self, record):
        # 首先将日志记录添加到内存列表中
        self.logs.append(self.format(record))

        # 然后调用父类的emit方法将日志输出到屏幕
        super().emit(record)


def get_logger(name):
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    handler = MemoryAndScreenHandler()

    formatter = logging.Formatter('%(asctime)s %(levelname)s: %(message)s')
    handler.setFormatter(formatter)

    logger.addHandler(handler)

    return logger
