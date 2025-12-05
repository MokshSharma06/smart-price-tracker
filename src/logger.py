class Log4j(object):
    def __init__(self, spark, logger_name: str = "smart-price-tracker"):
        log4j = spark._jvm.org.apache.log4j
        self.logger = log4j.LogManager.getLogger(logger_name)
        self.logger_name = logger_name

    def warn(self, message):
        self.logger.warn(f"[{self.logger_name}] {message}")

    def info(self, message):
        self.logger.info(f"[{self.logger_name}] {message}")

    def error(self, message):
        self.logger.error(f"[{self.logger_name}] {message}")

    def debug(self, message):
        self.logger.debug(f"[{self.logger_name}] {message}")


def get_logger(spark, logger_name: str):
    return Log4j(spark, logger_name)
