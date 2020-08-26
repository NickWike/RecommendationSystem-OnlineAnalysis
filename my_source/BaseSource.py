from pretreatment import ETL
from namespace import FieldsName


class BaseLogAnalysis(object):
    def __init__(self):
        self.query_task = None
        self.spark_session = None
        self.source_lines = None
        self.final_df = None
        self.sink_batch = None

    def _split_main_lines(self):
        # 将流数据行拆分多个字段列,日志数据行的格式如下:
        self.main_fields = ETL.split_line_to_columns(self.source_lines)
        # 对上面的RequestInfo 字段做进一步分割
        self.request_info_fields = ETL.split_request_info_to_columns(self.main_fields.get(FieldsName.REQUEST_INFO))
        # 对response_info 做分割转换
        self.response_info_fields = ETL.split_response_info_to_columns(self.main_fields.get(FieldsName.RESPONSE_INFO))

    def __init_final_df(self):
        pass

    def start(self):
        pass

    def stop(self):
        if self.query_task is not None:
            self.query_task.stop()
            print("Query task exited")
        if self.spark_session is not None:
            self.spark_session.stop()
            print("Spark session task exited")
        if self.sink_batch is not None:
            self.sink_batch.stop()
            print("Sink batch task exited")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()

