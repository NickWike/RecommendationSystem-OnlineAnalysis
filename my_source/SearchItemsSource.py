from pyspark.sql import SparkSession
from namespace import FieldsName, SchemaName
from my_sink.KafkaSink import SearchItemsBatch
from pretreatment import ETL
from conf.SparkConf import SearchItemsConf
import threading


class BrowsingTimeAnalysis(object):

    def __init__(self):
        # 构造Session对象
        self.spark_session = SparkSession \
            .builder \
            .master(SearchItemsConf.MASTER) \
            .appName(SearchItemsConf.APP_NAME) \
            .getOrCreate()
        # 从指定源中获取流数据
        self.lines = self.spark_session \
            .readStream \
            .format("socket") \
            .option("host", "127.0.0.1") \
            .option("port", 9998) \
            .load()
        self.final_df = None
        self.query_task = None
        self.__init_final_df()

    def __init_final_df(self):
        # 将流数据行拆分多个字段列,日志数据行的格式如下:
        main_fields = ETL.split_line_to_columns(self.lines)
        # 对上面的RequestInfo 字段做进一步分割
        request_info_fields = ETL.split_request_info_to_columns(main_fields.get(FieldsName.REQUEST_INFO))
        # 对response_info 做分割转换
        response_info_fields = ETL.split_response_info_to_columns(main_fields.get(FieldsName.RESPONSE_INFO))
        # 将response_data 从字符串转换为json格式
        response_info_data_col = ETL.change_column_to_json(response_info_fields.get(FieldsName.RESPONSE_INFO_DATA),
                                                           SchemaName.SEARCH_ITEMS)
        # 获取客户端传过来的参数字段
        client_args_args_col = response_info_data_col.getItem(FieldsName.CLIENT_ARGS)
        # 将需要的数据字段组合成格式为 {field_name:column} 的字典
        data_info_fields = {
            FieldsName.USER_ID: response_info_data_col.getItem(FieldsName.USER_ID),
            FieldsName.KEYWORD_TOKENIZER: response_info_data_col.getItem(FieldsName.KEYWORD_TOKENIZER),
            FieldsName.ITEMS: response_info_data_col.getItem(FieldsName.ITEMS),
            FieldsName.ORDER_BY: client_args_args_col.getItem(FieldsName.ORDER_BY),
            FieldsName.PAGE: client_args_args_col.getItem(FieldsName.PAGE),
            FieldsName.KEYWORD: client_args_args_col.getItem(FieldsName.KEYWORD),
            FieldsName.REVERSE: client_args_args_col.getItem(FieldsName.REVERSE)
        }
        # 最终合并的 {field_name:column} 字典
        merge_fields_dict = {}
        # 向其中合并需要的字段列
        merge_fields_dict.update(main_fields)
        merge_fields_dict.update(request_info_fields)
        merge_fields_dict.update(response_info_fields)
        merge_fields_dict.update(data_info_fields)
        # 将上面的字典转换为最终需要的 DataFrame ,并删除一些不必要的列
        self.final_df = ETL.dict_to_df(self.lines,
                                       merge_fields_dict,
                                       "value",
                                       FieldsName.REQUEST_INFO,
                                       FieldsName.RESPONSE_INFO,
                                       FieldsName.RESPONSE_INFO_DATA)

    def start(self):
        # 构造一个 Batch 处理对象
        with SearchItemsBatch() as S:
            # 将最终的DataFrame写入到Batch中,通过 process_batch 方法对其处理
            self.query_task = self.final_df \
                .writeStream \
                .foreachBatch(S.process_batch) \
                .outputMode("append") \
                .trigger(processingTime="5 second") \
                .start()
            self.query_task.awaitTermination()

    def stop(self):
        # 供停止任务时使用
        self.query_task.stop()


def s(b):
    input()
    b.stop()


if __name__ == '__main__':
    ba = BrowsingTimeAnalysis()
    threading.Thread(target=s, args=(ba,)).start()
    ba.start()
