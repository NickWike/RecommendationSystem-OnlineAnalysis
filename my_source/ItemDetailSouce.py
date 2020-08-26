from pyspark.sql import SparkSession
from conf.SparkConf import ItemDetailConf
from pretreatment import ETL
from namespace import FieldsName
from namespace import SchemaName
from my_sink.KafkaSink import ItemDetailBatch
import threading


class ItemDetailAnalysis(object):

    def __init__(self):
        self.spark_session = SparkSession \
            .builder \
            .master(ItemDetailConf.MASTER) \
            .appName(ItemDetailConf.APP_NAME) \
            .getOrCreate()
        self.lines = self.spark_session \
            .readStream \
            .format("socket") \
            .option("host", "127.0.0.1") \
            .option("port", 9999) \
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
                                                           SchemaName.ITEM_DETAIL)
        data_info_fields = {
            FieldsName.USER_ID: response_info_data_col.getItem(FieldsName.USER_ID),
            FieldsName.ITEMS: response_info_data_col.getItem(FieldsName.ITEMS)
        }

        # 最终合并的 {field_name:column} 字典
        merge_fields_dict = {
            FieldsName.REQUEST_INFO_REQUEST_REFERRER: request_info_fields.get(FieldsName.REQUEST_INFO_REQUEST_REFERRER),
        }

        merge_fields_dict.update(data_info_fields)
        merge_fields_dict.update(main_fields)

        self.final_df = ETL.dict_to_df(self.lines,
                                       merge_fields_dict,
                                       "value",
                                       FieldsName.REQUEST_INFO,
                                       FieldsName.RESPONSE_INFO)

    def start(self):
        # 构造一个 Batch 处理对象
        with ItemDetailBatch() as batch:
            # 将最终的DataFrame写入到Batch中,通过 process_batch 方法对其处理
            self.query_task = self.final_df \
                .writeStream \
                .foreachBatch(batch.process_batch) \
                .outputMode("append") \
                .trigger(processingTime="5 second") \
                .start()
            self.query_task.awaitTermination()

    def stop(self):
        # 供停止任务时使用
        self.query_task.stop()


def my_batch(df, batch_id):
    df.show()


def s(task: ItemDetailAnalysis):
    input()
    task.stop()


if __name__ == '__main__':
    i = ItemDetailAnalysis()
    threading.Thread(target=s, args=(i,)).start()
    i.start()
