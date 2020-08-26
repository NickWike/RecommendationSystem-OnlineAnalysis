from pyspark.sql import SparkSession
from namespace import FieldsName, SchemaName
from pretreatment import ETL
from conf.SparkConf import ShopCarConf
from my_source.BaseSource import BaseLogAnalysis
from my_sink.KafkaSink import ShopCarOptBatch
import threading


class ShopCarAnalysis(BaseLogAnalysis):

    def __init__(self):
        super().__init__()
        # 构造Session对象
        self.spark_session = SparkSession \
            .builder \
            .master(ShopCarConf.MASTER) \
            .appName(ShopCarConf.APP_NAME) \
            .getOrCreate()
        # 从指定源中获取流数据
        self.source_lines = self.spark_session \
            .readStream \
            .format("socket") \
            .option("host", "127.0.0.1") \
            .option("port", 9999) \
            .load()
        self.sink_batch = ShopCarOptBatch()
        super()._split_main_lines()
        self.__init_final_df()

    def __init_final_df(self):
        # 将response_data 从字符串转换为json格式
        response_info_data_col = ETL.change_column_to_json(
            self.response_info_fields.get(FieldsName.RESPONSE_INFO_DATA),
            SchemaName.SHOP_CAR)

        data_info_fields = {
            FieldsName.USER_ID: response_info_data_col.getItem(FieldsName.USER_ID),
            FieldsName.CHECKED: response_info_data_col.getItem(FieldsName.CHECKED),
            FieldsName.PRODUCT_ID: response_info_data_col.getItem(FieldsName.PRODUCT_ID),
            FieldsName.ITEMS: response_info_data_col.getItem(FieldsName.ITEMS),
            FieldsName.QUANTITY: response_info_data_col.getItem(FieldsName.QUANTITY)
        }

        merge_fields_dict = {
            FieldsName.REQUEST_INFO_REQUEST_PATH: self.request_info_fields.get(FieldsName.REQUEST_INFO_REQUEST_PATH)
        }
        merge_fields_dict.update(self.main_fields)
        merge_fields_dict.update(data_info_fields)

        self.final_df = ETL.dict_to_df(
            self.source_lines,
            merge_fields_dict,
            "value",
            FieldsName.REQUEST_INFO,
            FieldsName.RESPONSE_INFO
        )

    def start(self):
        self.query_task = self.final_df \
            .writeStream \
            .foreachBatch(self.sink_batch.process_batch) \
            .outputMode("append") \
            .trigger(processingTime="5 second") \
            .start()
        self.query_task.awaitTermination()


def my_batch(df, batch_id):
    print(df.isStreaming)
    df.show()


def s(task: ShopCarAnalysis):
    input()
    task.stop()


if __name__ == '__main__':
    with ShopCarAnalysis() as sca:
        t = threading.Thread(target=s, args=(sca,))
        t.start()
        sca.start()
