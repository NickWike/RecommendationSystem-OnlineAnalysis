from conf import KafkaConf
from kafka import KafkaProducer
from pyspark.sql.dataframe import DataFrame
from namespace import FieldsName
from pyspark.sql import Window
from pyspark.sql import functions as f


class BaseKafkaProducer(KafkaProducer):

    def __init__(self):
        super().__init__(**KafkaConf.PRODUCER_CONFIG)

    def process_batch(self, df: DataFrame, batch_id):
        pass

    def stop(self):
        self.flush()
        self.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()


class SearchItemsBatch(BaseKafkaProducer):

    def process_batch(self, df: DataFrame, batch_id):
        window = Window.partitionBy(FieldsName.USER_ID).orderBy(FieldsName.ACCESS_TIME)

        new_df = df.withColumn("before_time", f.lag(FieldsName.ACCESS_TIME, 1).over(window))

        items_to_time = new_df.select(FieldsName.USER_ID, FieldsName.ITEMS,
                                      (f.col(FieldsName.ACCESS_TIME) - f.col("before_time")).alias("browsing_time"))

        window2 = Window.partitionBy(FieldsName.USER_ID).orderBy(f.col("browsing_time").desc())

        final_result = items_to_time.withColumn("rank", f.rank().over(window2)).filter("rank <= 2")

        final_result = final_result.select(FieldsName.USER_ID, FieldsName.ITEMS, "browsing_time")

        for r in final_result.collect():
            self.send(
                topic="python_test1",
                key=r[FieldsName.USER_ID],
                value={
                    "items": r[FieldsName.ITEMS],
                    "browsing_time": r["browsing_time"]}
            )


class ItemDetailBatch(BaseKafkaProducer):

    def process_batch(self, df: DataFrame, batch_id):
        valid_df = df.filter(f"{FieldsName.LOG_LEVEL} == 'INFO' and {FieldsName.MESSAGE} == 'OK'")

        collect_df = valid_df.select(
            FieldsName.USER_ID,
            f.explode(FieldsName.ITEMS).alias(FieldsName.PRODUCT_ID)
        ).dropna(how="any")

        for row in collect_df.collect():
            self.send(
                topic="python_test1",
                key=row[FieldsName.USER_ID],
                value=row[FieldsName.PRODUCT_ID]
            )


class ShopCarOptBatch(BaseKafkaProducer):

    def process_batch(self, df: DataFrame, batch_id):
        checked_df = df.filter(f"{FieldsName.REQUEST_INFO_REQUEST_PATH} == '/product/car/checked_car_items.action'") \
                        .select(FieldsName.USER_ID, f.explode(FieldsName.ITEMS).alias(FieldsName.PRODUCT_ID)) \
                        .drop(FieldsName.ITEMS) \
                        .dropna(how="any")

        add_car_df = df.filter(f"{FieldsName.REQUEST_INFO_REQUEST_PATH} == '/product/car/add_to_car.action'") \
            .select(FieldsName.USER_ID, FieldsName.PRODUCT_ID) \
            .dropna(how="any")

        collect_df = checked_df.union(add_car_df).distinct()

        for row in collect_df.collect():
            self.send(
                topic="python_test1",
                key=row[FieldsName.USER_ID],
                value=row[FieldsName.PRODUCT_ID]
            )


if __name__ == '__main__':
    with BaseKafkaProducer() as b:
        for i in range(10):
            b.send("python_test1", key=f"key{i}", value={"test_key": i})
        b.send("python_test1", key="end", value=True)


