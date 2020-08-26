from pyspark.sql.types import *
from namespace import SchemaName
from namespace import FieldsName


s = StructType([
    StructField("name", StringType()),
    StructField("age", IntegerType()),
    StructField("interests", ArrayType(StringType())),
    StructField("edu", StructType([
        StructField("primary", StructType([
            StructField("name", StringType()),
            StructField("graduation", TimestampType())
        ]))
    ]))
])


class FieldsSchema:
    __SEARCH_ITEMS_SCHEMA = StructType([
        StructField(FieldsName.USER_ID, StringType()),
        StructField(FieldsName.CLIENT_ARGS, StructType([
            StructField(FieldsName.ORDER_BY, StringType()),
            StructField(FieldsName.PAGE, StringType()),
            StructField(FieldsName.KEYWORD, StringType()),
            StructField(FieldsName.REVERSE, StringType()),
        ])),
        StructField(FieldsName.KEYWORD_TOKENIZER, ArrayType(StringType())),
        StructField(FieldsName.ITEMS, ArrayType(StringType()))
    ])

    __SEARCH_DETAIL_SCHEMA = StructType([
        StructField(FieldsName.USER_ID, StringType()),
        StructField(FieldsName.ITEMS, ArrayType(StringType()))
    ])

    __SHOP_CAR_SCHEMA = StructType([
        StructField(FieldsName.USER_ID, StringType()),
        StructField(FieldsName.ITEMS, ArrayType(StringType())),
        StructField(FieldsName.PRODUCT_ID, StringType()),
        StructField(FieldsName.CHECKED, StringType()),
        StructField(FieldsName.QUANTITY, StringType())
    ])

    __SCHEMAS = {
        SchemaName.SEARCH_ITEMS: __SEARCH_ITEMS_SCHEMA,
        SchemaName.ITEM_DETAIL: __SEARCH_DETAIL_SCHEMA,
        SchemaName.SHOP_CAR: __SHOP_CAR_SCHEMA
    }

    @classmethod
    def get_schema(cls, schema_name):
        return cls.__SCHEMAS.get(schema_name)


if __name__ == '__main__':
    print(type(FieldsSchema.get_schema(SchemaName.SEARCH_ITEMS)))
