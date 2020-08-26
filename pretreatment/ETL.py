from pyspark.sql.dataframe import DataFrame
from pyspark.sql.column import Column
from pyspark.sql import functions as f
from namespace import FieldsName
from my_struct.ResponseStruct import FieldsSchema

"""
    @author:zh123
    @date:2020-04-22
    @description:
        1.用于流数据的预处理
        2.主要是列数据的转换
"""


def split_line_to_columns(df: DataFrame) -> dict:
    """
    对传过来的日式数据行做初步拆分
    原来的日志行的格式如下
    +-----------------------------------------------------------------------------------------------+
    |                                         value                                                 |
    +-----------------------------------------------------------------------------------------------+
    | {access_time} - {log_level} - {remote_address} - {request_info} - {message} - {response_info} |
    +-----------------------------------------------------------------------------------------------+

    :param df: 流数据过来的日志行
    :return: 格式为 {"field_name: column, ...} 的字典
    """

    # 按照 ' - ' 对value列进行分割
    main_fields = f.split(df.value, " - ")
    # 来访时间列转换为 Unix 时间戳方便后面计算 session_time
    access_time_col = f.unix_timestamp(main_fields.getItem(0), "[yyyy-MM-dd HH:mm:ss,SSS]")
    # 按照上面所给的日志格式,拆分后取相应的列
    log_level_col = main_fields.getItem(1)
    remote_ip_col = main_fields.getItem(2)
    request_info_col = main_fields.getItem(3)
    message_col = main_fields.getItem(4)
    response_info_col = main_fields.getItem(5)

    # 字段名称,和列组合成字典返回
    fields_dict = {
        FieldsName.ACCESS_TIME: access_time_col,
        FieldsName.LOG_LEVEL: log_level_col,
        FieldsName.REMOTE_IP: remote_ip_col,
        FieldsName.REQUEST_INFO: request_info_col,
        FieldsName.MESSAGE: message_col,
        FieldsName.RESPONSE_INFO: response_info_col
    }

    return fields_dict


def split_request_info_to_columns(col: Column) -> dict:
    """
    将传入的 RequestInfo 列进行拆分
    RequestInfo 列存放的数据格式如下
    +-----------------------------------------------------------------------------------+
    |                                   request_info                                    |
    +-----------------------------------------------------------------------------------+
    | `{request_method} ~ {request_path} ~ {request_http_version} ~ {request_referrer}` |
    +-----------------------------------------------------------------------------------+

    :param col: 装有 RequestInfo 的列
    :return: 格式为 {"field_name: column, ...} 的字典
    """

    # 整个列字段的值由 '` `' 包裹,所以先替换掉,然后按照 ' ~ ' 进行分割
    request_info_fields = f.split(f.regexp_replace(col, "`", ""), " ~ ")
    # 按照上面所给的日志格式,拆分后取相应的列
    request_method_col = request_info_fields.getItem(0)
    request_path_col = request_info_fields.getItem(1)
    request_http_version_col = request_info_fields.getItem(2)
    request_referrer_col = request_info_fields.getItem(3)

    # 字段名称,和列组合成字典返回
    fields_dict = {
        FieldsName.REQUEST_INFO_REQUEST_METHOD: request_method_col,
        FieldsName.REQUEST_INFO_REQUEST_PATH: request_path_col,
        FieldsName.REQUEST_INFO_HTTP_VERSION: request_http_version_col,
        FieldsName.REQUEST_INFO_REQUEST_REFERRER: request_referrer_col
    }

    return fields_dict


def split_response_info_to_columns(col: Column) -> dict:
    """
    将传入的 ResponseInfo 列进行拆分
    ResponseInfo 列存放的数据格式如下
    +-------------------------------------------------------------+
    |                       response_info                         |
    +-------------------------------------------------------------+
    | `{response_status} ~ {response_duration} ~ {response_data}` |
    +-------------------------------------------------------------+

    :param col: 装有 ResponseInfo 的列
    :return: 格式为 {"field_name: column, ...} 的字典

    """
    # 整个列字段的值由 '` `' 包裹,所以先替换掉,然后按照 ' ~ ' 进行分割
    response_info_fields = f.split(f.regexp_replace(col, "`", ""), " ~ ")
    response_info_status_col = response_info_fields.getItem(0)
    response_info_duration_col = response_info_fields.getItem(1)
    response_info_data_col = response_info_fields.getItem(2)

    # 字段名称,和列组合成字典返回
    fields_dict = {
        FieldsName.RESPONSE_INFO_STATUS: response_info_status_col,
        FieldsName.RESPONSE_INFO_DURATION: response_info_duration_col,
        FieldsName.RESPONSE_INFO_DATA: response_info_data_col
    }

    return fields_dict


def change_column_to_json(col: Column, schema_name: str) -> Column:
    """
    通过传入的 ResponseData 列, 和模式名称,将这一列的字符串数据转换成对应的 json 格式
    返回一列新的列

    :param col: 装有 ResponseData 的列
    :param schema_name: 列需要转换成的目标格式
    :return: 通过转换后的列
    """

    # 根据模式名获取对应的模式
    schema = FieldsSchema.get_schema(schema_name)
    # 转换后的列
    response_info_data_col = None

    # 判断模式是否为空
    if schema:
        # 因为整个 response_data 由 '<>' 所以要先替换掉在来解析列中的字符串数据
        response_info_data_col = f.from_json(f.regexp_replace(col, "[<>]", ""), schema)

    return response_info_data_col


def dict_to_df(source: DataFrame, fields_dict: dict, *drop) -> DataFrame:
    """
    将 {fields:column, ...} 的字典转换为流式的DataFrame 拱流的写入时使用

    :param source: 最开始读入的流式 DataFrame
    :param fields_dict: 格式为 {field_name:column, ...} 的字典
    :param drop: 需要删除的列名称
    :return:
    """
    df = source
    # 遍历字典
    for fields_name, column in fields_dict.items():
        # 根据字典中的列名称,和列 附加在原来的DataFrame后面
        df = df.withColumn(fields_name, column)
    # 删除不需要的行并返回
    return df.drop(*drop)
