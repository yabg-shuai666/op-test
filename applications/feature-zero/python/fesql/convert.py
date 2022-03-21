#!/usr/bin/python
# -*- coding: UTF-8 -*-
from feql import convert as feqlconvert
from . import context
from . import front
import logging
import json

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s', \
                    datefmt='%a, %d %b %Y %H:%M:%S')
logger = logging.getLogger(__name__)

# 字段直出
ORIGIN = {"original"}
# label op
LABEL_OP = {"binary_label", "regression_label"}
# split相关op
SPLIT_OP = {"split", "split_key", "split_value"}
# window基础op
WINDOW_BASE_OP = {"window_sum", "window_max", "window_min", "window_avg", "window_unique_count", "window_count"}
# window 高阶op
WINDOW_ADVANCE_OP = {"window_top1_ratio"}
# 多表基础op
MULTI_BASE_OP = feqlconvert.MULTI_OP
# 多表高阶op
MULTI_ADVANCE_OP = {"multi_top3frequency"}
# 函数op
FUNCTION_OP = {"isin", "combine", "timediff", "log", "dayofweek", "isweekday", "hourofday"}
# 二元函数操作op
BINARY_OP = {"add", "divide", "multiply", "subtract"}

# join op 和 math op参考feql/convert.py
JOIN_OP = feqlconvert.JOIN_OP

# fesql的函数
sql_split = "fz_split"
sql_split_key = "fz_split_by_key"
sql_split_value = "fz_split_by_value"
sql_join = "fz_join"

# sql关键词
# SQL_KEYS = {"time", "status", "date", 'all'}
SQL_KEYS = {}

# 直接把相关op拉入黑名单，目前sql不支持的op
BLACK_LIST_OP = {"window_ratio","multi_std"}


def debug_info():
    logger.info("fz to sql begin")


def isInBlackList(op):
    for black_op in BLACK_LIST_OP:
        if black_op in op:
            return True
    return False


def remove_op(input, config):
    """
    删除fesql 目前不支持的op
    :param input:
    :param config:
    :return:
    """
    # lines = ""
    # tmp = p.FzParser()
    # table = config["target_entity"]
    sum = 0
    good_num = 0
    new_input = []
    for idx, op in enumerate(input.splitlines()):
        sum = sum + 1
        if isInBlackList(op):
            continue
        logger.info(op)
        new_input.append(op)
        good_num = good_num + 1
    logger.info(f"sum = {sum}")
    logger.info(f"good op = {good_num}")
    return "\n".join(new_input), sum, good_num


def replace_sql_keys(config, new_config):
    for table in config["entity_detail"]:
        print(table)
        for field in config["entity_detail"][table]["features"]:
            name = field["id"].split(".")[1]
            if name in SQL_KEYS:
                print(name)



def to_sql(input_op, config):
    logger.info("fz to sql begin")
    logging.info(input_op)
    config_str = json.dumps(config)
    logging.info(config_str)
    filtered, _, _ = feqlconvert.remove_op(input_op, config)
    input_op, _, _ = remove_op(filtered, config)    #删除fesql 目前不支持的op  得到支持的op
    logger.info("sql support ops are {} ".format(input_op))
    # config = replace_sql_keys(config, json.loads(config_str))
    # 生成fe的schema
    # =================================================
    from feql import schema
    fe_config = {}
    ok, tables = schema.build_tables(config)
    if not ok:
        logger.warning("fail to build tables")
        return False, "", "", ""
    tables = tables
    table_encoder = schema.SchemaEncoder()
    fe_config['tableInfo'] = tables
    fe_config_str = table_encoder.encode(fe_config)
    logging.info(fe_config_str)
    # =================================================

    sql_context = context.SQLCodeGenContext(config)

    sql_context.init()
    from feql import parser
    fz_parser = parser.FzParser()
    for idx, line in enumerate(input_op.splitlines()):
        if not line:
            continue
        fn_node = fz_parser.parse(line)
        if not fn_node:
            continue
        code_segment = []
        state = front.convert_ops(fn_node, idx, code_segment, sql_context)
        if not state.ok:
            logger.error("convert op failed. row num is {}, op is {}".format(state.row_num + 1, line))

    #for select in sql_context.select:
    #    logger.info(select.gen_code())
    # logger.info(sql_context.gen_sql())

    fe_str = sql_context.gen_fe()
    logger.info("fe script={}".format(fe_str))
    # logger.info(sql_context.gen_table_ddl())
    sql = sql_context.gen_sql() + ";"    #拿到sql
    # 针对关键词加转义符
    from fesql import parser
    regex = parser.Regex()
    regex.stop_words.append(parser.LEFT_BUCKET)
    regex.stop_words.append(parser.RIGHT_BUCKET)
    regex.stop_words.append(parser.SPLITTER)
    regex.stop_words.append(parser.DOT)
    regex.stop_words.append(parser.WHITH_SPACE)
    regex.parseToken(sql)
    tokens = []
    for token in regex.parseToken(sql):
        if token.name.lower() in SQL_KEYS:
            tokens.append(token)
    sql = regex.replaceKeys(sql, tokens)
    logger.info("fz to sql end")
    return True, sql, fe_config_str, fe_str



# print("fz to sql begin")
logger.info("fz to sql begin")
logger.info(feqlconvert.WINDOW_OP)
logger.info(BLACK_LIST_OP)
