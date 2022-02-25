#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
#

"""

"""

from feql import parser as p
from feql import model
from feql import ops
import logging
import json

WINDOW_OP = {"window_sum", "window_max", "window_min", "window_avg", "window_unique_count", "window_count",
             "window_ratio", "window_top1_ratio"}
MULTI_OP = {"multi_sum", "multi_max", "multi_min", "multi_avg", "multi_std", "multi_unique_count", "multi_count"}

JOIN_OP = {"multi_direct", "multi_last_value"}
MATH_OP = {"dayofweek", "isweekday", "hourofday", "log"}
UNMATH_OP = {"multi_top3frequency"}
UNKNOW_OP = {"isin", "combine"}
BINARY_OP = {"add", "divide", "multiply", "subtract"}
WRONG_OP = {}
# WRONG_OP["isin"] = "multi_top3frequency"

def get_col_type(name, table, config):
    for entity in config["entity_detail"][table]["features"]:
        if entity["id"] == "{}.{}".format(table, name):
            return str(entity["feature_type"]).lower()
    return None

def isNumber(type):
    if type == "int" or type == "float" or type == "double" or type == "bigint" or type == "smallint" or type == "long":
        return True
    return False

def getOps(fn_node, ops):
    if fn_node.name in WINDOW_OP:
        ops.append(fn_node.name)
    if fn_node.name in MULTI_OP:
        ops.append(fn_node.name)
    if fn_node.name in JOIN_OP:
        ops.append(fn_node.name)
    if fn_node.name in MATH_OP:
        ops.append(fn_node.name)
    # 不支持数学函数计算
    if fn_node.name in UNMATH_OP:
        ops.append(fn_node.name)
    if fn_node.name in UNKNOW_OP:
        ops.append(fn_node.name)
    if fn_node.name in BINARY_OP:
        ops.append(fn_node.name)
    for arg in fn_node.args:
        if isinstance(arg, p.FnNode):
            getOps(arg, ops)

def isDifferentWindow(ops):
    for key in WRONG_OP:
        if key in ops and WRONG_OP[key] in ops:
            return True
    isWindow = False
    isMulti = False
    isJoin = False
    isMath = False
    isUnknown = False
    isBinary = False
    isUnmath = False

    for op in ops:
        # 不能加continue！
        if op in WINDOW_OP:
            isWindow = True
            # continue
        if op in MULTI_OP:
            isMulti = True
            # continue
        if op in JOIN_OP:
            isJoin = True
            # continue
        if op in MATH_OP:
            isMath = True
        if op in UNMATH_OP:
            isUnmath = True
        if op in UNKNOW_OP:
            isUnknown = True
        if op in BINARY_OP:
            isBinary = True
        if isWindow and isMulti or isJoin and (isWindow or isMath or isUnmath):
            return True
    return False


def remove_op(input, config):
    lines = ""
    tmp = p.FzParser()
    table = config["target_entity"]
    input_lines = input.splitlines()
    for op in input_lines:
        if op == "":
            logging.info("remove null op")
            continue
        if "//" in op:
            continue
        # if "split" in op:
        #     logging.info("remove split_kv op {}".format(op))
        #     continue
        fn_node = tmp.parse(op)
        ops = []
        getOps(fn_node, ops)
        # print("ops : {}".format(ops))
        if isDifferentWindow(ops):
            logging.info("ops : {}".format(ops))
            logging.info("lots of window error : {}".format(op))
            continue
        function = fn_node.name
        if len(ops) <= 1 and (function == "window_sum" or function == "window_max" or function == "window_avg" or function == "window_min"):
            col = fn_node.args[3].name.split(".")[1]
            col_type = get_col_type(col, table, config)
            if not isNumber(col_type):
                logging.info("bad op {} with type {} in {}".format(col, col_type, table))
                logging.info("schema type error {}".format(op))
                continue
        if len(ops) <= 1 and (function == "multi_sum" or function == "multi_max" or function == "multi_avg" or function == "multi_min" or function == "multi_std"):
            if "split" in fn_node.get_id():
                lines += op
                lines += "\n"
                continue
            union_table = fn_node.args[1].name.split(".")[0]
            col = fn_node.args[1].name.split(".")[1]
            col_type = get_col_type(col, union_table, config)
            if not isNumber(col_type):
                logging.info("bad op {} with type {} in {}".format(col, col_type, union_table))
                logging.info("schema type error", op)
                continue
        # if "combine" in op:
        #     continue
        lines += op
        lines += "\n"
    good_op_cnt = len(lines.splitlines())
    logging.info("good op size = {}".format(good_op_cnt))
    # print("good op size = {}".format(len(lines.splitlines())))
    return lines, len(input_lines), good_op_cnt

def to_feql(input, config):
    """
    multi_min(sample,product.a1,10)
    multi_avg(sample,product.a1,100)
    multi_max(sample,product.a1,100)
    """
    logging.info("temp feql size = {}".format(len(input.splitlines())))
    logging.info(input)
    logging.info(json.dumps(config))
    # op_json = json.loads(config)
    input, _, _ = remove_op(input, config)
    logging.info("support ops are {} ".format(input))
    # print("support ops are {} ".format(input))
    #TODO check result
    context = model.FeQLCodeGenContext(config)
    ok = context.init()
    logging.info(context.fe_config_str)
    if not ok:
        return False, None, None
    fz_parser = p.FzParser()
    for idx, line in enumerate(input.splitlines()):
        if not line:
            continue
        fn_node = fz_parser.parse(line)
        if not fn_node:
            continue
        # logging.info("current op[{}]:{}".format(idx, line))
        ok, var = ops.handle_all_ops(fn_node, idx, context)
        if not ok:
            logging.warning("fail to convert for line %s", line)
            return False, None, None
        context.put_op_var(fn_node.get_id(), var)
    ok, gen_code = context.gen()
    # print("temp to feql")
    # print(gen_code)
    if not ok:
        return False, None, None
    return True, gen_code, context.fe_config_str

def split_feql(feql = ""):
    logging.info(feql)
    column_code = []
    sign_code = []
    for line in feql.split("\n"):
        if line == "":
            continue
        if "w_feature_output" in line:
            sign_code.append(line)
        else:
            column_code.append(line)
    return "\n".join(column_code), "\n".join(sign_code)

def get_feql(input, config):
    status, feql, fe_config = to_feql(input, config)
    column_code, sign_code = split_feql(feql)
    return status, feql, column_code, sign_code, fe_config



