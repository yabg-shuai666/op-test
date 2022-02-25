#!/usr/bin/python
# -*- coding: UTF-8 -*-

import unittest
import json

# fmt:off
import sys
import os
sys.path.append(os.path.dirname(__file__) + "/..")
from fesql import convert
from feql import convert as feqlconvert
# fmt:on

# TODO: add test case easier, maybe yaml case?

resource_path = os.path.dirname(__file__)


def abs_path(param):
    return resource_path + "/" + param


def fesql(op_file, config_file, cfg_is_info=False, debug=False):
    with open(abs_path(op_file)) as op, open(abs_path(config_file)) as config:
        config_all = json.load(config)   #加载配置文件
        if debug:
            print(config_all)
        real_config = config_all if cfg_is_info else config_all['app']['feature_info']
        input_ops = op.read()   #读取原数据
        print(type(input_ops))
        print(type(input_ops))
        print("读取原数据")
        ok, sql, sql_config, fe = convert.to_sql(input_ops, real_config)   #true sql语句  sql结构  sql特征
        assert ok   #断言
        # sign(fe) has 2 more lines:
        # # start fe code
        # w_feature_output = window(table=sql_table, output="w_output_feature_table")
        filtered, _, _ = feqlconvert.remove_op(input_ops, real_config)
        _, _, good = convert.remove_op(filtered, real_config)
        result_cnt = len(fe.splitlines())
        print("convert ", good, " ops to fesql sign, result ",
              result_cnt, " line(if !=0, include 2 more lines)")
        assert result_cnt - 2 == good or (result_cnt == 0 and good == 0)
        print("--------------------sql sql sql sql sql sql sql sql--------------------")
        print(sql)
        print("--------------------sql sql sql sql sql sql sql sql--------------------")
        print("--------------------fe fe fe fe fe fe fe fe fe fe fe --------------------")
        print(fe)
        print("--------------------fe fe fe fe fe fe fe fe fe fe fe --------------------")
    return sql, fe


def feql(op_file, config_file, cfg_is_info=False, debug=False):
    with open(abs_path(op_file)) as op, open(abs_path(config_file)) as config:
        config_all = json.load(config)
        real_config = config_all if cfg_is_info else config_all['app']['feature_info']
        if debug:
            print(real_config)
        input_ops = op.read()
        ok, feql, column, sign, _ = feqlconvert.get_feql(
            input_ops, real_config)
        assert ok
        # sign has 1 more line:
        # w_feature_output = window(table=xxx, output="w_output_feature_table")
        _, _, good = feqlconvert.remove_op(input_ops, real_config)
        result_cnt = len(sign.splitlines())
        print("convert ", good, " ops to feql sign, result ",
              result_cnt, " line(if !=0, include 1 more lines)")
        assert result_cnt - 1 == good or (result_cnt == 0 and good == 0)
        print("--------------------column column column column column column column column--------------------")
        print(column)
        print("--------------------column column column column column column column column--------------------")
        print("--------------------sign sign sign sign sign sign sign sign sign sign --------------------")
        print(sign)
        print("--------------------sign sign sign sign sign sign sign sign sign sign--------------------")
    return column, sign


class TestConvert(unittest.TestCase):
    def test_window_union_new_key(self):
        # union_selected_ops has ops like `dayofweek(multi_direct())`, convert(fesql convert use feql convert too) can't handle this, so remove it
        sql, sign = fesql("union_selected_ops.bk", "union_pyconf.json")
        feql("union_selected_ops.bk", "union_pyconf.json")
        print("HERE")
        print(sql, "\n\n", sign)
        print("test_window_union_new_keytest_window_union_new_keytest_window_union_new_key")

    # def test_split_key(self):
    #     fesql("split.ops", "split.json", cfg_is_info=True)
    #     feql("split.ops", "split.json", cfg_is_info=True)
    #     print("test_split_key test_split_keyt est_split_key")

    # def test_myhug_sql_window_count(self):
    #     sql, _ = fesql("myhug_selected_ops_window_count.bk",
    #                    "myhug_pyconf.json")
        # assert sql.find("count_where") != -1
        # feql("myhug_selected_ops_window_count.bk", "myhug_pyconf.json")
        # print("test_myhug_sql_window_count test_myhug_sql_window_count test_myhug_sql_window_count")

    # def test_myhug_total(self):
    #     fesql("myhug_selected_ops.bk", "myhug_pyconf.json")
    #     feql("myhug_selected_ops.bk", "myhug_pyconf.json")

    # def test_last_value(self):
    #     feql("last_value_selected_ops.bk", "last_value_pyconf.json")
    #     fesql("last_value_selected_ops.bk", "last_value_pyconf.json")

    # def test_join_condition(self):
    #     feql("join_selected_ops_one.bk", "join_pyconf.json")
    #     sql, sign = fesql("join_selected_ops_one.bk", "join_pyconf.json")
    #     print("HERE")
    #     print(sql, "\n\n", sign)
        # assert sql == """# start sql code
# # output table name: sql_table

# select
#     `batch110174_flatten_request`.`reqId` as reqId_1,
#     `batch110062_gf_bianjieceshi_biaozhun_sag_f5_PRD_CODE__eventTime_0_2147483645`.`amt2` as batch110062_gf_bianjieceshi_biaozhun_sag_f5_amt2_multi_last_value_0
# from
#     `batch110174_flatten_request`
#     last join `batch110062_gf_bianjieceshi_biaozhun_sag_f5` as `batch110062_gf_bianjieceshi_biaozhun_sag_f5_PRD_CODE__eventTime_0_2147483645` order by batch110062_gf_bianjieceshi_biaozhun_sag_f5_PRD_CODE__eventTime_0_2147483645.`ingestionTime` on `batch110174_flatten_request`.`PRD_CODE` = `batch110062_gf_bianjieceshi_biaozhun_sag_f5_PRD_CODE__eventTime_0_2147483645`.`PRD_CODE` and `batch110062_gf_bianjieceshi_biaozhun_sag_f5_PRD_CODE__eventTime_0_2147483645`.`ingestionTime` < `batch110174_flatten_request`.`eventTime`;"""

        # sql, _ = fesql("join_selected_ops.bk", "join_pyconf.json")


if __name__ == '__main__':
    unittest.main()
