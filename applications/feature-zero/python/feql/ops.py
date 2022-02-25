# -*- coding: utf-8 -*-

import logging
from feql import model
from feql import parser as p
import re

logger = logging.getLogger(__name__)
CONTINUOUS_FEAT_CATEGORY = {"add", "subtract", "divide", "multiply",
    "window_sum", "window_avg", "window_min", "window_max", "multi_avg", "multi_min", "multi_max",
    "multi_sum", "multi_std", "multi_count", "multi_unique_count"}
WINDOW_OP = {"window_sum", "window_max", "window_min", "window_avg", "window_unique_count", "window_count"}
MULTI_OP = {"multi_avg", "multi_min", "multi_max", "multi_sum", "multi_std"}
SPLIT_OP = {"split", "split_key", "split_value"}

def handle_last_join(fn_node, idx, ctx, def_var = True):
    """
    fz
    multi_last_value(from_entity_name, to_entity_name.feature_name, window_size)
    feql
    lastjoin(from_entity_name, to_entity_name.feature_name)
    window(windowsize)
    :param fn_node:
    :param idx:
    :param ctx:
    :param def_var:
    :return:handle_last_join
    """
    op_var = model.OpVar()
    if not fn_node.name.endswith("last_value"):
        logger.warning("last_value fn node is required but %s", fn_node.name)
        return False, None
    fn_name = fn_node.name.split("_")[1] + fn_node.name.split("_")[2]
    main_table = fn_node.args[0].name
    union_table = fn_node.args[1].name.split(".")[0]
    ok, window_object = ctx.get_window(main_table, union_table, "slice")
    # key = window_object.get_window_key()
    if not ok:
        logger.warning("fail to find window with main table %s and union table %s", main_table, union_table)
        return False, None
    relation = window_object.relation
    if relation['type'] != "SLICE":
        logger.warning("fail to lastjoin for type %s", relation['type'])
        return False, None

    # 副表的查询字段，不是key！！！
    col_name = fn_node.args[1].name.split(".")[1]
    cnt = fn_node.args[2].name
    f_name = "f_%s_%s_%s_%s_%s" % (main_table, union_table, col_name, fn_name, idx)
    # 管理select表
    ctx.add_select(main_table)
    ctx.add_select(union_table)
    wname = window_object.wname + "_" + f_name

    # 仅支持条数，不支持天数
    # if model.is_time_limit(cnt):
    #     logger.warning("fail to get window size %s", cnt)
    #     return False, None

    _, _, _, _, limit = parse_window_size(cnt)
    count = limit

    join = model.JoinConfig(fn_node, idx, "lastjoin", col_name, relation["from_entity_keys"], relation["to_entity_keys"], relation['from_entity_time_col'], relation['to_entity_time_col'])

    # lastjoin 没有输出表
    wc = model.WindowConfig(wname, main_table, union_table, [], "", count, None, None, None, window_object.tables, window_object.relation, join)
    if not fn_node.is_gen_feature():
        wc.disable_gen_feature()
    ctx.add_lastjoin(wc)
    ctx.put_var(fn_node.get_id(), f_name)
    op_var.f_name = f_name
    return True, op_var

def handle_left_join(fn_node, idx, ctx, def_var = True, label = None):
    """
    multi_direct(from_entity_name, to_entity_name.feature_name)
    :param fn_node:
    :param idx:
    :param ctx:
    :param def_var:
    :return:
    """
    op_var = model.OpVar()
    if not fn_node.name.endswith("direct"):
        logger.warning("multi direct fn node is required but %s", fn_node.name)
        return False, None
    fn_name = fn_node.name.split("_")[1]
    main_table = fn_node.args[0].name
    union_table = fn_node.args[1].name.split(".")[0]
    ok, window_object = ctx.get_window(main_table, union_table, '1-1')
    if not ok:
        logger.warning("fail to find window with main table %s and union table %s", main_table, union_table)
        return False, None
    relation = window_object.relation
    if relation['type'] != "1-1":
        logger.warning("fail to leftjoin for type %s", relation['type'])
        return False, None
    # 管理select表
    ctx.add_select(main_table)
    ctx.add_select(union_table)

    col_name = fn_node.args[1].name.split(".")[1]
    f_name = "f_%s_%s_%s_%s_%s" % (main_table, union_table, col_name, fn_name, idx)
    wname = window_object.wname + "_" + f_name

    # leftjoin不需要时间列
    join = model.JoinConfig(fn_node, idx, "leftjoin", col_name, relation["from_entity_keys"], relation["to_entity_keys"], None, None)
    if label:
        join.label = feql_label
    wc = model.WindowConfig(wname, main_table, union_table, [], "", None, None, None, None, window_object.tables, window_object.relation, join)
    if not fn_node.is_gen_feature():
        wc.disable_gen_feature()
    ctx.add_leftjoin(wc)
    ctx.put_var(fn_node.get_id(), f_name)
    op_var.f_name = f_name
    return True, op_var

# feql_label = 'binary_label'
def handle_label(fn_node, idx, ctx, def_var = True):
    '''
    multi_direct original 可以支持label op。last_value不支持label的op
    :param fn_node:
    :param idx:
    :param ctx:
    :param def_var:
    :return:
    '''
    op_var = model.OpVar()
    fn_name = fn_node.name
    if fn_name == "binary_label" or fn_name == "regression_label":
        function = fn_node.args[0]
        global feql_label
        feql_label = fn_name
        if function.name == "original":
            ok, label_op = add_label_original(function, idx, ctx, True)
            # label_op.code = label_op.code.format()
            op_var.f_name = fn_name
            return True, op_var
        if function.name == "multi_direct":
            ok, label_op = add_label_leftjoin(function, idx, ctx, True)
            op_var.f_name = fn_name
            return True, op_var
        logging.info("label = {}".format(fn_name))
    return False, None

def add_label_leftjoin(fn_node, idx, ctx, def_var = True):
    return handle_left_join(fn_node, idx, ctx, def_var, True)

def add_label_original(fn_node, idx, ctx, def_var = True):
    op_var = model.OpVar()
    wname = ctx.sample_table + "_window"
    arg = fn_node.args[0].name
    tname = arg.split('.')[0]
    assert tname == ctx.sample_table
    col_name = arg.split(".")[1]
    f_name = "f_%s_%s_%s" % (fn_node.name, col_name, idx)
    ok, key, wn = ctx.get_only_table_window(tname)
    if not ok:
        wc = model.WindowConfig(wname, tname, None, [], "", None, None, None, tname + "_output", None)
        wn, key = ctx.add_window(wc)
    # 单表和多表区分
    # if wn != ctx.output_window.wname:
    #     code = "%s = column(%s.%s[0])" % (f_name, wn, col_name)
    #     ctx.append_code(key, code)
    #     ctx.add_label(f_name)
    #     return True, f_name
    code = "%s = column(%s.%s[0])" % (f_name, wn, col_name)
    ctx.put_var(fn_node.get_id(), f_name)
    ctx.append_code(key, code)
    # ctx.add_label(f_name)
    value = ctx.get_default_value(ctx.get_col_type(col_name, tname, ctx.config))
    ctx.add_label(f_name, None, value, feql_label)
    op_var.f_name = f_name
    return True, op_var

def handle_top_freq(fn_node, idx, ctx, def_var = True):
    """
    multi_top3frequency(instance,credit2debit_card_info_part2.card_agmt_id_an,32d:1000:0s)
    :param fn_node:
    :param idx:
    :param ctx:
    :param def_var:
    :return:
    """
    op_var = model.OpVar()
    if not fn_node.name.endswith("top3frequency"):
        logger.warning("top3freq fn node is required but %s", fn_node.name)
        return False, None
    fn_name = fn_node.name.split("_")[1]
    split_name = None
    if isinstance(fn_node.args[1], p.FnNode):
        split_node = fn_node.args[1]
        # distinct_count(split(join(列，",")))
        if split_node.name == "split":
            union_table = split_node.args[0].name.split(".")[0]
            col_name = split_node.args[0].name.split(".")[1]
            split_name = "split"
            separator = chr(int(split_node.args[1].name))

        if split_name is None:
            return False, None
    main_table = fn_node.args[0].name
    union_table = fn_node.args[1].name.split(".")[0]
    col_name = fn_node.args[1].name.split(".")[1]
    cnt = fn_node.args[2].name
    # start, end, _, _, _ = parse_window_sizze(cnt)
    start, end, at_least, time_limit, max_size = parse_window_size(cnt)
    ok, union_window = ctx.get_window(main_table, union_table, '1-M')
    if not ok:
        logger.warning("fail to find window with main table %s and union table %s", main_table, union_table)
        return False, None
    wname = union_window.wname
    key = union_window.get_window_key()
    if int(at_least) == 0:
        wc = model.WindowConfig(wname, main_table, union_table, None, None, max_size, None, time_limit,
                                main_table + "_output", None, None)
    else:
        wc = model.WindowConfig(wname, main_table, union_table, None, None, max_size, at_least, time_limit,
                                main_table + "_output", None, None)
    union_window.merge_window(wc)

    f_name = "f_%s_%s_%s_%s_%s" % (main_table, union_table, col_name, fn_name, idx)
    if def_var:
        code = "{} = column(range(get_values(sort_by_value(map(group_by({}[{}:{}], \"{}\"), x->count_of_window(x)))), 0, 3))".format(
                f_name, wname, start, end, col_name)
        # logger.info("gen code %s", code)
        ctx.append_code(key, code)
        ctx.put_var(fn_node.get_id(), f_name)
        ctx.add_discrete(f_name)
        op_var.f_name = f_name
        return True, op_var
    else:
        code = "range(get_values(sort_by_value(map(group_by(%s[%s:%s], \"%s\"), x->count_of_window(x))), 0, 3))" % (
                wname, start, end, col_name)
        ctx.append_code(key, code)
        logger.info("gen code %s", code)
        op_var.f_name = f_name
        return True, op_var

def handle_window_count(fn_node, idx, ctx, def_var = True):
    """
    window_count(date,sku_id,1d,user_id)
    """
    op_var = model.OpVar()
    op = fn_node.name
    # if not op.startswith("window_count") and not op.startswith("window_unique_count"):
    #     logger.warning("multi fn node is required but %s", fn_node.name)
    #     return False, None
    functionmap = {}
    functionmap["window_unique_count"] = "distinct_count"
    functionmap["window_count"] = "count"
    functionmap["window_sum"] = "sum"
    functionmap["window_avg"] = "avg"
    functionmap["window_min"] = "min"
    functionmap["window_max"] = "max"

    table = ctx.sample_table
    order = fn_node.args[0].name.split(".")[1]
    pk = fn_node.args[1].name.split(".")[1]
    cate_col = fn_node.args[3].name.split(".")[1]
    wname = "{}_{}_{}".format(table, order, pk)
    cnt = fn_node.args[2].name
    # cnt, at_least, time_limit = get_time_limit_least(cnt)
    start, end, at_least, time_limit, max_size = parse_window_size(cnt)
    if int(at_least) == 0:
        wc = model.WindowConfig(wname, table, None, [pk], order, max_size, None, time_limit, table + "_output",
                                None, None)
    else:
        if int(at_least) >= int(max_size):
            max_size = int(at_least) + 1
        wc = model.WindowConfig(wname, table, None, [pk], order, max_size, at_least, time_limit, table + "_output",
                                None, None)
    wn, key = ctx.add_window(wc)
    logger.info("get window name %s", wn)

    f_name = "f_%s_%s_%s_%s" % (table, fn_node.name, cate_col, idx)
    # current_var = "%s_%s" % (cate_col, idx)
    feature_col = "{}.{}".format(wn, cate_col)
    feature_window = "{}.{}[{}:{}]".format(wn, cate_col, start, end)
    if functionmap[op] == "count":
        bool_code = "{}[0] != null".format(feature_col)
        res1_code = "{}({}[0], {})".format(functionmap[op], feature_col, feature_window)
        res2_code = "null"
        code = ctx.if_function(bool_code, res1_code, res2_code)
    else:
        code = "{}({})".format(functionmap[op], feature_window)
    code = "{} = column({})".format(f_name, code)
    ctx.append_code(key, code)
    ctx.put_var(fn_node.get_id(), f_name)
    ctx.add_continuous(f_name)
    op_var.f_name = f_name
    return True, op_var

def parse_window_size(window_size):
    start = None
    end = None
    at_least = "0"
    time_limit = "0s"
    # 默认最大条数1000
    limit = 100
    parts = window_size.split(":")
    if len(parts) == 3:
        start = parts[2]
        end = parts[0]
        # at_least = 0
        limit = int(parts[1])
        # if int(parts[1]) > limit:
        time_limit = parts[0]
    if len(parts) == 2:
        start = parts[1]
        end = parts[0]
        at_least = end
        limit = int(at_least) + 1
        # time_limit = "1d"
    return start, end, at_least, time_limit, limit

def handle_window_top1_ratio(fn_node, idx, ctx,
                            def_var = True):

    """
    window_top1_ratio(date,sku_id,32d,user_id)
    """
    op_var = model.OpVar()
    if not fn_node.name.startswith("window_top1_ratio") and not fn_node.name.startswith("window_ratio"):
        logger.warning("multi fn node is required but %s", fn_node.name)
        return False, None

    table = ctx.sample_table
    order = fn_node.args[0].name.split(".")[1]
    pk = fn_node.args[1].name.split(".")[1]
    cate_col = fn_node.args[3].name.split(".")[1]
    wname = "{}_{}_{}".format(table, order, pk)
    cnt = fn_node.args[2].name
    start, end, at_least, time_limit, max_size = parse_window_size(cnt)
    if int(at_least) == 0:
        wc = model.WindowConfig(wname, table, None, [pk], order, max_size, None, time_limit, table + "_output", None,
                                None)
    else:
        if int(at_least) >= int(max_size):
            max_size = int(at_least) + 1
        wc = model.WindowConfig(wname, table, None, [pk], order, max_size, at_least, time_limit, table + "_output",
                                None, None)
    wn, key = ctx.add_window(wc)

    f_name = "f_%s_%s_%s_%s" % (table, fn_node.name, cate_col, idx)


    var2_condition = "x -> x.{} != null".format(cate_col)
    window_cnt = "{}[{}:{}]".format(wn, start, end)
    where_temp = "{}_where = {}".format(f_name, ctx.where_function(window_cnt, var2_condition))
    ctx.append_code(key, where_temp)
    where_temp = "{}_where".format(f_name)
    var2_code = "x -> double(count(x))/{}".format(ctx.count_function(where_temp))
    var1_code = ctx.group_by_function(window_cnt, "\"{}\"".format(cate_col))
    group_temp = "{}_group = {}".format(f_name, var1_code)
    ctx.append_code(key, group_temp)
    var1_code = "{}_group".format(f_name)

    if fn_node.name == "window_ratio":
        var1_condition = "x -> x.{}[0] == {}.{}[0]".format(cate_col, wn, cate_col)
        code = "first(get_values({}))".format(ctx.map_function(ctx.where_function(var1_code, var1_condition), var2_code))
        bool_code = "{}.{}[0] != null".format(wn, cate_col)
        code = ctx.if_function(bool_code, code, "null")
        if def_var:

            code = "{} = column({})".format(f_name, code)
            ctx.append_code(key, code)
            ctx.put_var(fn_node.get_id(), f_name)
            ctx.add_continuous(f_name)
            op_var.f_name = f_name
            return True, op_var
        else:

            op_var.code = code
            return True, op_var

    if fn_node.name == "window_top1_ratio":
        code = "first(top(get_values({}), 1))".format(ctx.map_function(var1_code, var2_code))
        if def_var:
            f_name = "f_%s_%s_%s_%s" % (table, fn_node.name, cate_col, idx)
            code = "{} = column({})".format(f_name, code)
            ctx.append_code(key, code)
            ctx.put_var(fn_node.get_id(), f_name)
            ctx.add_continuous(f_name)
            op_var.f_name = f_name
            return True, op_var
        else:
            op_var.code = code
            return True, op_var

def handle_multi_to_feql(fn_node, idx, ctx, def_var = True):
    op_var = model.OpVar()
    OP_FUNC = {"top3frequency": handle_top_freq}
    OP_MAPPING = {"unique": "distinct_count",
                  "count": "count_of_window"}
    if not fn_node.name.startswith("multi_") or fn_node.name in "multi_direct" or fn_node.name in "multi_last_value":
        logger.warning("multi fn node is required but %s", fn_node.name)
        return False,  None
    fn_name = fn_node.name.split("_")[1]
    # if fn_name in OP_FUNC:
    #     return OP_FUNC[fn_name](fn_node, idx, ctx, def_var)
    if fn_name in OP_MAPPING:
        fn_name = OP_MAPPING[fn_name]
    main_table = fn_node.args[0].name
    split_name = "none"
    separator = ","
    if isinstance(fn_node.args[1], p.FnNode):
        split_node = fn_node.args[1]
        # distinct_count(split(join(列，",")))
        if split_node.name == "split":
            union_table = split_node.args[0].name.split(".")[0]
            col_name = split_node.args[0].name.split(".")[1]
            split_name = "split"
            separator = chr(int(split_node.args[1].name))
        # distinct_count(get_key(splitbykey(join(列，",")))) kv形式 a:a,b:b,c:c
        if split_node.name == "split_key":
            union_table = split_node.args[0].name.split(".")[0]
            col_name = split_node.args[0].name.split(".")[1]
            split_name = "split_key"
            separator = chr(int(split_node.args[1].name))
            kv_separator = chr(int(split_node.args[2].name))
        if split_node.name == "split_value":
            union_table = split_node.args[0].name.split(".")[0]
            col_name = split_node.args[0].name.split(".")[1]
            split_name = "split_value"
            separator = chr(int(split_node.args[1].name))
            kv_separator = chr(int(split_node.args[2].name))
        if split_name == "none":
            return False, None
    else:
        union_table = fn_node.args[1].name.split(".")[0]
        col_name = fn_node.args[1].name.split(".")[1]

    if len(fn_node.args) < 3:
        logging.info("op[{}]:table[{}]".format(fn_node.name, fn_node.args[0].name))
    cnt = fn_node.args[2].name
    # start, end, _, _, _ = parse_window_size(cnt)
    start, end, at_least, time_limit, max_size = parse_window_size(cnt)
    ok, union_window = ctx.get_window(main_table, union_table, '1-M')
    # ok, key, wname = ctx.get_window_by_table_and_other_table(main_table, union_table)
    if not ok:
        logger.warning("fail to find window with main table %s and union table %s", main_table, union_table)
        return False, None
    wname = union_window.wname
    key = union_window.get_window_key()
    if int(at_least) == 0:
        wc = model.WindowConfig(wname, main_table, union_table, None, None, max_size, None, time_limit,
                                main_table + "_output", None, None)
    else:
        wc = model.WindowConfig(wname, main_table, union_table, None, None, max_size, at_least, time_limit,
                            main_table + "_output", None, None)
    union_window.merge_window(wc)
    f_name = "f_%s_%s_%s_%s_%s" % (main_table, union_table, col_name, fn_name, idx)
    if fn_name == "top3frequency":
        range_code = "range(get_keys(sort_by_value({})), 0, 3)"
        window_data = "{}.{}[{}:{}]".format(wname, col_name, start, end)
        column_join = "join(\"{}\", \"NULL\", {})".format(separator, window_data)
        # if split_name != "none":

        if split_name == "none":
            # column_join = window_data
            temp_window = "{}[{}:{}]".format(wname, start, end)
            split_code = ctx.group_by_function(temp_window, "\"{}\"".format(col_name))
            split_code = "map({}, x->count_of_window(x))".format(split_code)
        elif split_name == "split_key":
            split_code = "map(group_by(split({}, \"{}\"), x -> first(split(x, \"{}\"))), x -> count(x))".format(column_join, separator, kv_separator)
            # just_code = "map(group_by(split({}, \"{}\"), x -> first(split(x, \"{}\"))), x -> count(x))".format(column_join, separator, kv_separator)
        else:
            split_code = "map(group_by(split({}, \"{}\"), x -> x), x->count(x))".format(column_join, separator)
            # just_code = "map(group_by(split({}, \"{}\"), x -> x), x->count(x))".format(column_join, separator)
        range_code = range_code.format(split_code)
        range_code = ctx.join_function(",", "NULL", range_code)

        if range_code != "":
            code = "{} = column({})".format(f_name, range_code)
            ctx.append_code(key, code)
            ctx.put_var(fn_node.get_id(), f_name)

        output_code = "{}.{}[0]".format(ctx.feature_window.wname, f_name)
        output_code = ctx.split_function(output_code, ",")
        # continuous discrete
        if def_var:
            # ctx.add_sign_code(f_name, "continuous", output_code)
            ctx.add_sign_code(f_name, "discrete", output_code)
            op_var.f_name = f_name
            op_var.code = output_code
            return True, op_var
        else:
            op_var.code = output_code
            op_var.f_name = f_name
            return True, op_var

    feature_window = "{}.{}[{}:{}]".format(wname, col_name, start, end)
    feature_col = "{}.{}".format(wname, col_name)
    if def_var:
        if split_name == "none":
            code = feature_window
        elif split_name == "split_key":
            code = "get_keys(splitbykey(join(\"{}\", \"NULL\", {}), \"{}\", \"{}\"))".format(separator, feature_window, separator, kv_separator)
            # code = "{} = column({}())".format(f_name, fn_name, separator, wname, col_name, start, end, separator, kv_separator)
        elif split_name == "split_value":
            code = "foreach(get_values(splitbykey(join(\"{}\", \"NULL\", {}), \"{}\", \"{}\")), x -> double(x))".format(separator, feature_window, separator, kv_separator)
            # code = "{} = column({}(foreach(get_values(splitbykey(join(\"{}\", \"NULL\", {}.{}[{}:{}]), \"{}\", \"{}\")), x -> double(x))))".format(f_name, fn_name, separator, wname, col_name, start, end, separator, kv_separator)
        else:
            code = "split(join(\"{}\", \"NULL\", {}), \"{}\")".format(separator, feature_window, separator)
            # code = "{} = column({}(split(join(\"{}\", \"NULL\", {}.{}[{}:{}]), \"{}\")))".format(f_name, fn_name, separator, wname, col_name, start, end, separator)

        if fn_name == "count_of_window":
            bool_code = "{}[1] != null".format(feature_col)
            res1_code = "{}({}[1], {})".format(fn_name, feature_col, code)
            res2_code = "null"
            code = ctx.if_function(bool_code, res1_code, res2_code)
        else:
            code = "{}({})".format(fn_name, code)

        code = "{} = column({})".format(f_name, code)

        # logging.info(code)
        ctx.append_code(key, code)
        ctx.put_var(fn_node.get_id(), f_name)
        if fn_node.name in CONTINUOUS_FEAT_CATEGORY:
            ctx.add_continuous(f_name)
        else:
            ctx.add_discrete(f_name)
        op_var.f_name = f_name
        return True, op_var
    else:
        op_var.code = "%s(%s.%s[%s:%s])" % (fn_name, wname, col_name, start, end)
        return True,  op_var

def handle_original_op(fn_node, idx, ctx, def_var = True, label = None):
    """
    original(sample.user_id)
    """
    op_var = model.OpVar()
    wname = ctx.sample_table + "_window"
    # if ctx.var_exist(fn_node.get_id):
    #     f_name = ctx.get_var(fn_node.get_id())
    arg = fn_node.args[0].name
    tname = arg.split('.')[0]
    assert tname == ctx.sample_table
    col_name = arg.split(".")[1]
    f_name = "f_%s_%s_%s" % (fn_node.name, col_name, idx)
    ok, key, wn = ctx.get_only_table_window(tname)
    if not ok:
        wc = model.WindowConfig(wname, tname, None, [], "", None, None, None, tname+"_output", None)
        wn, key = ctx.add_window(wc)
    # add sign in window
    if wn == ctx.feature_window.wname:
        if col_name == ctx.label:
            ctx.add_label(col_name)
        else:
            ok, feat_type = ctx.tables[tname].get_feat_type(col_name)
            # ok, column = ctx.tables[tname].get_column(col_name)
            # if column.ty_ == "date":
            #     col_name = ctx.date_function(col_name)
            if feat_type == 1:
                ctx.add_continuous(col_name)
            else:
                ctx.add_discrete(col_name)
        return True, op_var
    else:
        # ok, column = ctx.tables[tname].get_column(col_name)
        var = "{}.{}[0]".format(wn, col_name)
        # if column.ty_ == "date":
        #     var = ctx.date_function(var)
        code = "{} = column({})".format(f_name, var)
        ctx.append_code(key, code)
        if col_name == ctx.label:
            ctx.add_label(f_name)
        else:
            ok, feat_type = ctx.tables[tname].get_feat_type(col_name)
            if feat_type == 1:
                ctx.add_continuous(f_name)
            else:
                ctx.add_discrete(f_name)
            # if column.ty_ == "timestamp" or feat_type == 1:
            #     ctx.add_continuous(f_name)
            # else:
            #     ctx.add_discrete(f_name)
    # if ctx.output_key == "":
    #     config = ctx.config
    #     value = config['target_entity_index']
    #     table = config['target_entity']
    #     # 表名 + outputkey + 默认列名
    #     feature = "%s_outputkey_%s_%s" % (table, table, value)
    #     code = "%s = output(%s.%s[0])" % (feature, wname, value)
    #     ctx.append_code(key, code)
    #     ctx.output_key = feature
    op_var.f_name = f_name
    return True, op_var

def single_function(fn_node, idx, ctx, function):
    op_var = model.OpVar()
    arg = fn_node.args[0].name
    tname = arg.split('.')[0]
    col_name = arg.split('.')[1]
    f_name = "f_%s_%s" % (fn_node.name, idx)
    # if ctx.var_exist(fn_node.get_id()):
    #     var = ctx.get_var(fn_node.get_id())
    #     return True, var
    ctx.put_var(fn_node.get_id(), f_name)
    wname = tname + "_window"
    ok, key, wn = ctx.get_only_table_window(tname)
    if not ok:
        wc = model.WindowConfig(wname, tname, None, [], "", None, None, None, tname + "_output", None)
        wn, key = ctx.add_window(wc)
    wname = wn
    if wn == ctx.feature_window.wname:
        if col_name == ctx.label:
            ctx.add_label(col_name)
        else:
            ok, feat_type = ctx.tables[tname].get_feat_type(col_name)
            if feat_type == 1:
                ctx.add_continuous(col_name)
            else:
                ctx.add_discrete(col_name)
        return True, op_var
    else:
        if function != None:
            feature = "%s.%s[0]" % (wname, col_name)
            code = "%s = column(%s)" % (f_name, function.format(feature))
            ctx.append_code(key, code)
            ctx.counter += 1
            # out_name = "%s_%s" % (f_name, ctx.counter)
            # code = "%s = discrete(%s.%s[0])" % (out_name, f_name)
            # ctx.output_window.append(code)
            ctx.add_discrete(f_name)
            op_var.f_name = f_name
            return True, op_var
        code = "%s = column(%s.%s[0])" % (f_name, wn, col_name)
        ctx.append_code(key, code)
        if col_name == ctx.label:
            ctx.add_label(f_name)
        else:
            ok, feat_type = ctx.tables[tname].get_feat_type(col_name)
            if feat_type == 1:
                ctx.add_continuous(f_name)
            else:
                ctx.add_discrete(f_name)
    op_var.f_name = f_name
    return True, op_var

def handle_sample_table_ops(fn_node, idx, ctx, def_var = True):
    op_var = model.OpVar()
    BINARY_OPS = {
        "divide":"{} / {}",
        # "add":"{} + {}",
        # "multiply":"{} * {}",
        "subtract":"{} - {}",
        "isin":"is_in_window(string({}), {})",
        "timediff":"timediff(timestamp({}), timestamp({}))"
        }

    MULTIARY_OPS = {
        "add": " + ",
        "multiply": " * "
    }

    UNARY_OPS = {"log": "log({0})",
                 "dayofweek": "dayofweek(timestamp({0}))",
                 "isweekday": "if(1 < dayofweek(timestamp({0})) and dayofweek(timestamp({0})) < 7, 1, 0)",
        "hourofday": "hour(timestamp({0}))"}

    # if fn_node.name.startswith("original"):
    #     return handle_original_op(fn_node, idx, ctx, def_var)

    function = fn_node.name
    arg = fn_node.args[0].name
    if ctx.sample_table in arg:
        if function == "isweekday":
            return single_function(fn_node, idx, ctx, UNARY_OPS[function])
        if function == "hourofday":
            return single_function(fn_node, idx, ctx, UNARY_OPS[function])
        if function == "dayofweek":
            return single_function(fn_node, idx, ctx, UNARY_OPS[function])
        if function == "log":
            return single_function(fn_node, idx, ctx, UNARY_OPS[function])
    ok, key, wn = ctx.get_only_table_window(ctx.sample_table)
    if not ok:
        wc = model.WindowConfig(ctx.feature_window.wname, ctx.sample_table, None, [], "", None, None, None,
                                ctx.sample_table + "_output", None)
        wn, key = ctx.add_window(wc)
    vars = []
    for arg in fn_node.args:
        ctx.counter += 1
        if isinstance(arg, p.FnNode):
            if ctx.var_exist(arg.get_id()):
                id = arg.get_id()
                temp_op_var = ctx.get_op_var(id)
                var = temp_op_var.f_name
                if temp_op_var.code != None:
                    vars.append(temp_op_var.code)
                else:
                    vars.append("%s.%s[0]" % (ctx.feature_window.wname, var))
            else:
                arg.disable_gen_feature()
                if arg.name in SPLIT_OP:
                    ok, var = handle_all_ops(arg, idx, ctx, False)
                else:
                    ok, var = handle_all_ops(arg, idx, ctx, True)
                ctx.put_op_var(arg.get_id(), var)
                if not ok:
                    return False, None
                if var.code != None:
                    vars.append(var.code)
                else:
                    vars.append("%s.%s[0]" % (ctx.feature_window.wname, var.f_name))
        else:
            # 和主表有关
            # table = ctx.sample_table
            f_name = "f_%s_%s_%s" % (fn_node.name, idx, ctx.counter)
            table_name = arg.name.split(".")[0]
            col_name = arg.name.split(".")[1]
            if table_name == ctx.sample_table:
                code = "%s = column(%s.%s[0])" % (f_name, wn, col_name)
                ctx.append_code(key, code)
                vars.append("%s.%s[0]" % (ctx.feature_window.wname, f_name))

    if fn_node.name in BINARY_OPS and len(vars) == 2:
        f_name = "f_%s_%s" % (fn_node.name, idx)
        if fn_node.name in CONTINUOUS_FEAT_CATEGORY:
            code = "%s = continuous(%s)" % (f_name, BINARY_OPS[fn_node.name].format(vars[0], vars[1]))
        else:
            code = "%s = discrete(%s)" % (f_name, BINARY_OPS[fn_node.name].format(vars[0], vars[1]))
        ctx.feature_window.append(code)
        op_var.f_name = f_name
        return True, op_var
    elif fn_node.name in UNARY_OPS:
        f_name = "f_%s_%s" % (fn_node.name, idx)
        if fn_node.name == "log":
            code = "%s = continuous(%s)" % (f_name, UNARY_OPS[fn_node.name].format(vars[0]))
        else:
            code = "%s = discrete(%s)" % (f_name, UNARY_OPS[fn_node.name].format(vars[0]))

        ctx.feature_window.append(code)
        op_var.f_name = f_name
        return True, op_var
    elif fn_node.name in MULTIARY_OPS and len(vars) >= 2:
        f_name = "f_%s_%s" % (fn_node.name, idx)
        code = "%s = continuous(%s)" % (f_name, MULTIARY_OPS[fn_node.name].join(vars))
        ctx.feature_window.append(code)
        op_var.f_name = f_name
        return True, op_var
    else:
        # combine 支持列
        f_name = "f_%s_%s" % (fn_node.name, idx)
        code = "%s = discrete(%s(%s))" % (f_name, fn_node.name, ",".join(vars))
        ctx.feature_window.append(code)
        op_var.f_name = f_name
        return True, op_var

def handle_split(fn_node, idx, ctx, def_var):
    """
    split(main.as,44)
    split_key(main.ks,44,58)
    isin(multi_last_value(main,table_3.s1,10:0),split_key(main.kn,44,58))
    isin(s2,split_key(multi_last_value(main,table_3.ks,10:0),44,58))
    split(multi_direct(main,table_1.astr),44)
    :param fn_node:
    :param idx:
    :param ctx:
    :param def_var:
    :return:
    """
    op_var = model.OpVar()
    split = fn_node.name
    arg = fn_node.args[0]
    f_name = "f_%s_%s" % (fn_node.name, idx)
    if isinstance(fn_node.args[0], p.FnNode):
        if ctx.var_exist(arg.get_id()):
            var_op = model.OpVar()
            var_op.f_name = ctx.get_var(arg.get_id())
            # vars.append("%s.%s[0]" % (ctx.feature_window.wname, var))
        else:
            # nested op, do not generate extra feature
            arg.disable_gen_feature()
            ok, var_op = handle_all_ops(arg, idx, ctx)
            if not ok:
                return False, None
            # vars.append("%s.%s[0]" % (ctx.feature_window.wname, var))
        # f_name = var
        w_name = "{}.{}[0]".format(ctx.feature_window.wname, var_op.f_name)
        separator = chr(int(fn_node.args[1].name))
        if split == "split":
            code = ctx.split_function(w_name, separator)
        elif split == "split_key":
            kv_separator = chr(int(fn_node.args[2].name))
            code = ctx.get_keys_function(ctx.split_key_function(w_name, separator, kv_separator))
        else:
            kv_separator = chr(int(fn_node.args[2].name))
            code = ctx.get_values_function(ctx.split_key_function(w_name, separator, kv_separator))
        op_var.code = code
        if def_var:
            ctx.add_sign_code(f_name, "discrete", code)
        return True, op_var
    else:
        arg = fn_node.args[0].name
        tname = arg.split('.')[0]
        wname = tname + "_window"
        col_name = arg.split(".")[1]
        ok, key, wn = ctx.get_only_table_window(tname)
        if not ok:
            wc = model.WindowConfig(wname, tname, None, [], "", None, None, None, tname+"_output", None)
            wn, key = ctx.add_window(wc)
        code = "%s = column(%s.%s[0])" % (f_name, wn, col_name)
        ctx.append_code(key, code)
    separator = chr(int(fn_node.args[1].name))
    if split == "split":
        ctx.add_split(f_name, separator)
    elif split == "split_key":
        kv_separator = chr(int(fn_node.args[2].name))
        ctx.add_split_key(f_name, separator, kv_separator)
    else:
        kv_separator = chr(int(fn_node.args[2].name))
        ctx.add_split_value(f_name, separator, kv_separator)
    op_var.f_name = f_name
    op_var.code = ctx.feature_window.fcodes[-1]
    p1 = re.compile(r'[(](.*)[)]', re.S)
    op_var.code = re.findall(p1, op_var.code)[0]
    ctx.put_var(fn_node.get_id(), f_name)
    return True, op_var

def handle_all_ops(fn_node, idx, ctx, def_var = True):
    func_map = {"window_ratio": handle_window_top1_ratio,
                "window_top1_ratio": handle_window_top1_ratio,
                # "multi_top3frequency": handle_top_freq,
                "multi_last_value": handle_last_join,
                "multi_direct": handle_left_join,
                "binary_label": handle_label,
                "regression_label": handle_label,
                "window_count": handle_window_count,
                "window_unique_count": handle_window_count,
                "original": handle_original_op}

    if fn_node.name in func_map:
        return func_map[fn_node.name](fn_node, idx, ctx, def_var)
    if fn_node.name.startswith("multi_"):
        return handle_multi_to_feql(fn_node, idx, ctx, def_var)
    if fn_node.name in WINDOW_OP:
        return handle_window_count(fn_node, idx, ctx, def_var)
    if fn_node.name in SPLIT_OP:
        return handle_split(fn_node, idx, ctx, def_var)
    return handle_sample_table_ops(fn_node, idx, ctx, def_var)

