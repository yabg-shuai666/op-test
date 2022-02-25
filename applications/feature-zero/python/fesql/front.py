# -*- coding: utf-8 -*-
# fzop front

from . import convert as sqlConvert
import logging

# todo 变量复用 lastjoin的时间窗
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                    datefmt='%a, %d %b %Y %H:%M:%S')
logger = logging.getLogger(__name__)


def get_relation(from_table, to_table, type, context):
    relations = context.relation
    for e in relations:
        if from_table == e['from_entity'] and to_table == e['to_entity'] and type.lower() == str(e['type']).lower():
            return e
    return None


def get_default_value(fetype):
    value = None
    if fetype == "string":
        value = "\"0\""
    elif fetype == "boolean":
        value = "false"
    elif fetype == "int" or fetype == "smallint":
        value = "0"
    elif fetype == "bigint":
        value = "0L"
    elif fetype == "double":
        value = "0.0D"
    elif fetype == "float":
        value = "0.0"
    else:
        logging.warning("%s is not supported for label" % (fetype))
    return value


def get_col_type(field, table, config):
    for entity in config["entity_detail"][table]["features"]:
        if entity["id"] == "{}.{}".format(table, field):
            return str(entity["feature_type"]).lower()
    return None


def get_data_type(field, table, config):
    for value in config["entity_detail"][table]["features"]:
        if value['id'] == "{}.{}".format(table, field):
            return value['data_type']
    return None


def gen_origin(fn_node, row_num, code_segment, context):
    '''
    original(sample.user_id)
    original(label(sample.user_id))
    :param fn_node:
    :param row_num:
    :param code_segment:
    :param context:
    :return:
    '''
    if fn_node.args[0].type == "var":
        table, col = update_table_var(fn_node.args[0])
    if fn_node.args[0].type == "function":
        fn_node.args[0].disable_gen_feature()
        state = convert_ops(fn_node.args[0], row_num, code_segment, context)
        table = state.table
        col = state.col
    col_code = "`{}`".format(col)
    rename_col = "{}_{}_{}_{}".format(table, col, fn_node.name, row_num)
    feature_code = "{} as {}".format(col_code, rename_col)
    select_node = context.get_select_node(table)
    select_node = context.add_select_node(select_node)
    select_node.feature_code.append(feature_code)
    select_node.update_output_key_id(context.get_output_key_id())
    select_node.update_key_id(context.key_id)
    gen_fe(rename_col, table, col, context)
    state = context.get_op_state(True, table, col, row_num, feature_name=rename_col)
    return state


# 输入列名，表名，
def gen_fe(input, table, col, context):
    type_feature = get_data_type(col, table, context.config)
    if type_feature == "ContinueNum":
        sign = "continuous"
    else:
        sign = "discrete"
    code = "{}_{} = {}({}.{}[0])".format(sign, input, sign, context.fe_window_name, input)
    context.add_sign(code)


def add_sign_code(input, context, sign):
    code = "{}_{} = {}({}.{}[0])".format(sign, input, sign, context.fe_window_name, input)
    context.add_sign(code)


def gen_continuous_code(output, input):
    code = "{} = continuous({})".format(output, input)
    return code


def gen_discrete_code(output, input):
    '''
    只生成离散签名的代码
    :param output: 输出列名
    :param input: 输入列名
    :return:
    '''
    code = "{} = discrete({})".format(output, input)
    return code


def gen_label(fn_node, row_num, code_segment, context):
    '''
    original(label(sample.user_id))
    regression_label(multi_direct(flattenRequest,action.actionValue))
    :param fn_node:
    :param row_num:
    :param code_segment:
    :param context:
    :return:
    '''
    # logger.info("label")
    if fn_node.args[0].type == "var":
        table, col = update_table_var(fn_node.args[0])
    if fn_node.args[0].type == "function":
        # label op needs disable gen feature?
        fn_node.args[0].disable_gen_feature()
        state = convert_ops(fn_node.args[0], row_num, code_segment, context)
        table = state.table
        col = state.col
        feature_name = state.feature_name
    output = feature_name
    label = fn_node.name
    value = get_default_value(get_col_type(col, table, context.config))
    feature_code = "label_{} = {}(ifnull({}.{}[0], {}))".format(output, label, context.fe_window_name, output, value)
    # TODO(hw): check this
    context.add_sign(feature_code)
    state = context.get_op_state(True, table, col, row_num)
    return state


def gen_window(fn_node, row_num, code_segment, context):
    start, end, max_size = 0, 0, ''

    if fn_node.args[0].type == "var":
        table, time_col = update_table_var(fn_node.args[0])
    if fn_node.args[1].type == "var":
        table, key_col = update_table_var(fn_node.args[1])
    if fn_node.args[2].type == "var":
        start, end, max_size = update_time_window(fn_node.args[2])

    col_code = ""
    if fn_node.args[3].type == "var":
        table, col = update_table_var(fn_node.args[3])
        col_code = "`{}`".format(col)
    if fn_node.args[3].type == "function":
        fn_node.arg[3].disable_gen_feature()
        state = convert_ops(fn_node.args[3], row_num, code_segment, context)
        table = state.table
        col = state.col
        col_code = state.feature_name

    # window_count is count(col[0], window.col) , it should be `count_where` in fesql
    functionmap = {"window_unique_count": "distinct_count", "window_sum": "sum", "window_avg": "avg",
                   "window_min": "min", "window_max": "max", "window_top1_ratio": "fz_top1_ratio",
                   "window_count": "count_where"}

    if max_size == '':
        ttl = '{}|{}'.format(end, 0)
    else:
        ttl = '{}|{}'.format(end, max_size)
    context.update_table_index(table, [key_col], time_col, ttl)

    window_name = "{}_{}_{}_{}_{}_{}".format(table, key_col, time_col, start, end, max_size)
    rename_col = "{}_{}_{}_{}".format(table, col, fn_node.name, row_num)

    if "window_unique_count" in fn_node.name:
        feature_code = "distinct_count({}) over {}".format(
            col_code,
            window_name)
    elif "window_count" in fn_node.name:
        feature_code = "case when !isnull(at({}, 0)) over {} " \
                       "then count_where({}, {} = at({}, 0)) over {} " \
                       "else null end".format(col_code, window_name,
                                              col_code, col_code, col_code, window_name)
    elif "window_top1_ratio" in fn_node.name:
        feature_code = "{}({}) over {}".format(functionmap[fn_node.name], col_code, window_name)
    else:
        feature_code = "{}({}) over {}".format(functionmap[fn_node.name], col_code, window_name)
    feature_code = "{} as {}".format(feature_code, rename_col)
    select_node = context.get_select_node(table)

    window_node = context.get_window_node(window_name, table, [key_col], time_col, start, end, max_size)
    select_node.add_window_node(window_node)
    select_node = context.add_select_node(select_node)
    select_node.feature_code.append(feature_code)
    select_node.update_output_key_id(context.get_output_key_id())
    select_node.update_key_id(context.key_id)

    if "split" in fn_node.id_:
        gen_fe(rename_col, table, col, context)
    else:
        add_sign_code(rename_col, context, "continuous")
    state = context.get_op_state(True, table, col, row_num, rename_col)
    return state


def gen_multi(fn_node, row_num, code_segment, context):
    if fn_node.args[0].type == "var":
        table = fn_node.args[0].name
    col_code = ""
    if fn_node.args[1].type == "var":
        union_table, col = update_table_var(fn_node.args[1])
        col_code = "`{}`".format(col)
    if fn_node.args[1].type == "function":
        fn_node.args[1].disable_gen_feature()
        state = convert_ops(fn_node.args[1], row_num, code_segment, context)
        union_table = state.table
        col = state.col
        col_code = state.feature_name
    if fn_node.args[2].type == "var":
        start, end, max_size = update_time_window(fn_node.args[2])
    type = '1-M'
    relation = get_relation(table, union_table, type, context)
    if relation is None:
        state = context.get_op_state(False, union_table, col, row_num)
        return state

    time_col = relation['to_entity_time_col']
    key_col = relation['to_entity_keys']

    functionmap = {}
    functionmap["multi_unique_count"] = "distinct_count"
    functionmap["multi_count"] = "count"
    functionmap["multi_sum"] = "sum"
    functionmap["multi_avg"] = "avg"
    functionmap["multi_min"] = "min"
    functionmap["multi_max"] = "max"
    functionmap["multi_std"] = "std"
    functionmap["multi_top3frequency"] = "fz_topn_frequency"

    window_name = "{}_{}_{}_{}_{}_{}".format(union_table, "_".join(key_col), time_col, start, end, max_size)
    rename_col = "{}_{}_{}_{}".format(union_table, col, fn_node.name, row_num)

    if "multi_unique_count" in fn_node.name:
        feature_code = "distinct_count({}) over {}".format(
            col_code,
            window_name)
    elif "multi_count" == fn_node.name:
        feature_code = "case when !isnull(at({}, 1)) over {} then count({}) over {} else null end".format(col_code,
                                                                                                          window_name,
                                                                                                          col_code,
                                                                                                          window_name)
    elif fn_node.name == "multi_top3frequency":
        feature_code = "{}({}, 3) over {}".format(functionmap[fn_node.name], col_code, window_name)
    else:
        feature_code = "{}({}) over {}".format(functionmap[fn_node.name], col_code, window_name)
    feature_code = "{} as {}".format(feature_code, rename_col)
    # select_scope = context.add_select(table, feature_code)
    # 获取select
    select_scope = context.get_select_node(table)

    # 获取窗口
    window_clause = context.get_window_node(window_name, table, key_col, time_col, start, end, max_size)
    relation = context.get_table_relation(table, union_table, '1-M')
    if not relation:
        state = context.get_op_state(False, union_table, col, row_num)
        return state
    union_clause = context.get_union_node(relation, table, union_table, context.key_id)
    union_clause.update_schema()
    # union表的时候，全局key的名字可能会被修改，因为需要对齐schema
    if union_clause.new_key_id != "":
        select_scope.key_id = union_clause.new_key_id
    else:
        select_scope.key_id = union_clause.key_id

    # union_clause = window_clause.add_union_clause(relation, union_table, 'mcuid', table1_info, table2_info)

    # 更新select和window
    window_clause.add_union_node(union_clause)
    select_scope.add_window_node(window_clause)

    select_scope = context.add_select_node(select_scope)
    select_scope.feature_code.append(feature_code)

    # key_code = union_clause.get_union_key()
    select_scope.update_output_key_id(context.get_output_key_id())
    select_scope.update_key_id(context.key_id)
    select_scope.table_code = union_clause.gen_table1()

    input_code = ""
    if fn_node.name == "multi_top3frequency":
        input_code = "{}.{}[0]".format(context.fe_window_name, rename_col)
        input_code = context.split_function(input_code, ",")
        output_code = "discrete_{}".format(rename_col)
        code = gen_discrete_code(output_code, input_code)
        context.add_sign(code)
        # rename_col = output_code
    else:
        if "split" in fn_node.id_:
            gen_fe(rename_col, table, col, context)
        else:
            add_sign_code(rename_col, context, "continuous")
    state = context.get_op_state(True, table, col, row_num, rename_col, input_code)
    return state


def gen_join(fn_node, row_num, code_segment, context):
    '''
    join节点是列数增加，所以它是依附于主表，select的id由主表名确定即可

    multi_direct(main,table_1.s1)
    multi_direct(main,table_1.t1)
    multi_direct(main,table_1.t2)

    multi_last_value(main,table_1.c1,10:0)
    multi_last_value(main,table_1.c2,10:0)
    multi_last_value(main,table_1.d1,10:0)
    multi_last_value(main,table_1.d2,10:0)
    :param fn_node:
    :param row_num:
    :param code_segment:
    :param context:
    :return:
    '''
    table1 = fn_node.args[0].name
    table2, col = update_table_var(fn_node.args[1])
    rename_col = "{}_{}_{}_{}".format(table2, col, fn_node.name, row_num)
    col_code = "`{}`".format(col)

    select_node = context.get_select_node(table1)
    start = None
    if fn_node.name == "multi_last_value":
        start, end, max_size = update_time_window(fn_node.args[2])
        tp = 'slice'
    else:
        tp = '1-1'

    lastjoin_node = context.get_lastjoin_node(table1, table2, tp)
    if start != None:
        lastjoin_node.start = start
        lastjoin_node.end = end
        lastjoin_node.max_size = max_size
    select_node.add_lastjoin_node(lastjoin_node)
    lastjoin_node.change_table2_name()
    select_node = context.add_select_node(select_node)

    feature_code = "`{}`.{} as {}".format(lastjoin_node.table2, col_code, rename_col)
    select_node.feature_code.append(feature_code)
    key_code = "{}.{}".format(table1, context.key_id)
    select_node.update_output_key_id(context.get_output_key_id())
    select_node.update_key_id(key_code)
    if fn_node.is_gen_feature():
        gen_fe(rename_col, table2, col, context)
    state = context.get_op_state(True, table2, col, row_num, rename_col)
    return state


def update_table_var(var_symbol):
    '''
    :param var_symbol: 输入类型是node，不是字符串
    :return:
    '''
    args1 = var_symbol.name
    table = args1.split(".")[0]
    col = args1.split(".")[1]
    # col = "`{}`".format(col)
    return table, col


def update_time_window(time_symbol):
    parts = time_symbol.name.split(":")
    if len(parts) == 3:
        start = parts[2]
        end = parts[0]
        max_size = parts[1]
    if len(parts) == 2:
        start = parts[1]
        end = parts[0]
        max_size = ""
    return start, end, max_size


def gen_binary(fn_node, row_num, code_segment, context):
    op_code = {
        "divide": "{} / {}",
        "add": "{} + {}",
        "multiply": "{} * {}",
        "subtract": "{} - {}",
    }
    multi_op_code = {
        "add": " + ",
        "multiply": " * ",
        "combine": ", "
    }
    vars = []
    rename_cols = []
    for arg in fn_node.args:
        if arg.type == "function":
            arg.disable_gen_feature()
            state = convert_ops(arg, row_num, code_segment, context)
            table = state.table
            col = state.col
            rename_col = state.feature_name
            rename_cols.append(rename_col)
            v = "{}.{}[0]".format(context.fe_window_name, rename_col)
            vars.append(v)
        if arg.type == "var":
            table, col = update_table_var(arg)
            col_code = "`{}`".format(col)
            rename_col = "{}_{}_{}_{}".format(table, col, fn_node.name, row_num)
            feature_code = "{} as {}".format(col_code, rename_col)
            select_node = context.get_select_node(table)
            select_node = context.add_select_node(select_node)
            select_node.feature_code.append(feature_code)
            vars.append("{}.{}[0]".format(context.fe_window_name, rename_col))
            rename_cols.append(rename_col)
    if len(vars) < 2:
        state = context.get_op_state(False, None, None, row_num)
        return state
    if len(vars) == 2:
        code = op_code[fn_node.name].format(vars[0], vars[1])
    else:
        if fn_node.name in multi_op_code:
            code = multi_op_code[fn_node.name].join(vars)
        else:
            state = context.get_op_state(False, None, None, row_num)
            return state
    if len(rename_cols) == 2:
        sign_name = "continuous_{}_{}_{}".format(rename_cols[0], fn_node.name, rename_cols[1])
    else:
        sign_name = "continuous_{}_{}".format(fn_node.name, "_".join(rename_cols))
    code = gen_continuous_code(sign_name, code)
    context.add_sign(code)

    state = context.get_op_state(True, table, col, row_num)
    return state


def gen_function(fn_node, row_num, code_segment, context):
    '''
    isweekday(multi_direct(main,table_1.t2))
    isweekday(multi_last_value(main,table_1.t2,10:0))
    isweekday(main.t2)

    dayofweek(multi_last_value(main,table_1.t2,10:0))
    dayofweek(multi_last_value(main,table_1.t1,10:0))
    dayofweek(main.t2)

    isin(multi_last_value(main,table_1.d2,10:0),multi_top3frequency(main,table_2.d2,1d:1000:0s))
    isin(multi_last_value(main,table_1.d2,10:0),split_key(multi_last_value(main,table_1.kn,10:0),44,58))
    isin(main.d1,multi_top3frequency(main,split_key(table_1.ks,44,58),1d:1000:0s))

    combine(main.d2,split_key(main.ks,44,58))
    combine(multi_direct(main,table_1.d1),multi_direct(main,table_1.s1),multi_last_value(main,table_1.d2,10:0))
    :param fn_node:
    :param row_num:
    :param code_segment:
    :param context:
    :return:
    '''
    # 放到fe里面做
    fe_code = {
        "isin": "is_in_window(string({}), {})",
        "timediff": "timestampdiff(timestamp({}), timestamp({}))",
        "combine": "combine({})"
    }
    # 放到sql里面做
    sql_code = {
        "log": "log({0})",
        "dayofweek": "dayofweek(timestamp({0}))",
        "isweekday": "case when 1 < dayofweek(timestamp({0})) and dayofweek(timestamp({0})) < 7 then 1 else 0 end",
        "hourofday": "hour(timestamp({0}))"
    }
    vars = []
    rename_cols = []
    for arg in fn_node.args:
        if arg.type == "function":
            # nested op should not gen feature, all convert_ops in gen_xxx should disable it.
            arg.disable_gen_feature()
            state = convert_ops(arg, row_num, code_segment, context)
            table = state.table
            col = state.col
            rename_col = state.feature_name
            rename_cols.append(rename_col)
            if state.feature_code != "":
                v = state.feature_code
            else:
                v = "{}.{}[0]".format(context.fe_window_name, rename_col)
            vars.append(v)
        if arg.type == "var":
            table, col = update_table_var(arg)
            col_code = "`{}`".format(col)
            rename_col = "{}_{}_{}_{}".format(table, col, fn_node.name, row_num)
            if fn_node.name in sql_code:
                feature_code = "{} as {}".format(sql_code[fn_node.name].format(col_code), rename_col)
                gen_fe(rename_col, table, col, context)
            else:
                feature_code = "{} as {}".format(col_code, rename_col)
            select_node = context.get_select_node(table)
            select_node = context.add_select_node(select_node)
            select_node.feature_code.append(feature_code)
            select_node.update_output_key_id(context.get_output_key_id())
            select_node.update_key_id(context.key_id)
            vars.append("{}.{}[0]".format(context.fe_window_name, rename_col))
            rename_cols.append(rename_col)

    if fn_node.name in fe_code:
        # if fn_node.name == "isin":
        #     code = fe_code[fn_node.name].format(vars[0], vars[1])
        #     sign_name = "discrete_{}_{}_{}".format(rename_cols[0], fn_node.name, rename_cols[1])
        #     code = gen_discrete_code(sign_name, code)
        #     context.add_sign(code)
        if fn_node.name == "timediff" or fn_node.name == "isin":
            code = fe_code[fn_node.name].format(vars[0], vars[1])
            sign_name = "discrete_{}_{}_{}".format(rename_cols[0], fn_node.name, rename_cols[1])
            code = gen_discrete_code(sign_name, code)
            context.add_sign(code)
        if fn_node.name == "combine":
            code = fe_code[fn_node.name].format(", ".join(vars))
            sign_name = "discrete_{}_{}_cols_{}".format(fn_node.name, len(vars), row_num)
            code = gen_discrete_code(sign_name, code)
            context.add_sign(code)

    state = context.get_op_state(True, table, col, row_num)
    return state


def gen_split(fn_node, row_num, code_segment, context):
    '''
    split_key(multi_last_value(main,table_1.kn,10:0),44,58)
    multi_unique_count(main,split_key(table_1.kn,44,58),1d:1000:0s)
    split_key(main.ks,44,58)
    split(multi_direct(main,table_1.ai),44)
    multi_top3frequency(main,split_key(table_1.kn,44,58),1d:1000:0s)

    :param fn_node:
    :param row_num:
    :param code_segment:
    :param context:
    :return:
    '''
    logging.info(fn_node.name)
    args0 = fn_node.args[0]
    split = fn_node.name
    separator = chr(int(fn_node.args[1].name))
    kv_separator = ""
    if len(fn_node.args) == 3:
        kv_separator = chr(int(fn_node.args[2].name))

    # 有两种情况
    # 一种是split主表，需要当做origin的操作，为防止多余操作，可以直出字段，让fe做split。不需要sql，split然后再join的多余操作
    # 另一种是split副表，直接对字段做split，返回含split的code multi_unique_count(main,split_key(table_1.kn,44,58),1d:1000:0s)
    if args0.type == "var":
        table, col = update_table_var(args0)
        col_code = "`{}`".format(col)
        # 主表的情况
        if table == context.get_main_table():
            rename_col = "{}_{}_{}".format(table, col, row_num)
            feature_code = "{} as {}".format(col_code, rename_col)
            select_node = context.get_select_node(table)
            select_node = context.add_select_node(select_node)
            select_node.feature_code.append(feature_code)
            select_node.update_output_key_id(context.get_output_key_id())
            select_node.update_key_id(context.key_id)
            code = "{}.{}[0]".format(context.fe_window_name, rename_col)
            if split == "split":
                code = context.split_function(code, separator)
            if split == "split_key":
                code = context.get_keys_function(context.split_key_function(code, separator, kv_separator))
            if split == "split_value":
                code = context.get_values_function(context.split_key_function(code, separator, kv_separator))
            split_code = code
            sign_name = "discrete_{}_{}".format(rename_col, fn_node.name)
            code = gen_discrete_code(sign_name, code)
            if fn_node.is_gen_feature():
                context.add_sign(code)
            state = context.get_op_state(True, table, col, row_num, feature_name=rename_col, feature_code=split_code)
            return state
        # 副表的情况
        else:
            code = col
            # "{}.{}".format(table, col)
            code = gen_sql_split_code(code, split, separator, kv_separator, context)
            state = context.get_op_state(True, table, col, row_num, code)
            return state

    # 需要拿到函数的返回字段，然后扔到fe做split
    if args0.type == "function":
        # convert_ops gen an extra sign, but replace it later
        fn_node.args[0].disable_gen_feature()
        state = convert_ops(fn_node.args[0], row_num, code_segment, context)
        rename_col = state.feature_name
        code = "{}.{}[0]".format(context.fe_window_name, rename_col)
        code = gen_fe_split_code(code, split, separator, kv_separator, context)
        sign_name = "discrete_{}_{}".format(rename_col, fn_node.name)
        code = gen_discrete_code(sign_name, code)
        # ~~todo 需要确定 删除的特征是否正确~~
        # already disable nested convert_ops gen extra sign, so add sign here, not replace
        if fn_node.is_gen_feature():
            context.add_sign(code)
        state = context.get_op_state(True, "", "", row_num, rename_col)
        return state


def gen_fe_split_code(code, split, separator, kv_separator, context):
    if split == "split":
        code = context.split_function(code, separator)
    if split == "split_key":
        code = context.get_keys_function(context.split_key_function(code, separator, kv_separator))
    if split == "split_value":
        code = context.get_values_function(context.split_key_function(code, separator, kv_separator))
    return code


def gen_sql_split_code(code, split, separator, kv_separator, context):
    if split == "split":
        code = context.fz_window_split_function(code, separator)
    if split == "split_key":
        code = context.fz_window_split_by_key_function(code, separator, kv_separator)
    if split == "split_value":
        code = context.fz_window_split_by_value_function(code, separator, kv_separator)
    return code


def convert_ops(fn_node, row_num, code_segment, context):
    # if fn_node.name in sqlConvert.LABEL_OP:
    logger.info("{}_{}".format(fn_node.name, row_num + 1))
    if context.is_reuse(fn_node):
        state = context.get_symbol_from_table(fn_node)
        return state

    if fn_node.name in sqlConvert.ORIGIN:
        state = gen_origin(fn_node, row_num, code_segment, context)
        context.add_symbol_to_table(fn_node, state)
        return state
    if fn_node.name in sqlConvert.LABEL_OP:
        state = gen_label(fn_node, row_num, code_segment, context)
        context.add_symbol_to_table(fn_node, state)
        return state
    if fn_node.name in sqlConvert.WINDOW_BASE_OP or fn_node.name in sqlConvert.WINDOW_ADVANCE_OP:
        state = gen_window(fn_node, row_num, code_segment, context)
        context.add_symbol_to_table(fn_node, state)
        return state
    if fn_node.name in sqlConvert.MULTI_BASE_OP or fn_node.name in sqlConvert.MULTI_ADVANCE_OP:
        state = gen_multi(fn_node, row_num, code_segment, context)
        context.add_symbol_to_table(fn_node, state)
        return state
    if fn_node.name in sqlConvert.JOIN_OP:
        state = gen_join(fn_node, row_num, code_segment, context)
        context.add_symbol_to_table(fn_node, state)
        return state
    if fn_node.name in sqlConvert.BINARY_OP:
        state = gen_binary(fn_node, row_num, code_segment, context)
        context.add_symbol_to_table(fn_node, state)
        return state
    if fn_node.name in sqlConvert.FUNCTION_OP:
        state = gen_function(fn_node, row_num, code_segment, context)
        context.add_symbol_to_table(fn_node, state)
        return state
    if fn_node.name in sqlConvert.SPLIT_OP:
        state = gen_split(fn_node, row_num, code_segment, context)
        context.add_symbol_to_table(fn_node, state)
        return state

    state = context.get_op_state(False, "none", "none", row_num)
    return state
