
# -*- coding: utf-8 -*-
from feql import schema
import logging
logger = logging.getLogger(__name__)

CONTINUOUS_FEAT_CATEGORY = set(["add", "subtract", "divide", "multiply",
    "window_sum", "window_avg", "window_min", "window_max", "multi_avg", "multi_min", "multi_max",
    "multi_sum", "multi_std"])

def is_time_limit(cnt):
    if cnt[-1] == 'd':
        return True
    return False

def get_limit_and_time_limit(windows):
    limit = "-1"
    time_limit = "0s"
    at_least = "0"
    for l in windows:
        parts = l.split(",")
        if len(parts) == 2:
            parts_rd = parts[1].split(":")
            time_limit = parts[0]
            if int(parts_rd[0]) > int(limit):
                limit = parts_rd[0]
        if len(parts) == 1:
            parts_rd = parts[0].split(":")
            at_least = parts_rd[0]
    return at_least, limit, time_limit

class JoinConfig(object):
    def __init__(self, node, idx, type, col_name, left_key, right_key, left_ts, right_ts):
        self.node = node
        self.idx = idx
        self.join_type = type
        self.col_name = col_name
        self.left_key = left_key
        self.right_key = right_key
        self.left_ts_col = left_ts
        self.right_ts_col = right_ts
        self.label = None
        # 多表拼接
        self.join_name = None
        # label拼接
        self.label = ""


class WindowConfig(object):
    def __init__(self, wname, table,
            other_table, 
            pks, order, 
            limit,
            at_least = None,
            time_limit = None,
            output = None, tables = None, relation = None, join = None):
        self.wname = wname
        self.pks = pks
        self.order = order
        self.limit = limit
        self.at_least = at_least
        self.time_limit = time_limit
        self.output = output
        self.table = table
        self.other_table = other_table
        self.fcodes = []
        self.tables = tables
        self.relation = relation
        self.join = join
        self.is_gen_fcodes = True

    def disable_gen_feature(self):
        self.is_gen_fcodes = False

    def is_gen_feature(self):
        return self.is_gen_fcodes

    def is_only_table(self, table):
        if self.table == table and not self.other_table:
            return True, self.get_window_key()
        return False, None

    def get_window_key(self):

        """
        get the window key 
        """
        key = None
        if self.other_table:
            key = "main_" + self.table + "_other_" + self.other_table + ",".join(self.pks) + self.order + "_" + \
                  self.relation[
                      'type']
        else:
            key = "main_" + self.table + ",".join(self.pks) + self.order
            # 单行窗口无type类型
            if self.relation is not None and self.relation['type'] is not None:
                key = key + "_" + self.relation['type']
        return key

    def merge_window(self, window_config):
        if window_config.at_least:
            if not self.at_least:
                self.at_least = window_config.at_least
                logger.info("merge window %s with new at least %s", self.wname, self.at_least)
            elif int(self.at_least) < int(window_config.at_least):
                self.at_least = window_config.at_least
                logger.info("merge window %s with new at least %s", self.wname, self.at_least)
        if window_config.limit:
            if not self.limit:
                self.limit = window_config.limit
                logger.info("merge window %s with new limit %s", self.wname, self.limit)
            elif int(self.limit) < int(window_config.limit):
                self.limit = window_config.limit
                logger.info("merge window %s with new limit %s", self.wname, self.limit)
        if window_config.time_limit:
            if not self.time_limit:
                self.time_limit = window_config.time_limit
                logger.info("merge window %s with new time limit %s", self.wname, self.time_limit)
            elif int(self.time_limit[0:-1]) < int(window_config.time_limit[0:-1]):
                self.time_limit = window_config.time_limit
                logger.info("merge window %s with new time limit %s", self.wname, self.time_limit)
        if not self.pks:
            self.pks = window_config.pks
        if not self.order:
            self.order = window_config.order

    def append(self, code):
        self.fcodes.append(code)

    def insert_code(self, index, code):
        self.fcodes.insert(index, code)

    def gen(self):
        # window没有生成特征，就不需要产出window了
        if not self.fcodes:
            return True, ""
        code = []
        if self.other_table:
            ok, line = self.build_window_union()
            if not ok:
                return False, None
            code.append(line)
        else:
            code.append(self.build_window())
        if self.is_gen_fcodes:
            code.extend(self.fcodes)
        return True, "\n".join(code)

    def join_table(self):
        join = self.join
        codes = []
        if join.join_name != None:
            left_name = join.join_name
        else:
            left_name = "join_" + self.table

        right_name = "join_" + self.other_table
        join_name = left_name + "_" + right_name

        join_code = []
        for index in range(len(join.left_key)):
            left_key = self.table + "_" + join.left_key[index]
            right_key = self.other_table + "_" + join.right_key[index]
            if join.join_type == 'lastjoin':
                join_code.append("%s.%s == %s.%s" % (left_name, left_key, right_name, right_key))
                join_code.append(" and ")
            if join.join_type == 'leftjoin':
                join_code.append("%s.%s = %s.%s" % (left_name, left_key, right_name, right_key))
                join_code.append(" and ")
        join_code.pop()


        if join.join_type == 'lastjoin':
            join_name = join_name + "_" + "last"
            right_ts = self.other_table + "_" + join.right_ts_col
            left_ts = self.table + "_" + join.left_ts_col
            code = "%s = lastjoin(%s, %s, %s, %s.%s between (unbound, %s.%s))\n" % (
            join_name, left_name, right_name, "".join(join_code), right_name, right_ts, left_name, left_ts)
            codes.append(code)
            return join_name, "\n".join(codes)

        if join.join_type == 'leftjoin':
            join_name = join_name + "_" + "left"
            code = "%s = leftjoin(%s, %s, \"%s\")\n" % (join_name, left_name, right_name, "".join(join_code))
            codes.append(code)
            return join_name, "\n".join(codes)

    def build_rename(self, new_name, old_name, table_obj):
        code = "%s = select(%s" % (new_name, old_name)
        for field in table_obj.columns_:
            code += ", %s as %s" % (field.name_, old_name + "_" + field.name_)
        code += ")"
        print(code)
        return code


    def build_window(self):

        if not self.pks:
            return """%s = window(table=%s, output="%s")""" %(self.wname, self.table, self.output)
        else:
            if self.at_least == None:
                code = """%s = window(table=%s, keys=[%s], order=%s, max_size=%s, offset=%s, output="%s")""" % (
                    self.wname,
                    self.table, ",".join(self.pks), self.order, self.limit, self.time_limit, self.output)
                return code
            if self.limit == "-1":
                self.limit = int(self.at_least) + 1
            code = """%s = window(table=%s, keys=[%s], order=%s, max_size=%s, at_least=%s, offset=%s, output="%s")"""%(self.wname,
                  self.table,  ",".join(self.pks), self.order, self.limit, self.at_least, self.time_limit, self.output)
            return code

    def build_select(self):
        """
        match the two table schema
        """
        from_keys = self.relation['from_entity_keys']
        to_keys = self.relation['to_entity_keys']
        right_table = self.tables[self.other_table]
        selects = right_table.build_empty_select()
        for idx, k in enumerate(to_keys):
          from_key = from_keys[idx]
          _, k_idx_in_schema = right_table.find_idx(k)
          # replace the default select
          selects[k_idx_in_schema] =  "%s as %s"%(from_key, k)
        from_entity_time_key = self.relation['from_entity_time_col']
        to_entity_time_key = self.relation['to_entity_time_col']
        _, k_idx_in_schema = right_table.find_idx(to_entity_time_key)
        # replace timestamp col
        selects[k_idx_in_schema] = "%s as %s" % (from_entity_time_key, to_entity_time_key)
        select_cols = ",".join(selects)
        return "select(%s, %s)" % (self.relation['from_entity'], select_cols)

    def build_window_union(self):
        from_table = self.relation['from_entity']
        to_table = self.relation['to_entity']
        to_keys = self.relation['to_entity_keys']
        to_entity_time_key = self.relation['to_entity_time_col']
        logger.info("build window union from table %s to table %s", from_table, to_table)
        select_table_name = "select_" + to_table 
        codes = []
        if select_table_name not in self.tables:
            segment = select_table_name + " = " + self.build_select()
            codes.append(segment)
        #TODO support multi limit condition
        hard_limit = self.limit
        at_least = self.at_least
        timelimit = self.time_limit
        if self.relation['type'] == "1-M":
            at_least_relation, hard_limit_relation, timelimit_relation = get_limit_and_time_limit(self.relation['time_windows'])
            if hard_limit is None:
                hard_limit = hard_limit_relation
            if at_least is None:
                at_least = at_least_relation
            if timelimit is None:
                timelimit = timelimit_relation
        else:
            logging.error("union type is wrong for type %s", self.relation['type'])
            return False, None
        if not hard_limit:
            hard_limit = int(at_least) + 1
        if at_least == "0" and timelimit:
            window_segment = """%s = window(table=%s, other_table=[%s], keys=[%s], order=%s, max_size=%s, offset=%s, instance_is_window=false, output="%s")"""%(self.wname,
                  select_table_name, to_table, ",".join(to_keys), to_entity_time_key, hard_limit, timelimit, "%s_output"%from_table)
            codes.append(window_segment)
        elif at_least and timelimit:
            if int(at_least) >= int(hard_limit):
                hard_limit = int(at_least) + 1
            window_segment = """%s = window(table=%s, other_table=[%s], keys=[%s], order=%s, at_least=%s, max_size=%s, offset=%s, instance_is_window=false, output="%s")"""%(self.wname,
                  select_table_name, to_table, ",".join(to_keys), to_entity_time_key, at_least, hard_limit, timelimit, "%s_output"%from_table)
            codes.append(window_segment)
        elif at_least and not timelimit:
            window_segment = """%s = window(table=%s, other_table=[%s], keys=[%s], order=%s, at_least=%s, instance_is_window=false, output="%s")"""%(self.wname,
                  select_table_name, to_table, ",".join(to_keys), to_entity_time_key, at_least, "%s_output"%from_table)
            codes.append(window_segment)
        return True, "\n".join(codes)

class OpVar(object):
    def __init__(self):
        self.f_name = None
        self.code = None

class FeQLCodeGenContext(object):
    def __init__(self, config):
        self.windows = []
        self.select = []
        self.lastjoin = []
        self.leftjoin = []
        self.config = config
        self.sample_table = config['target_entity']
        self.label = config['target_label']
        self.var_cache = {}
        self.op_var_cache = {}
        self.counter = 0
        self.output_key = ""
        self.lastjoin_key = ""
        self.leftjoin_key = ""
        self.build_table_code = []
        self.colname2table = {}
        self.colname2col = {}
        self.join_output_table = {}

    def put_var(self, id, var):
        self.var_cache[id] = var

    def put_op_var(self, id, op_var):
        self.op_var_cache[id] = op_var

    def get_var(self, id):
        return self.var_cache[id]

    def get_op_var(self, id):
        return self.op_var_cache[id]

    def op_var_exist(self, id):
        return id in self.op_var_cache

    def var_exist(self, id):
        return id in self.var_cache

    def init(self):
        fe_config = {}
        ok, tables = schema.build_tables(self.config)
        if not ok:
            logger.warning("fail to build tables")
            return False
        self.tables = tables
        table_encoder = schema.SchemaEncoder()
        fe_config['tableInfo'] = tables
        self.fe_config_str = table_encoder.encode(fe_config)
        if 'relations' not in self.config or not self.config['relations'] or len(self.config['relations']) == 0:
            self.has_multi_table = False
        else:
            self.has_multi_table = True
        for relation in self.config["relations"]:
            table = relation['from_entity']
            other_table = relation['to_entity']
            to_keys = relation['to_entity_keys']
            order = relation['to_entity_time_col']
            if order == None:
                order = self.config['target_pivot_timestamp']
            window_name = other_table + "_union_window"
            # or relation['type'] == "SLICE"

            if relation['type'] == "1-M":
                at_least, max_size, timelimit = get_limit_and_time_limit(relation['time_windows'])
                # max_size = int(at_least) + 1
                if int(at_least) == 0:
                    wc = WindowConfig(window_name, table, other_table, to_keys, order, max_size, None, timelimit,
                                      table + "_output", tables, relation)
                else:
                    wc = WindowConfig(window_name, table, other_table, to_keys, order, max_size, at_least, timelimit,
                                      table + "_output", tables, relation)
            else:
                max_size = "100"
                wc = WindowConfig(window_name, table, other_table, to_keys, order, max_size, None, None, table + "_output", tables, relation)
            self.windows.append(wc)
        self.build_feature_window()
        return True

    def build_feature_window(self):
        if self.has_multi_table:
            self.feature_table = self.sample_table + "_output"
            self.feature_window = WindowConfig("w_feature_output", self.feature_table, None, [], "", None, None,
                                               None, "w_output_feature_table", None)
            # self.windows.append(self.feature_window)
            logger.info("init feature table")
            self.build_global_table()
            logger.info("init multi table")
        else:
            self.feature_table = self.sample_table + "_output"
            self.feature_window = WindowConfig("w_feature_output", self.feature_table, None, [], "", None, None, None, "w_output_feature_table", None)
            # self.windows.append(self.feature_window)
            logger.info("init single table")


    def build_global_table(self):
        self.counter += 1
        config = self.config
        table = config['target_entity']
        value = config['target_entity_index']
        window_name = "key_{}_{}_{}".format(table, value, self.counter)
        wc = WindowConfig(window_name, table, None, [], "", None, None, None, table + "_output", None)
        wn, key = self.add_window(wc)
        feature = "%s_outputkey_%s_%s" % (table, table, value)
        code = "%s = output(%s.%s[0])" % (feature, window_name, value)
        self.output_key = feature
        self.append_code(key, code)
        # self.add_column(feature)

    def add_window(self, window_config):
        for w in self.windows:
            if w.other_table is None and not w.pks:
                if window_config.pks is not None:
                    continue
                if w.table == window_config.table:
                    w.merge_window(window_config)
                    return w.wname, w.get_window_key()
            if w.get_window_key() == window_config.get_window_key():
                w.merge_window(window_config)
                return w.wname, w.get_window_key()
        window_config.tables = self.tables
        if window_config.other_table is None and not window_config.pks:
            self.windows.insert(0, window_config)
            return window_config.wname, window_config.get_window_key()
        self.windows.append(window_config)
        # self.windows.insert(-1, window_config)
        return window_config.wname, window_config.get_window_key()

    def add_select(self, table):
        if table not in self.select:
            self.select.append(table)

    def add_lastjoin(self, window_config):
        self.lastjoin.append(window_config)
        return window_config.wname, window_config.get_window_key()

    def add_leftjoin(self, window_config):
        self.leftjoin.append(window_config)
        return window_config.wname, window_config.get_window_key()

    def add_split(self, var, separator, type = None):
        self.counter += 1
        f_name = "%s_%s" % (var, self.counter)
        var = "{}.{}[0]".format(self.feature_window.wname, var)

        if type is None:
            split_code = "split({}, \"{}\")".format(var, separator)
        code = "{} = discrete({})".format(f_name, split_code)
        self.feature_window.append(code)

    def add_sign_code(self, var, sign, user_code):
        self.counter += 1
        f_name = "%s_%s" % (var, self.counter)
        code = "{} = {}({})".format(f_name, sign, user_code)
        self.feature_window.append(code)

    def add_split_key(self, var, separator, key_separator, type = None):
        self.counter += 1
        f_name = "%s_%s" % (var, self.counter)
        var = "{}.{}[0]".format(self.feature_window.wname, var)
        code = "{} = discrete({})".format(f_name, self.get_keys_function(self.split_key_function(var, separator, key_separator)))
        self.feature_window.append(code)

    def if_function(self, bool_code, result1, result2):
        return "if({}, {}, {})".format(bool_code, result1, result2)

    def group_by_function(self, datas, condition):
        return "group_by({}, {})".format(datas, condition)

    def where_function(self, datas, condition):
        return "where({}, {})".format(datas, condition)

    def map_function(self, datas, condition):
        return "map({}, {})".format(datas, condition)

    def join_function(self, separater, default_value, code):
        return "join(\"{}\", \"{}\", {})".format(separater, default_value, code)

    def bigint_function(self, code):
        return "bigint({})".format(code)

    def date_function(self, code):
        return "date({})".format(code)

    def count_function(self, code):
        return "count({})".format(code)

    def single_function(self, function, code):
        return "{}({})".format(function, code)

    def get_keys_function(self, code):
        code = "get_keys({})".format(code)
        return code

    def get_values_function(self, code):
        code = "get_values({})".format(code)
        return code

    def split_function(self, var, separator, type = None):
        if type is None:
            split_code = "split({}, \"{}\")".format(var, separator)
        else:
            split_code = "split({}, \"{}\", {})".format(var, separator, type)
        return split_code

    def split_key_function(self, var, separator, key_separatore):
        split_code = "splitbykey({}, \"{}\", \"{}\")".format(var, separator, key_separatore)
        return split_code

    def add_split_value(self, var, separator, key_separator, type = None):
        self.counter += 1
        f_name = "%s_%s" % (var, self.counter)
        var = "{}.{}[0]".format(self.feature_window.wname, var)
        code = "{} = discrete({})".format(f_name, self.get_values_function(self.split_key_function(var, separator, key_separator)))
        self.feature_window.append(code)

    def add_continuous(self, var, var2 = None):
        self.counter += 1
        if not var2:
            f_name = "%s_%s"%(var, self.counter)
        else:
            f_name = "%s_%s"%(var2, self.counter)
        code = "%s = continuous(%s.%s[0])" % (f_name, self.feature_window.wname, var)
        # self.output_window.append(code)
        self.feature_window.append(code)

    def add_discrete(self, var, var2 = None):
        self.counter += 1
        if not var2:
            f_name = "%s_%s"%(var, self.counter)
        else:
            f_name = "%s_%s"%(var2, self.counter)
        code = "%s = discrete(%s.%s[0])" % (f_name, self.feature_window.wname, var)
        self.feature_window.append(code)

    def add_label(self, var, var2 = None, default_value = None, label_type = None):
        self.counter += 1
        if not var2:
            f_name = "%s_%s" % (var, self.counter)
        else:
            f_name = "%s_%s" % (var2, self.counter)
        if label_type is None:
            label_type = 'binary_label'
        if default_value is None:
            code = "%s = %s(%s.%s[0])" % (f_name, label_type, self.feature_window.wname, var)
        else:
            code = "%s = %s(ifnull(%s.%s[0], %s))" % (f_name, label_type, self.feature_window.wname, var, default_value)
        self.feature_window.append(code)

    def add_column(self, var, var2 = None):
        self.counter += 1
        if not var2:
            f_name = "%s_%s"%(var, self.counter)
        else:
            f_name = "%s_%s"%(var2, self.counter)
        code = "%s = column(%s.%s[0])"%(f_name, self.feature_window.wname, var)
        self.feature_window.append(code)

    def out_feature(self, feature, var):
        self.counter += 1
        f_name = "%s_%s"%(var, self.counter)
        # code = "%s = column(%s.%s[0])"%(f_name, self.output_window.wname, var)
        code = "%s = %s(%s.%s[0])".format(f_name, feature, self.feature_window.wname, var)
        self.feature_window.append(code)

    def append_code(self, key, code):
        for w in self.windows:
            if w.get_window_key() == key:
                w.append(code)
                return True
        return False

    def insert_code(self, key, code, index):
        for w in self.windows:
            if w.get_window_key() == key:
                w.insert_code(index, code)
                return True
        return False

    def get_only_table_window(self, table):
        for w in self.windows:
            ok, key = w.is_only_table(table)
            if not ok:
                continue
            else:
                return ok, key, w.wname
        return False, None, None

    def get_window_by_table_and_other_table(self, table, 
            other_table):
        for w in self.windows:
            if w.table == table and w.other_table == other_table:
                return True, w.get_window_key(), w.wname
        return False, None, None

    def get_window(self, table, other_table, type):
        for w in self.windows:
            if w.table == table and w.other_table == other_table:
                if w.relation['type'].lower() == type.lower():
                    return True, w
        return False, None

    def gen(self):
        codes = []
        # if self.has_multi_table:
        #     codes.append("\n".join(self.build_table_code))
        for w in self.windows:
            ok, code = w.gen()
            if not ok:
                return False, None
            codes.append(code)
            if code != "" and self.output_key == "" and w.relation is not None:
                config = self.config
                value = config['target_entity_index']
                table = config['target_entity']
                feature = "%s_outputkey_%s_%s" % (table, table, value)
                code = "%s = output(%s.%s[0])" % (feature, w.wname, value)
                codes.append(code)
                self.output_key = feature
        is_join, code = self.gen_join()
        codes.append(code)
        code = self.gen_grove(is_join)
        codes.append(code)
        ok, code = self.feature_window.gen()
        if not ok:
            return False, None
        codes.append(code)
        all = "\n".join(codes)
        all.replace("\r", "")
        all.replace("\n", "\\n")
        all.replace("\"", "\\\"")
        # logger.info("gen feql with %s", all)
        return True, all

    # 构建树林，由3类树组成，单表树，多表树和拼接表树
    def gen_grove(self, is_join):
        if is_join:
            left = self.join_output_table
            left_key = self.leftjoin_key
            right = self.feature_table
            right_key = self.output_key
            table = left + "_" + right
            self.feature_window.table = table
            # codes = []
            code = "%s = leftjoin(%s, %s, \"%s.%s = %s.%s\")\n" % (table, left, right, left, left_key, right, right_key)
            return code
        return ""

    def gen_select(self):
        code = []
        for s in self.select:
            schema = self.tables[s]
            code.append(self.rename_table('join_' + s, s, schema))
        return "\n".join(code)

    def rename_table(self, new_name, old_name, table_obj):
        code = "%s = select(%s" % (new_name, old_name)
        map = self.colname2table
        for field in table_obj.columns_:
            new_col = old_name + "_" + field.name_
            code += ", %s as %s" % (field.name_, new_col)
            map[new_col] = old_name
            self.colname2col[new_col] = field.name_
        code += ")"
        # print(code)
        return code

    def gen_lastjoin(self):
        codes = []
        column = []
        # 不能重复join
        joined = []
        join_name = None
        if len(self.lastjoin) == 0:
            return False, None, None, None
        column_map = {}
        # 本质还是遍历windowconfig
        for join in self.lastjoin:
            if join_name != None:
                join.join.join_name = join_name
            if join.other_table not in joined:
                join_name, code = join.join_table()
                joined.append(join.other_table)
                codes.append(code)
            col = join.other_table + "_" + join.join.col_name
            if join.is_gen_feature():
                column_map[col] = join
                column.append(col)
            # todo 考虑cnt作用！
        node_last, code = self.join_output_colomn(join_name, column, [], column_map)
        codes.append(code)
        field = self.output_key.split("outputkey")[1][1:]
        self.lastjoin_key = "lastjoinkey_" + field
        code = "%s = output(%s.%s[0])" % (self.lastjoin_key, node_last.window_name, field)
        codes.append(code)

        # right_name = "w_output_table"
        # left_name = node_last.output_name
        # window_join = left_name + "_" + right_name
        # left_key = self.lastjoin_key
        # right_key = "w_" + self.output_key
        # code = "%s = leftjoin(%s, %s, \"%s.%s = %s.%s\")\n" % (
        #     window_join, left_name, right_name, left_name, left_key, right_name, right_key)
        # codes.append(code)
        # node_last.join_name = left_name
        return True, join_name, node_last, "\n".join(codes)

    # 将两张表的树拼在一起
    def join_table_tree(self, join_table, left_name, right_name, left_key, right_key):
        code = "%s = leftjoin(%s, %s, \"%s.%s = %s.%s\")\n" % (
            join_table, left_name, right_name, left_name, left_key, right_name, right_key)
        return code

    def gen_leftjoin(self):
        codes = []
        column = []
        label = []
        # 不能重复join
        joined = []
        join_name = None
        if len(self.leftjoin) == 0:
            return False, None, None, None
        # 本质还是遍历windowconfig，输出column
        column_map = {}
        for wc in self.leftjoin:
            if join_name != None:
                wc.join.join_name = join_name
            if wc.other_table not in joined:
                join_name, code = wc.join_table()
                joined.append(wc.other_table)
                codes.append(code)
            col = wc.other_table + "_" + wc.join.col_name
            column_map[col] = wc
            if wc.join.label == "binary_label" or wc.join.label == "regression_label":
                label.append(col)
                # codes.append(self.join_label(join_name, col))
                continue
            if wc.is_gen_feature():
                column.append(col)

        node_left, code = self.join_output_colomn(join_name, column, label, column_map)
        codes.append(code)
        join_name = node_left.output_name
        return True, join_name, node_left, "\n".join(codes)

    # 拼接后的表，以column形式输出
    def join_output_colomn(self, join, column, label = [], column_map = {}):
        codes = []
        window = join + "_window"
        output = join + "_join_output"
        code = "%s = window(%s, \"%s\")" % (window, join, output)
        codes.append(code)
        table_map = self.colname2table
        for col in column:
            table = table_map[col]
            field = self.colname2col[col]
            type_feature = self.get_data_type(table, field)
            assert type_feature is not None
            self.counter += 1
            wc = column_map[col]
            f_name = self.get_var(wc.join.node.get_id())
            code = "%s = column(%s.%s[0])" % (f_name, window, col)
            # feature = "%s_instance_%s_%s" % (join, f_name, self.counter)
            if type_feature == "ContinueNum":
                # code = "%s = continuous(%s.%s[0])" % (feature, window, f_name)
                self.add_continuous(f_name)
            else:
                # code = "%s = discrete(%s.%s[0])" % (feature, window, f_name)
                self.add_discrete(f_name)
            codes.append(code)
        for col in label:
            self.counter += 1
            wc = column_map[col]
            f_name = self.get_var(wc.join.node.get_id())
            code = "%s = column(%s.%s[0])" % (f_name, window, col)
            # feature = "%s_label_%s_%s" % (join, col, self.counter)
            # code = "%s = binary_label(%s.%s[0])" % (feature, window, col)
            default_value = self.find_col_value(col)
            self.add_label(f_name, None, default_value, wc.join.label)
            codes.append(code)
        node = OutputNode()
        node.output_name = output
        node.window_name = window
        return node, "\n".join(codes)

    def get_col_type(self, name, table, config):
        for entity in config["entity_detail"][table]["features"]:
            if entity["id"] == "{}.{}".format(table, name):
                return str(entity["feature_type"]).lower()
        return None

    def get_default_value(self, fetype):
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

    def find_col_value(self, col):
        src_col = self.colname2col[col]
        src_table = self.colname2table[col]
        fetype = self.get_col_type(src_col, src_table, self.config)
        return self.get_default_value(fetype)

    def join_label(self, join, col):
        self.counter += 1
        window = join + "_window"
        feature = "%s_label_%s_%s" % (join, col, self.counter)
        code = "%s = binary_label(%s.%s[0])" % (feature, window, col)
        return code

    def get_data_type(self, table, field):
        for value in self.config["entity_detail"][table]["features"]:
            if value['id'] == "{}.{}".format(table, field):
                return value['data_type']
        return None

    def gen_join(self):
        codes = []
        join = []
        codes.append(self.gen_select())
        # join_name = ""
        # right_name = "w_join_table"
        # output_name = "w_join"
        # feature = "w_" + self.output_key
        # _ = "%s = output(%s.%s[0])" % (feature, output_name, self.output_key)
        # codes.append(_)

        last_ok, lastjoin_name, node_last, code = self.gen_lastjoin()
        if last_ok:
            codes.append(code)
            join.append(lastjoin_name)
            lastjoin_name = node_last.output_name
            self.join_output_table = lastjoin_name
            self.leftjoin_key = self.lastjoin_key


        left_ok, leftjoin_name, node_left, code = self.gen_leftjoin()
        # 拼接树的输出表名
        if not last_ok and not left_ok:
            # codes.pop()
            return False, "\n".join(codes)

        if left_ok:
            self.join_output_table = leftjoin_name
            join.append(leftjoin_name)
            codes.append(code)
            field = self.output_key.split("outputkey")[1][1:]
            self.leftjoin_key = "leftjoinkey_" + field
            code = "%s = output(%s.%s[0])" % (self.leftjoin_key, node_left.window_name, field)
            codes.append(code)

            left_name = leftjoin_name
            left_key = self.leftjoin_key

            if last_ok:
                right_name = lastjoin_name
                right_key = self.lastjoin_key
                self.join_output_table = left_name + "_" + right_name
                code = self.join_table_tree(self.join_output_table, left_name, right_name, left_key, right_key)
                codes.append(code)

        return True, "\n".join(codes)

class OutputNode(object):
    def __init__(self):
        self.output_name = None
        self.key = None
        self.window_name = None
        self.join_name = None