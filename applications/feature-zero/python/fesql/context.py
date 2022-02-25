# -*- coding: utf-8 -*-


# fz
# class
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s', \
                    datefmt='%a, %d %b %Y %H:%M:%S')
logger = logging.getLogger(__name__)

EmptyLine = "\n"
Tabs = "    "
Comma = ","


def to_millisecond(ts):
    '''
    需要加单位，否则不知道是条数还是时间戳
    :param ts:
    :return:
    '''
    time = ''
    if ts[-1] == 's':
        time = int(ts[0: -1])
        time = time * 1000
    if ts[-1] == 'm':
        time = int(ts[0: -1])
        time = time * 1000 * 60
    if ts[-1] == 'h':
        time = int(ts[0: -1])
        time = time * 1000 * 60 * 60
    if ts[-1] == 'd':
        time = int(ts[0: -1])
        time = time * 1000 * 60 * 60 * 24
    if ts[-1] == 'y':
        time = int(ts[0: -1])
        time = time * 1000 * 60 * 60 * 24 * 365
    return time


class OpState():
    def __init__(self):
        self.ok = False
        self.table = ""
        self.col = ""
        # 有问题op行号
        self.row_num = -1
        # 输出特征列名，用于fe下游直接使用
        self.feature_name = ""


class UnionClause():
    def __init__(self, relation, table1, table2, key_id, table1_info, table2_info):
        '''
        两张表union，需要考虑到key_id，表字段和拼接key之间是否有重名的问题，并且做出调整，同时还要修改
        特征字段和分片字段名。

        第一步 判断key_id 是否存在 leftkey
        存在
            第二步 是否存在 rightkey
            存在 不变
            不存在
                第三步 是否存在副表字段中
                存在 reqid 不是同类型的 reqid ,统一跟着副表key, 同时修改feature_code
                不存在 统一跟着副表key，同时修改feature_code
        不存在
            第二步 是否存在rightkey
            存在 leftkey 跟着副表，同时key_id重命名，副表新增一列，修改feature_code
            不存在
                第三步 是否存在副表字段
                    不存在 两张表新增一列
                    存在 统一跟着副表，key_id重命名，并新增一列，修改feature_code

        :param relation:
        :param table1:
        :param table2:
        :param key_id:
        :param table1_info:
        :param table2_info:
        '''
        self.relation = relation
        self.table1 = table1
        self.table2 = table2
        # 连接全表的唯一key
        self.key_id = key_id
        self.table1_info = table1_info
        self.table2_info = table2_info
        self.table1_str = ""
        self.table2_str = ""
        self.new_key_id = ""
        self.common_cols = []

    def update_schema(self):
        '''
        对齐左右两表的 schema
        :return:
        '''
        # table1
        from_keys = self.relation['from_entity_keys']
        from_entity_time_key = self.relation['from_entity_time_col']
        # table2
        to_keys = self.relation['to_entity_keys']
        to_entity_time_key = self.relation['to_entity_time_col']
        common_cols = self.table2_info.build_empty_select()
        replace_cols = {}
        i = 0
        for key in to_keys:
            idx = self.table2_info.get_idx(key)
            replace_cols[from_keys[i]] = idx
            i += 1
        replace_cols[from_entity_time_key] = self.table2_info.get_idx(to_entity_time_key)
        # 将table1的字段重命名对齐table2，主要是时间列和拼接key
        for col in replace_cols:
            idx = replace_cols[col]
            as_col = common_cols[idx][common_cols[idx].find('as') + 3:]
            common_cols[idx] = '`{}` as {}'.format(col, as_col)

        isFromKeys = (self.key_id in from_keys)
        isTokeys = (self.key_id in to_keys)
        isInTable = (self.key_id in self.table2_info.get_cols_name())

        # if isFromKeys and isTokeys:
        #     # 不用修改
        #     return
        # 全局key 和 拼接key的生成策略
        if isFromKeys and isTokeys == False and isInTable == False:
            # 主表对齐副表，更新key的列名
            self.key_id = to_keys[from_keys.index(self.key_id)]

        if isFromKeys == False and isTokeys == False and isInTable == False:
            # 主表对齐副表，并同时在两表schema基础上加一列 全局key
            common_cols.append(self.key_id)

        if isFromKeys == False and isTokeys == True:
            # 主表对齐副表，修改全局key的名字，并同时新增一列
            self.new_key_id = self.key_id + "_4paradigm"

        if isFromKeys == False and isTokeys == False and isInTable == True:
            # 主表对齐副表，修改全局key的名字，并同时新增一列
            self.new_key_id = self.key_id + "_4paradigm"

        if isFromKeys and isTokeys == False and isInTable == True:
            # 主表对齐副表，修改全局key的名字，并同时新增一列
            self.new_key_id = self.key_id + "_4paradigm"

        self.common_cols = common_cols

    def gen_table1(self):
        code = 'select '
        common_cols = self.common_cols
        if self.new_key_id != "":
            col = '{} as {}'.format(self.key_id, self.new_key_id)
            common_cols.append(col)
        code += ', '.join(common_cols)
        code += ' from `{}`'.format(self.table1)
        return '({})'.format(code)

    def gen_table2(self):
        from_keys = self.relation['from_entity_keys']
        to_keys = self.relation['to_entity_keys']
        # 新增一列
        if self.new_key_id != "":
            code = 'select '
            select = self.table2_info.get_cols_name(True)
            col = '{} as {}'.format(self.table1_info.get_constant_value(self.key_id), self.new_key_id)
            select.append(col)
            code += ', '.join(select)
            code += ' from `{}`'.format(self.table2)
            return '({})'.format(code)
        # 全局唯一key需要把所有表连接起来，如果不在默认的关系表keys中，那么就要添加一个默认值
        if self.key_id not in self.table2_info.get_cols_name() and self.key_id not in from_keys:
            code = 'select '
            select = self.table2_info.get_cols_name(True)
            code += ', '.join(select)
            code += ', {} from `{}`'.format(self.table1_info.get_default_value(self.key_id), self.table2)
            return '({})'.format(code)
        # if self.key_id in from_keys and from_keys != to_keys:
        else:
            return "`{}`".format(self.table2)


    # schema table2需要对齐 table1
    # table1是from_table, table2是to_table
    def gen_code(self):
        code = '\nUNION '
        # common_cols = self.get_common_cols()
        select_code = self.gen_table2()  # "select {} from {}".format(', '.join(common_cols), )
        code += "{}".format(select_code)
        return code



class WindowClause:
    def __init__(self, window_name, table, keys, ts, start, end, max_size):
        self.window_name = window_name
        self.table1 = table
        # union 副表
        self.table2 = ''
        self.keys = keys
        self.ts = ts
        self.start = start
        self.end = end
        self.max_size = max_size
        # union 定语
        self.clause = []
        self.id = ""
        self.range = "rows_range"

    def gen_code(self):
        code = "{} as".format(self.window_name)
        clause_code = []
        keys_str = []
        # 需要判断条数窗口还是时间窗口
        t = to_millisecond(self.start)
        if t == "":
            self.range = "rows"
        t = to_millisecond(self.end)
        if t == "":
            self.range = "rows"
        for k in self.keys:
            keys_str.append("`{}`".format(k))
        for e in self.clause:
            clause_code.append(e.gen_code())
        if len(clause_code) == 0:
            code += " (partition by {} order by `{}` {} between {} preceding and {} preceding".format(
                ",".join(keys_str), self.ts, self.range,
                self.end, self.start)
            if self.max_size != "" and int(self.max_size) > 0:
                code += " MAXSIZE {}".format(self.max_size)
        else:
            code += " ({} partition by {} order by `{}` {} between {} preceding and {} preceding".format(
                ',\n'.join(clause_code), ",".join(keys_str), self.ts, self.range,
            self.end, self.start)
            if self.max_size != "" and int(self.max_size) > 0:
                code += " MAXSIZE {}".format(self.max_size)
            code += " INSTANCE_NOT_IN_WINDOW"

        code += ")"
        return code

    def add_union_node(self, union):
        self.clause.append(union)
        self.table2 = union.table2
        self.id = "{}_{}_{}_{}_{}_{}_{}_{}".format(union.table1, 'union', union.table2, ",".join(self.keys), self.ts,
                                                   self.start,
                                                   self.end, self.max_size)

    def get_id(self):
        if self.id == "":
            self.id = "{}_{}_{}_{}_{}_{}".format(self.table1, "_".join(self.keys), self.ts, self.start, self.end,
                                                 self.max_size)
            # for e in self.clause:
            #     self.id += e.get_id()
        return self.id


class SelectScope:
    '''
    需要区分不同select之间的区别，尤其是主表和不同副表的关系的时候，哪些是共用select，哪些需要分开
    单独一张表：表名
    一张表有union窗口：表名 + union的表名
    一张表只有自己的窗口：表名
    一个select可以包含多个window，多个window只能依附于一种表关系

    上游fz op是没有顺序的，所以需要确保能打在一张表里面
    先建主表的select，那么针对有union的select，可能会覆盖只有主表的select
    所以需要一次建好select的表，这样主表的select就不会被union的select所影响
    '''

    def __init__(self, table, table_info):
        self.feature_code = []
        self.clause = []
        self.table = table
        # 用来区分lastjoin 和 window定语分别
        self.scope = ""
        # union 节点会修改schema，对齐副表
        self.table_code = ""
        self.key_id = ""
        self.output_key_id = ""
        self.id = ""
        self.table_info = table_info
        self.valid_key = ""

    def get_id(self):
        '''
        select 的id是由覆盖的表名决定的
        :return:
        '''
        if self.id == "":
            if self.table == "":
                logging.error("table is empty string")
                return ""
            else:
                self.id += self.table
            # for e in self.clause:
            #     self.id += "_" + self.clause
        return self.id

    def gen_code(self):
        code = "\nselect"
        code += EmptyLine
        code += Tabs
        if "." in self.valid_key:
            key_code = "{} as {}".format(self.valid_key, self.output_key_id)
        else:
            key_code = "`{}` as {}".format(self.key_id, self.output_key_id)
        self.feature_code.insert(0, key_code)
        code += ",\n    ".join(self.feature_code)
        code += EmptyLine
        code += "from"
        code += EmptyLine
        code += Tabs
        code += self.gen_table_code()
        code += EmptyLine
        code += Tabs
        if len(self.clause) > 0:
            if isinstance(self.clause[0], WindowClause):
                code += "window "
                clause_code = []
                for e in self.clause:
                    clause_code.append(e.gen_code())
                code += ',\n    '.join(clause_code)
            if isinstance(self.clause[0], LastJoinClause):
                clause_code = []
                for e in self.clause:
                    clause_code.append(e.gen_code())
                code += '\n    '.join(clause_code)
        return code

    def gen_table_code(self):
        if self.table_code == "":
            return "`{}`".format(self.table)
        else:
            return self.table_code

    def update_key_id(self, key_id):
        if key_id == "":
            raise RuntimeError("key_id or target_entity_index is empty string. please check your config json")
        if self.key_id == "":
            #code = "{} as {}".format(key_id, self.output_key_id)
            #self.feature_code.insert(0, code)
            if "." in key_id:
                self.key_id = key_id.split(".")[1]
                self.valid_key = "`{}`.`{}`".format(key_id.split(".")[0], key_id.split(".")[1])
            else:
                self.key_id = key_id
                self.valid_key = "`{}`".format(key_id)
            # code = "{} as {}".format(valid_key, self.output_key_id)
            # self.feature_code.insert(0, code)

    def update_output_key_id(self, id):
        if self.output_key_id == "":
            self.output_key_id = id

    def add_clause(self, clause):
        id = clause.get_id()
        for e in self.clause:
            if id == e.get_id():
                return e
        self.clause.append(clause)
        return clause

    def add_window_node(self, node):
        id = node.get_id()
        if node.table2 != "":
            self.id = "{}_{}".format(node.table1, node.table2)
        for e in self.clause:
            if id == e.get_id():
                return e
        self.clause.append(node)
        if self.scope == "":
            self.scope = "window"
        return node

    def add_lastjoin_node(self, node):
        id = node.get_id()
        for e in self.clause:
            if id == e.get_id():
                return e
        self.clause.append(node)
        if self.id == "":
            self.id = "{}_lastjoin".format(self.get_id())
        if self.scope == "":
            self.scope = "lastjoin"
        return node

    def add_node(self, node):
        id = node.get_id()
        for e in self.clause:
            if id == e.get_id():
                return e
        self.clause.append(node)
        return node


class LastJoinClause:
    '''
    作为select的一个定语情况
    '''
    def __init__(self, relation, table1, table2, table1_info, table2_info, key_pairs):
        self.relation = relation
        self.table1 = table1
        self.table2 = table2
        self.old_table2 = table2
        self.table1_info = table1_info
        self.table2_info = table2_info
        self.id = ""
        self.is_rename_table = True
        # 全局唯一id
        self.key_id = ""
        self.key_pairs = key_pairs
        self.ts1 = ""
        self.ts2 = ""
        self.start = ""
        self.end = ""
        self.max_size = ""

    def get_id(self):
        if self.id == "":
            self.id = "{}_lastjoin_{}_{}".format(self.table1, self.table2, self.key_pairs)
            if self.ts1 != "" and self.ts2 != "":
                code = "{}_{}_{}".format(self.ts1, self.start, self.end)
                self.id = "{}_{}".format(self.id, code)
        return self.id

    def change_table2_name(self):
        if self.is_rename_table:
            for k in self.key_pairs:
                self.table2 += "_{}".format(self.key_pairs[k])
            if self.ts2 != "":
                code = "_{}_{}_{}".format(self.ts1, self.start, self.end)
                self.table2 = "{}_{}".format(self.table2, code)

    def gen_table2(self):
        if self.is_rename_table:
            return "`{}` as `{}`".format(self.old_table2, self.table2)
        return "`{}`".format(self.table2)

    def gen_code(self):
        code = "last join "
        code += self.gen_table2()
        code += " "
        if self.ts1 != "" and self.ts2 != "":
            code += "order by {}.`{}` ".format(self.table2, self.ts2)
        code += "on {}".format(" and ".join(self.gen_key_pairs()))

        t1 = self.gen_table_col_code(self.table1, self.ts1)
        t2 = self.gen_table_col_code(self.table2, self.ts2)
        # 添加 时间范围边界 确保特征不会穿越
        # join t2 to t1, so t2.ts2 should < t1.ts1.(t2.ts2>t1.ts1 is useless)
        if self.ts2 != "" and self.ts1 != "" and to_millisecond(self.start) == "" and to_millisecond(self.end) == "":
            code += " and {} < {}".format(t2, t1)
        if self.start != "":
            start = to_millisecond(self.start)
            if start != "":
                code += " and {} < {} - {}".format(t2, t1, start)
        if self.end != "":
            end = to_millisecond(self.end)
            if end != "":
                code += " and {} > {} - {}".format(t2, t1, end)
        return code

    def gen_table_col_code(self, table, col):
        return "`{}`.`{}`".format(table, col)

    def gen_key_pairs(self):
        code = []
        for k, v in self.key_pairs.items():
            equal_code = "`{}`.`{}` = `{}`.`{}`".format(self.table1, k, self.table2, v)
            code.append(equal_code)
        return code

    # def gen_table1(self):


class Lastjoin:
    '''
    lastjoin node
    lastjoin -> select and select
    lastjoin -> select and table
    print lastjoin schema
    '''

    def __init__(self):
        self.table = 0

class TableInfo:
    def __init__(self):
        self.table = ''
        self.schema = []
        self.index = []
        self.cols = []

    class Column:
        def __init__(self, name, ty, data_type):
            self.name = name
            self.ty = ty
            self.data_type = data_type

    class RtidbIndex:
        def __init__(self):
            self.keys = []
            self.ts = ''
            self.ttl = '0'
            self.ttl_type = 'kabsorlat'
            self.id = ''

        def get_id(self):
            if self.id == '':
                self.id = '{}_{}'.format('_'.join(self.keys), self.ts)
            return self.id

    def add_index(self, keys, ts, ttl, ttl_type):
        index = TableInfo.RtidbIndex()
        index.keys = keys
        index.ts = ts
        index.ttl = ttl
        index.ttl_type = ttl_type
        id = '{}_{}'.format('_'.join(keys), ts)
        flag = False
        for e in self.index:
            if e.get_id() == id:
                flag = True
                if ttl > e.ttl:
                    e.ttl = ttl
                    e.ttl_type = ttl_type
        if not flag:
            self.index.append(index)

    def get_idx(self, col):
        for idx, column in enumerate(self.cols):
            if column.name == col:
                return idx
        return -1

    def get_column(self, col):
        for idx, column in enumerate(self.cols):
            if column.name == col:
                return True, column
        return False, None

    def build_empty_select(self):
        select = []
        for column in self.cols:
            if column.ty == "string":
                select.append("'' as %s" % self.add_escape(column.name))
            elif column.ty == "double":
                select.append("double(0) as %s" % self.add_escape(column.name))
            elif column.ty == "float":
                select.append("float(0) as %s" % self.add_escape(column.name))
            elif column.ty == "timestamp":
                select.append("timestamp('2019-07-18 09:20:20') as %s" % self.add_escape(column.name))
            elif column.ty == "int" or column.ty == "smallint":
                select.append("int(0) as %s" % self.add_escape(column.name))
            elif column.ty == "date":
                select.append("date('2019-07-18') as %s" % self.add_escape(column.name))
            elif column.ty == "bigint":
                select.append("bigint(0) as %s" % self.add_escape(column.name))
            else:
                logging.warning("%s is not supported for select" % column.ty)
        return select

    def add_escape(self, name):
        return "`{}`".format(name)

    def get_cols_name(self, is_ecape=False):
        '''
        :param is_ecape: 默认false, 不需要添加转义符
        :return: 
        '''
        select = []
        for column in self.cols:
            if is_ecape:
                select.append("`{}`".format(column.name))
            else:
                select.append(column.name)
            
        # select.append(col)
        return select

    def get_default_value(self, col):
        '''
        获取某一列的常量值，并返回 已经as 的字符串
        :param col:
        :return:
        '''
        select = []
        for column in self.cols:
            if column.name == col:
                if column.ty == "string":
                    select.append("'' as %s" % column.name)
                elif column.ty == "double":
                    select.append("double(0) as %s" % column.name)
                elif column.ty == "float":
                    select.append("float(0) as %s" % column.name)
                elif column.ty == "timestamp":
                    select.append("timestamp('2019-07-18 09:20:20') as %s" % column.name)
                elif column.ty == "int" or column.ty == "smallint":
                    select.append("int(0) as %s" % column.name)
                elif column.ty == "date":
                    select.append("date('2019-07-18') as %s" % column.name)
                elif column.ty == "bigint":
                    select.append("bigint(0) as %s" % column.name)
                else:
                    logging.warning("%s is not supported for select" % column.ty)
        if len(select) == 0:
            return ''
        return select[0]

    def get_constant_value(self, col):
        '''
        获取某一列的常量值，并返回字符串
        :param col:
        :return:
        '''
        select = []
        for column in self.cols:
            if column.name == col:
                if column.ty == "string":
                    select.append("''")
                elif column.ty == "double":
                    select.append("double(0)")
                elif column.ty == "float":
                    select.append("float(0)")
                elif column.ty == "timestamp":
                    select.append("timestamp('2019-07-18 09:20:20')")
                elif column.ty == "int" or column.ty == "smallint":
                    select.append("int(0)")
                elif column.ty == "date":
                    select.append("date('2019-07-18')")
                elif column.ty == "bigint":
                    select.append("bigint(0)")
                else:
                    logging.warning("%s is not supported for select" % column.ty)
        if len(select) == 0:
            return ''
        return select[0]


class SQLCodeGenContext:
    def __init__(self, config):
        self.config = config
        self.relation = []
        self.tables = {}
        # clause
        self.select = []
        self.lastjoin = []
        self.leftjoin = []
        self.window = []
        self.row_num = 0
        # fe
        self.sign = []
        self.fe_input_table = "sql_table"
        self.key_id = ""
        self.fe_window_name = "w_feature_output"
        # 自增id
        self.increase_id = 0
        # 变量复用
        # 全局op表:由op_id作为key， op_id=op全字符串， state 作为value
        self.op_table = {}

    def init(self):
        for relation in self.config["relations"]:
            self.relation.append(relation)
        self.init_ddl()
        self.key_id = self.config["target_entity_index"]
        # self.key_id = 'mcuid'

    def is_reuse(self, fn_node):
        '''
        检查op是否以前出现过
        :param fn_node:
        :return:
        '''
        id = fn_node.get_id()
        if self.op_table.get(id) is None:
            return False
        return True

    def add_symbol_to_table(self, fn_node, op_state):
        id = fn_node.get_id()
        if self.op_table.get(id) is None:
            self.op_table[id] = op_state

    def get_symbol_from_table(self, fn_node):
        id = fn_node.get_id()
        return self.op_table.get(id)


    def get_main_table(self):
        return self.config["target_entity"]

    def get_table_relation(self, from_table, to_table, tp):
        for table_relation in self.relation:
            logger.info(table_relation)
            if table_relation['from_entity'] == from_table and table_relation['to_entity'] == to_table and \
                    str(table_relation['type']).lower() == tp.lower():
                return table_relation
        return None

    def get_table_info(self, table):
        return self.tables[table]

    def get_select_node(self, table):
        table_info = self.get_table_info(table)
        select = SelectScope(table, table_info)
        return select

    def get_window_node(self, window_name, table, keys, ts, start, end, max_size):
        logger.info("gen window %s with table %s", window_name, table)
        window = WindowClause(window_name, table, keys, ts, start, end, max_size)
        return window

    def get_union_node(self, relation, table1, table2, key_id):
        logger.info('gen {} union {}'.format(table1, table2))
        table1_info = self.get_table_info(table1)
        table2_info = self.get_table_info(table2)
        union = UnionClause(relation, table1, table2, key_id, table1_info, table2_info)
        return union

    def get_lastjoin_node(self, table1, table2, tp):
        logger.info('gen {} lastjoin {}'.format(table1, table2))
        table1_info = self.get_table_info(table1)
        table2_info = self.get_table_info(table2)
        relation = self.get_table_relation(table1, table2, tp)
        key1 = relation['from_entity_keys']
        key2 = relation['to_entity_keys']
        key_pairs = {}
        for i in range(len(key1)):
            key_pairs[key1[i]] = key2[i]
        lastjoin = LastJoinClause(relation, table1, table2, table1_info, table2_info, key_pairs)
        if tp == "slice":
            lastjoin.ts1 = relation['from_entity_time_col']
            lastjoin.ts2 = relation['to_entity_time_col']
        return lastjoin

    def get_output_key_id(self):
        self.increase_id += 1
        return "{}_{}".format(self.key_id, self.increase_id)

    def add_select_node(self, select_scope):
        '''
        添加select node节点
        :param select_scope:
        :return:
        '''
        id = select_scope.get_id()
        for e in self.select:
            if e.get_id() == id:
                logger.warning("select id %s is existed", id)
                for node in select_scope.clause:
                    e.add_node(node)
                # e.scope = select_scope.scope
                return e
        self.select.append(select_scope)
        return select_scope

    def add_sign(self, feature_code):
        self.sign.append(feature_code)

    # 最后一行的签名被覆盖
    def replace_sign(self, feature_code):
        self.sign.pop()
        self.sign.append(feature_code)

    def fz_window_split_function(self, var, separator):
        code = "fz_window_split({},  \"{}\")".format(var, separator)
        return code

    def fz_window_split_by_key_function(self, var, separator, key_separatore):
        code = "fz_window_split_by_key({},  \"{}\",  \"{}\")".format(var, separator, key_separatore)
        return code

    def fz_window_split_by_value_function(self, var, separator, key_separatore):
        code = "fz_window_split_by_value({},  \"{}\",  \"{}\")".format(var, separator, key_separatore)
        return code

    def split_function(self, var, separator, type=None):
        if type is None:
            split_code = "split({}, \"{}\")".format(var, separator)
        else:
            split_code = "split({}, \"{}\", {})".format(var, separator, type)
        return split_code

    def split_key_function(self, var, separator, key_separatore):
        split_code = "splitbykey({}, \"{}\", \"{}\")".format(var, separator, key_separatore)
        return split_code

    def get_keys_function(self, code):
        code = "get_keys({})".format(code)
        return code

    def get_values_function(self, code):
        code = "get_values({})".format(code)
        return code

    def gen_fe(self):
        code = "# start fe code"
        code += "\nw_feature_output = window(table={}, output=\"w_output_feature_table\")".format(self.fe_input_table)
        code += EmptyLine
        code += "\n".join(self.sign)
        return code

    def get_op_state(self, ok, table, col, row_num, feature_name="", feature_code=""):
        '''

        :param ok:
        :param table:
        :param col:
        :param row_num:
        :param feature_name: 输出特征的名字
        :param feature_code: 输出特征的code，有些op不能直接用特征名，要做一次split操作才行
        :return:
        '''
        state = OpState()
        state.ok = ok
        state.table = table
        state.col = col
        state.row_num = row_num
        state.feature_name = feature_name
        state.feature_code = feature_code
        return state

    def init_ddl(self):
        for e in self.config["entity_detail"]:
            table_info = TableInfo()
            table_info.table = e
            # logger.info(e)
            code = "create table {}".format(e)
            schema = []
            cols = []
            for field in self.config["entity_detail"][e]['features']:
                name = field['id'].split(".")[1]
                tp = field['feature_type'].lower()
                dp = field['data_type'].lower()
                col = TableInfo.Column(name, tp, dp)
                cols.append(col)

                field_str = "{} {}".format(name, tp)
                schema.append(field_str)
                # table_info.schema[name] = tp

            code += "(\n{}\n)".format(",".join(schema))
            table_info.schema = schema
            table_info.cols = cols
            # logger.info(code)
            self.tables[table_info.table] = table_info

    def update_table_index(self, table, keys, ts, ttl):
        table_info = self.tables[table]
        table_info.add_index(keys, ts, ttl, 'kabsorlat')

    def gen_table_ddl(self):
        ddl = []
        for e in self.tables:
            code = "create table {}".format(e)
            table_info = self.tables[e]
            index_code = []
            for rtidb_index in table_info.index:
                if rtidb_index.ts == '':
                    index_str = 'index(key=[{}], ttl={}, ttl_type={})'.format(','.join(rtidb_index.keys),
                                                                              rtidb_index.ttl, rtidb_index.ttl_type)
                else:
                    index_str = 'index(key=[{}], ts={}. ttl={}, ttl_type={})'.format(','.join(rtidb_index.keys),
                                                                                     rtidb_index.ts, rtidb_index.ttl,
                                                                                     rtidb_index.ttl_type)
                index_code.append(index_str)
            index_code.append("index(key=(SK_ID_CURR), ts=time, ttl=32d)\n")
            code += "(\n{},\n{})".format(",\n".join(table_info.schema), ",\n".join(index_code))
            code += ";"
            ddl.append(code)
        return '\n'.join(ddl)

    def gen_sql(self):
        logger.info("join all select node")
        if len(self.select) == 0:
            logger.error("no select statemnet")
            return ""
        code = "# start sql code\n"
        code += "# output table name: " + self.fe_input_table + "\n"
        if len(self.select) == 1:
            return code + self.select[0].gen_code()

        # code = "# start sql code"
        code += "select * from "
        code += EmptyLine
        select_code = "({})".format(self.select[0].gen_code())
        select_code += EmptyLine
        select_code += "as out0"
        select_code += EmptyLine
        output_key1 = self.select[0].output_key_id
        for idx in range(1, len(self.select)):
            # select_code.append(self.select[idx].gen_code())
            output_key2 = self.select[idx].output_key_id
            select_code += "last join"
            select_code += EmptyLine
            select_code += "({})".format(self.select[idx].gen_code())
            select_code += EmptyLine
            select_code += "as out{}".format(idx)
            select_code += EmptyLine
            select_code += "on out0.{} = out{}.{}".format(output_key1, idx, output_key2)
            select_code += EmptyLine
        code += select_code
        return code
