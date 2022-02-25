# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
#

"""

"""
import json
import logging

class Column(object):
    def __init__(self, name, ty, feat_type):
        self.name_ = name
        self.ty_ = ty
        self.feat_type = feat_type

class Schema(object):

    def __init__(self, tname, columns):
        self.tname_ = tname
        self.columns_ = columns

    def find_idx(self, col):
        for idx, column in enumerate(self.columns_):
            if column.name_ == col:
                return True, idx
        return False, 0

    def get_feat_type(self, col):
        for idx, column in enumerate(self.columns_):
            if column.name_ == col:
                return True, column.feat_type 
        return False, -1

    def get_column(self, col):
        for idx, column in enumerate(self.columns_):
            if column.name_ == col:
                return True, column
        return False, None

    def build_empty_select(self):
        select = []
        for column in self.columns_:
            if column.ty_ == "string":
                select.append("'' as %s" % column.name_)
            elif column.ty_ == "double":
                select.append("0.0D as %s" % column.name_)
            elif column.ty_ == "float":
                select.append("0.0 as %s" % column.name_)
            elif column.ty_ == "timestamp":
                select.append("2019-07-18T09:20:20 as %s" % column.name_)
            elif column.ty_ == "int" or column.ty_ == "smallint":
                select.append("0 as %s" % column.name_)
            elif column.ty_ == "date":
                select.append("2019-07-18T as %s" % column.name_)
            elif column.ty_ == "bigint":
                select.append("0L as %s" % column.name_)
            else:
                logging.warning("%s is not supported for select" % (column.ty_))
        return select

class SchemaEncoder(json.JSONEncoder):

    def default(self, obj):
        if isinstance(obj, Schema):
            return  obj.columns_
        elif isinstance(obj, Column):
            return {"name":obj.name_, "type":obj.ty_}
        else:
            return json.JSONEncoder.default(self, obj)

def build_tables(config):
    """
    parse table schema from config.json return the table dict 
    """
    if "entity_detail" not in config or  not config["entity_detail"]:
        return False, "entity_detail is required"
    tables = {}
    for (k, v) in config["entity_detail"].items():
        cols = []
        for col in v['features']:
            col_name = col['id'].split('.')[1]
            feat_type = 0
            if col['data_type'] == 'ContinueNum':
                feat_type = 1
            ty = col.get("feature_type", "").lower()
            cols.append(Column(col_name, ty, feat_type))
        schema = Schema(k, cols)
        tables[k] = schema 
    return True, tables



