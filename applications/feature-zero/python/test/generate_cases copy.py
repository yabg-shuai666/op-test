#!/usr/bin/python
# -*- coding: UTF-8 -*-

from distutils.command.config import config

import yaml
import re
import sys
import os
sys.path.append(os.path.dirname(__file__) + "/..")
from fesql import convert
from feql import convert as feqlconvert

import unittest
import json

resource_path = os.path.dirname(__file__)

def abs_path(param):
    return resource_path + "/" + param

def case():
    # 获取当前脚本所在文件夹路径
    curPath = os.path.dirname(os.path.realpath(__file__))
    print(curPath)
    # 获取yaml文件路径
    yamlPath = os.path.join(curPath, "op_convert_case/data_originalc.yaml")
 
    # open方法打开直接读出来
    f = open(yamlPath, 'r', encoding='utf-8')
    cfg = f.read()
    data = yaml.load(cfg,Loader=yaml.FullLoader)  # 用load方法转字典
    return data

def conv(d):
    article_info = {}
    data = json.loads(json.dumps(article_info))

    app = {'feature_info': {}}
    data['app'] = app
    
    feature_info = {'target_entity': d['config'][0]['table_name'], 'target_entity_index': d['config'][0]['index'], 'target_label': 'c3' , 'target_pivot_timestamp':'c2','entity_detail':{}, 'relations': []}
    data['app']['feature_info'] = feature_info

    relations = [{'type':'1-1', 'time_windows': ['10:0','100:0','1d,1000:0s'],'window_delay':''
        ,'from_entity':d['config'][0]['table_name'],'from_entity_keys':[d['config'][0]['index']],'from_entity_time_col':'','to_entity':d['config'][1]['table_name'],
        'to_entity_keys':[d['config'][1]['index']],'to_entity_time_col':''},
        {'type':'SLICE','window_delay':'0s','to_entity':d['config'][1]['table_name'], 'from_entity':d['config'][0]['table_name'],'from_entity_keys':[d['config'][0]['index']],'to_entity_keys':[d['config'][1]['index']],
        'from_entity_time_col':'c2','to_entity_time_col':'c3','time_windows':['2147483645:0']},
        {'type':'1-M','window_delay':'2s','to_entity':d['config'][1]['table_name'], 'from_entity':d['config'][0]['table_name'],'from_entity_keys':[d['config'][0]['index']],'to_entity_keys':[d['config'][1]['index']],
        'from_entity_time_col':'c2','to_entity_time_col':'c3','time_windows':['10:0','112:0','1d,1000:0s','32d,100:0s']}
        ]
    data['app']['feature_info']['relations'] = relations

    entity_detail = {d['config'][0]['table_name']:{},d['config'][1]['table_name']:{}}
    data['app']['feature_info']['entity_detail'] = entity_detail

    t1 = {'features':[]}
    data['app']['feature_info']['entity_detail'][d['config'][0]['table_name']] = t1

    t2 = {'features':[]}
    data['app']['feature_info']['entity_detail'][d['config'][1]['table_name']] = t2
    
    lists = list()
    index=0
    while(index<len(d['config'])):
        for x in d['config'][index]['column']:
            name_type = x.split(' ')
#             import re
# ll = "a  b  c    d"
# print("re", re.split(r"[ ]+", ll))
            print(name_type)
            if(name_type[1]=='String'):
                dict = {'id': d['config'][index]['table_name']+'.'+name_type[0],'data_type':'SingleString','skip':'false', 'feature_type':name_type[1]}
            elif(name_type[1]=='Timestamp'):
                dict = {'id': d['config'][index]['table_name']+'.'+name_type[0],'data_type':'Timestamp','skip':'false', 'feature_type':name_type[1]}
            elif(name_type[1]=='Int' or name_type[1]=='Double'):
                dict = {'id': d['config'][index]['table_name']+'.'+name_type[0],'data_type':'ContinueNum','skip':'false', 'feature_type':name_type[1]}
            
            lists.append(dict)
        features = tuple(lists)
        # features = {'id':a+'.'+c, 'data_type':'SingleString', 'skip':'false', 'feature_type':'String'},{'id':a+'.'+'eventTime', 'data_type':'Timestamp', 'skip':'false', 'feature_type':'Timestamp'},{'id':a+'.'+'f_index', 'data_type':'SingleString', 'skip':'false', 'feature_type':'String'}
        table_n=d['config'][index]['table_name']
        data['app']['feature_info']['entity_detail'][table_n]['features']= features
        lists.clear()
        index=index+1

    article = json.dumps(data)
    return article

def save_file(path, item):
        # 先将字典对象转化为可写入文本的字符串
        item_str = yaml.dump(item,default_flow_style=False,sort_keys=False)
        print(item_str)
        try:
             with open(path, "w", encoding='utf-8') as f:
                    f.write(item_str + "\n")
                    print("^_^ write success")
        except Exception as e:
            print("write error==>", e)

def fesql(op_file, config_file, cfg_is_info=False, debug=False):
    config_all = json.loads(config_file)   #加载配置文件
    if debug:
        print(config_all)
    real_config = config_all if cfg_is_info else config_all['app']['feature_info']
        
    ok, sql, sql_config, fe = convert.to_sql(op_file, real_config)   #true sql语句  sql结构  sql特征
    assert ok   #断言
    # sign(fe) has 2 more lines:
    # # start fe code
    # w_feature_output = window(table=sql_table, output="w_output_feature_table")
    filtered, _, _ = feqlconvert.remove_op(op_file, real_config)
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
    config_all = json.loads(config_file)
    real_config = config_all if cfg_is_info else config_all['app']['feature_info']
    if debug:
        print(real_config)
        
    ok, feql, column, sign, _ = feqlconvert.get_feql(
        op_file, real_config)
    assert ok
        # sign has 1 more line:
        # w_feature_output = window(table=xxx, output="w_output_feature_table")
    _, _, good = feqlconvert.remove_op(op_file, real_config)
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
        data=case()
        print("**********#########*********")
        print(data)
        article=conv(data)
        print(article)
        print("**********#########*********")
        str='\n'.join(data['fz'])

        print(str)
        # print(type(data['fz']))
        print("读取原数据")
        sql, fe = fesql(str,article)
        column, sign = feql(str,article)
        print("Here"+"Start"+"\n\n\n\n\n")

        print(column)
        print("Here"+"End")
        print(sql)
       
        sql_after = re.sub('\n'+' *', '\n', 'select' + sql.split('select',1)[1])
        sql_after = re.sub(' *'+'\n', '\n', sql_after)

        data['sql']=sql_after
        data['column']=column
        print(data['sql'])
        save_file(abs_path('result.yaml'),data)
     
if __name__ == '__main__':
    unittest.main()
