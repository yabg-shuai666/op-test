
# import json
# import unittest
# import json

# from distutils.command.config import config
# from tkinter import N
# import yaml

# # fmt:off

# # fmt:off
# import sys
# import os
# def conv(d):
#     article_info = {}
#     data = json.loads(json.dumps(article_info))

#     app = {'feature_info': {}}
#     data['app'] = app
    
#     feature_info = {'target_entity': d['config']['table_name'], 'target_entity_index': d['config']['index'], 'target_label': '' , 'entity_detail':{}, 'relations': []}
#     data['app']['feature_info'] = feature_info
    
#     relations = []
#     data['app']['feature_info']['relations'] = relations

#     entity_detail = {'t1':{} }
#     data['app']['feature_info']['entity_detail'] = entity_detail

#     t1 = {'features':[]}
#     data['app']['feature_info']['entity_detail']['t1'] = t1
    
#     lists = list()
#     for x in d['config']['column']:
#         if(x['datatype']=='String'):
#             dict = {'id': d['config']['table_name']+'.'+x['name'],'data_type':'SingleString','skip':'false', 'feature_type':x['datatype']}
#         elif(x['datatype']=='Timestamp'):
#             dict = {'id': d['config']['table_name']+'.'+x['name'],'data_type':'Timestamp','skip':'false', 'feature_type':x['datatype']}
#         elif(x['datatype']=='Int' or x['datatype']=='Double'):
#             dict = {'id': d['config']['table_name']+'.'+x['name'],'data_type':'ContinueNum','skip':'false', 'feature_type':x['datatype']}
#         lists.append(dict)
#     features = tuple(lists)
#     # features = {'id':a+'.'+c, 'data_type':'SingleString', 'skip':'false', 'feature_type':'String'},{'id':a+'.'+'eventTime', 'data_type':'Timestamp', 'skip':'false', 'feature_type':'Timestamp'},{'id':a+'.'+'f_index', 'data_type':'SingleString', 'skip':'false', 'feature_type':'String'}
#     data['app']['feature_info']['entity_detail']['t1']['features']= features

#     article = json.dumps(data)
#     return article

# def test():
#     article_info = {}
#     data = json.loads(json.dumps(article_info))

#     app = {'feature_info': {}}
#     data['app'] = app
    
#     feature_info = {'target_entity': "a", 'target_entity_index':'b', 'target_label': '' , 'entity_detail':{}, 'relations': []}
#     data['app']['feature_info'] = feature_info
    
    

#     entity_detail = {'t1':{} }
#     data['app']['feature_info']['entity_detail'] = entity_detail

#     relations = [{'type':'type', 'time_windows': ['10:0','100:0','1d,1000:0s'],'window_delay':''
#         ,'from_entity':'','from_entity_keys':[],'from_entity_time_col':'','to_entity':'',
#         'to_entity_keys':[],'to_entity_time_col':''}]
#     data['app']['feature_info']['relations'] = relations


#     # time_windows = ['10:0','10:0']
#     # data['app']['feature_info']['relations']['time_windows'] = time_windows

#     # from_entity_keys = []
#     # data['app']['feature_info']['relations']['from_entity_keys'] = from_entity_keys

#     # to_entity_keys = []
#     # data['app']['feature_info']['relations']['to_entity_keys'] = to_entity_keys

    

#     t1 = {'features':[]}
#     data['app']['feature_info']['entity_detail']['t1'] = t1
    
#     lists = list()
    
#     features = tuple(lists)
#     # features = {'id':a+'.'+c, 'data_type':'SingleString', 'skip':'false', 'feature_type':'String'},{'id':a+'.'+'eventTime', 'data_type':'Timestamp', 'skip':'false', 'feature_type':'Timestamp'},{'id':a+'.'+'f_index', 'data_type':'SingleString', 'skip':'false', 'feature_type':'String'}
#     data['app']['feature_info']['entity_detail']['t1']['features']= features

#     article = json.dumps(data)
#     print(article)
#     return article


# class TestConvert(unittest.TestCase):
#     def test_window_union_new_key(self):
#         test()

# if __name__ == '__main__':
#     unittest.main()



