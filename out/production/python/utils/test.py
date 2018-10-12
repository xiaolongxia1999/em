import numpy as np
import random
import json
import os
import  pprint

# class demo(object):
#
#     def __init__(self, var1, var2):
#         self.var1 = var1
#         self.var2 = var2
#
#     def get_for(self, num):
#         for i in range(num):
#             num = num + 10
#             num = num + 10
#             print(str(num)+'\n')
#
#
# def get_index(num):
#     ran = random.uniform(0.0, 1.0)
#     print(ran)
#     for i in range(num):
#         if ran < 0.6:
#             print(ran)
#             return i
#
#
# # a =  get_index(100)
# # print(a)
#
#
# # a = demo(0,0)
# # b = a.get_for(5)
# # print(b)
#
# # print("this："+ str(b))
# # print(a)
# # print(get_for(5))
#
# #字典操作——增删改查、键或值的迭代器
#
# dic = {}
# dic1 = dic.copy()
# #增
# dic['sid'] = 27
# dic['jack'] = 24
# print(dic)
# #删
# dic.pop('sid')
# print(dic)
# #查
# print(dic.get('jack'))
#
# print(dic1.keys())
# print(dic1.values())
# print(dic1.items())
# [print(i) for i in dic1.keys()]
#
# json1 = {"jack": 20, "sid": 28}
# print(type(json1))
# print("-----------------------")
# print(json1)
#
#
# json2 = {"jack": 20, "sid": 28}
# print(type(json2))
# data = json.dumps(json2)
# print(data)
# print(type(data))

file_path = os.path.abspath(os.path.join(os.getcwd(), "../../conf")) + os.sep + "fitness_list.json"
# file_path = os.path.join(os.path.dirname(__file__) , '/../../../conf/fitness.json')
print(file_path)
f = open(file_path, "r")
params = json.load(f)
pprint.pprint(params)

print(type(params))

# print(params['goals_function'][0]['goal1']['decision_variables']['expr'])
a = params['goals_function'][0]['goal1']['expr']
print(a)
print(type(a))


