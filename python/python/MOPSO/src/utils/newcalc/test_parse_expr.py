import re
import os
import numpy as np
import pandas as pd
import math
import random
import json
import mopso.pareto as pa
from functools import reduce


#传入表达式和dataframe————根据dataframe的column名和表达式，计算得到新的一列
def compute_expr_df(expr, df):
    # print("expr is :" + str(expr))
    cols = df.columns
    # print("df columns is:" + str(cols))
    return compute_expr_list(expr,cols)


#仅传入表达式和一个“字段名”计算出新的一列————前提字段名必须是某个df的字段名
def compute_expr_list(expr, column_list):
    # print("column_list is %s------------------------------------",column_list)
    # cols = df.columns
    for i in column_list:
        if expr.find(i) == -1:
            continue
        else:
            expr = expr.replace(i,"df[\""+i+"\"]")
    #new_df: type of Series, not DataFrame
    new_series = eval(expr)
    return new_series

# def calc_fitness_matrix:
#输入DataFrame，计算fitness矩阵
def calc_fitness_df(df_in, expr_list):
    # print(df_in)
    # print("expr_list is:  "+str(expr_list))
    series_name_list = [("fitness_"+str(i+1)) for i in range(len(expr_list)) ]
    # print("series_name_list is-----------")
    # print(series_name_list)
    new_df1 = pd.DataFrame(columns=series_name_list)
    fitness_size = len(series_name_list)
    # print("fitness_size is "+str(fitness_size))
    for i in range(fitness_size):
        new_df1[series_name_list[i]] = compute_expr_df(expr_list[i], df_in)
    #返回DataFrame
    return new_df1

def calc_fitness_out_ndarray(df_in, expr_list):
    return calc_fitness_df(df_in, expr_list).values

#输入nparray，计算fitness矩阵
def calc_finess_nparry(nparray,expr_list, fields_set):
    fields_size = nparray.shape[1]
    # fields1 = ["field_".join(str(i)) for i in range(fields_size)]
    # df_from_nparry = pd.DataFrame(nparray,columns=fields1)
    df_from_nparry = pd.DataFrame(nparray, columns=fields_set)
    #返回DataFrame
    return calc_fitness_df(df_from_nparry, expr_list)

#输入nparry,输出nparray形式的适应度矩阵
#此处cols_list对应于所有“决策变量名”，是一个含所有“字段名”的列表————所以最好使用dataframe操作
def calc_fitness_out_nparray(nparray,expr_list,fields_set):
    new_df = calc_finess_nparry(nparray, expr_list,fields_set)
    #返回ndarray
    return new_df.values


def generate_df_from_fields(df, fields):
    return df[fields]


#获取决策变量的列表后，拟合获取权重weights——————————每一组（决策变量，fitness函数表达式expr)，计算出一个weight，(list[str])类型
#暂时为空，需要拟合函数
#@warn：写拟合函数——0914
def fit_dv_fitness(df,feature_cols,label_col):
    weights = []
    #例如线性回归
    def linear_regression(df,feature_col,label_col):
        pass
    return weights

#解析json,获取目标函数表达式
def get_fitness_expr():
    #目前配置文件路径写死
    file_path = os.path.abspath(os.path.join(os.getcwd(), "../../../conf")) +os.sep+ "fitness.json"
    f = open(file_path,"r")
    params = json.load(f)
    #表达式列表
    expr_list = []
    goals_keys = params['goals_function'].keys()
    #生成元组列表  ('goal1',["CZ3-FC7002","CZ3-TI7001","CZ3-TI7003","CZ3-TI7005","CZ3-TI7007"])这种元组元素的列表
    dv_fitness_tuple_list = []
    for key in range(len(goals_keys)):
        # print(type(goals_keys))
        #需要用json的多层键去获取“嵌套字典的值”
        fields_temp = params['goals_function'][key]['decision_variables']
        dv_fitness_tuple_list.append((key, fields_temp))

        #@warn0914——待续写
        # #@warn：此处以后要修改，只是测试，简单使用线性回归，模拟
        # #目前是随机生成拟合“权重”，（0,1）之间——————————————用线性回归模拟拟合函数
        # weights = [random.uniform(0, 1) for i in range(len(key))]
        # #生成元组——————以key（比如'goal1'为键，防止后面目标与权重混淆
        # expr_list.append((key, calc_fitness_funtion(fields_temp, weights)))



if __name__ == "__main__":

    # expr = "CZ3@FC7007/CZ3@FC7002*100"
    expr1 = "CZ3-FC7007/CZ3-FC7002*100"
    expr2 = "CZ3-FIC7009/CZ3-FC7002*100"
    expr3 = "CZ3-FI7017+CZ3-FI7018+CZ3-FI7019+CZ3-FI7020"
    expr4 = "CZ3-FI7006*0.0000899/CZ3-FC7002*100"
    #1:表达式列表——一个字符串表达式的“列表”————————————————————————
    #@warn:这个表达式列表只用来初始化“粒子群”
    #@warn:用户还需要为每个fitness目标，提供一个决策变量列表，我们将其用于“生成适应度函数”————这里写好了“表达式”的计算过程
    expr_list1 = [expr1,expr2,expr3,expr4]
    #2：表达式矩阵
    path = r'E:\bonc\工业第四期需求\数据\out\去除负9和0等异常值的DF0904.csv'
    df = pd.read_csv(path, header=0)
    array1 = df.values

    #基于ndarray的操作
    fields_set = df.columns
    array_result = calc_fitness_out_nparray(array1, expr_list1,fields_set)
    print("array result---------------------------------------------------------------")
    print(array_result)


    # s1 = compute_expr(expr1, df)
    # print(s1)
    #3:计算表达式

    #基于DataFrame的操作
    df1 = calc_fitness_df(df, expr_list1)
    print("df1 is --------------------------------------------------------------------")
    print(df1)

    def calc_fitness_funtion(decision_variables, weights):
        weights_str = list(map(lambda x: str(x), weights))
        weighted_expr_list = [(weights_str[i] + "*" + decision_variables[i]) for i in range(len(decision_variables))]
        weighted_expr_str = reduce(lambda x, y: x + "+" + y, weighted_expr_list)
        return weighted_expr_str
    #weights
    # decision_variables = ["CZ3-FC7002","CZ3-TI7001","CZ3-TI7003","CZ3-TI7005","CZ3-TI7007"]
    # weights = [ random.uniform(0,1) for i in range(len(decision_variables))]
    #
    # weighted_expr_str = calc_fitness_funtion(decision_variables,weights)
    #
    #
    #
    #
    # print(weighted_expr_str)

    #Pareto之决策变量-ndarray
    # array_fit_in = np.array(list(map(lambda x: df[x].values, decision_variables)))
    # print("array_fit_in---------------")
    # print(array_fit_in)
    # print(type(array_fit_in))

    #Pareto之fitness矩阵—ndarray
    # array_fitness_in =
    #读取conf/fitness.json
    file_path = os.path.abspath(os.path.join(os.getcwd(), "../../../conf")) +os.sep+ "fitness.json"
    # file_path = os.path.join(os.path.dirname(__file__) , '/../../../conf/fitness.json')
    print(file_path)
    f = open(file_path,"r")
    params = json.load(f)
    print(params)

    #生成expr_list
    expr_list = []
    goals_keys = params['goals_function'].keys()
    print("goals_keys")
    #fitness矩阵产生
    for key in goals_keys:
        #@warn：此处以后要修改，只是测试，简单使用线性回归，模拟
        #目前是随机生成拟合“权重”，（0,1）之间——————————————用线性回归模拟拟合函数
        weights = [random.uniform(0, 1) for i in range(len(key))]
        # print(type(goals_keys))
        #需要用json的多层键去获取“嵌套字典的值”
        fields_temp = params['goals_function'][key]['decision_variables']
        expr_list.append(calc_fitness_funtion(fields_temp, weights))

    #利用expr_list和df，生成fitness矩阵，一个ndarray
    print("expr_list is-----------------------------------")
    print(expr_list)
    fitness_in_array = calc_fitness_df(df, expr_list)
    print(fitness_in_array)

    print("list1------------")
    print(expr_list)

    fit_in_out, fitness_in_out =  pa.Pareto_(df.values, fitness_in_array.values).pareto()
    print(pd.DataFrame(fitness_in_out, columns=fitness_in_array.columns))

    # calc_fitness_df(df, weighted_expr_str).values
    # array_fitness_in =
    # a = pa.Pareto_






























# col_set = df.columns
# # col_set = ['CZ3@TI7003', 'CZ3@FI7020', 'CZ3@TI7041', 'CZ3@TI7001', 'CZ3@TI7005',
# #        'CZ3@PI7101', 'CZ3@FC7002', 'CZ3@TI7043', 'CZ3@PI7008', 'CZ3@FI7017',
# #        'CZ3@FI7019', 'CZ3@TI7007', 'CZ3@FI7018', 'CZ3@FC7007', 'CZ3@FIC7009',
# #        'CZ3@FI7006', 'label1', 'gitwo', 'githree', 'gifour']
#
# for i in col_set:
#     if expr.find(i) == -1:
#         pass
#     else:
#         expr = expr.replace(i,"df[\""+i+"\"]")
#         print(expr)
#
# print("last")
# print(expr)
# a = eval(expr)
# print(eval(expr))
# print(type(a))



