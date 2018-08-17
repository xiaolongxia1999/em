# -*- coding: utf-8 -*-
import numpy as np
import pandas as pd
from GA.test import non_dominated_sort
def config():
    pass

def get_data(path,with_header):
    return pd.DataFrame.from_csv(path, header=with_header)

"""
to get raw dataframe to be transformed, only the fields to be analyzed reserved.
so,we will get a smaller size of row and column 
"""
def get_transformed_df(raw_df,columns_list):
    raw_df.columns = [columns_list]
    return raw_df

def get_columns_list_from_csv(path):
    df = pd.read_csv(path,header=0,encoding="utf-8")
    print("in get_columns_list_from_csv--------------------------")
    print(df)
    # 或者list = sorted(set(df["强相关可调参数"]) )
    #去重
    list1 = list(set(df["decisionVariable"]))
    return list1



if __name__ == '__main__':
    # front = []
    #获取要分析的“决策变量”的df

    # path_goal = 'E:/bonc/工业第四期需求/数据/优化分析的目标及点位-oneCol.csv'
    # input_data_path = r"E:\bonc\工业第四期需求\数据\result0807\reslut-col3-0816.csv"
    # out_put_path = r"E:/bonc/工业第四期需求/数据/out/selectedVariable0816.csv"
    #
    # df0 = pd.read_csv(path_goal)
    # print(df0)
    # # f = open(path_goal)
    # df = pd.read_csv(input_data_path,header=0,encoding='utf-8')
    # another_varaible = ['CZ3-FC7007','CZ3-FIC7009','CZ3-FI7006']
    # list1 = get_columns_list_from_csv(path_goal)
    # #list的extend返回空值，是直接对list进行了修改、更新
    # list1.extend(another_varaible)
    #
    # df1 = df[list1]
    # #待分析变量写到本地csv
    # df1.to_csv(out_put_path)
    # print(df1)


    #读取"决策变量df"
    path = r"E:\bonc\工业第四期需求\数据\out\selectedVariable0816.csv"
    df1 = pd.read_csv(path,header=0,index_col=0)
    # print(df1)

    #生成"目标变量df"
    series1 = df1['CZ3-TI7007'] / df1['CZ3-FC7002'] * 100
    series2 = df1['CZ3-FIC7009'] /df1['CZ3-FC7002'] * 100
    series3 = df1['CZ3-FI7017'] + df1['CZ3-FI7018'] + df1['CZ3-FI7019'] + df1['CZ3-FI7020']
    series4 = (df1['CZ3-FI7006'] * 0.0000899) / df1['CZ3-FC7002'] * 100

    #构造目标变量值的“最优解”
    df2 = pd.DataFrame([series1,series2,series3,series4]).transpose()
    # print(df2.iloc[0:100,:])

    #计算前沿面，作为可行解（初始种群）——————存疑，是否需要加入“劣质解”作为种群的一部分？

    #先只取100行作计算
    df2 = df2.iloc[0:300,:]

    #获取分层的种群
    population = non_dominated_sort(df2,2)
    print("population is -----------------------------------")
    print(population)
    #获取pareto前沿面，
    front1 = population[0]
    print("front is -----------------------------------")
    print(front1)
    print("front length--------------------------------")
    print(len(front1))

    # path = r""
    # columns_list = []
    # df = get_data(path,True)
    # get_transformed_df(df)