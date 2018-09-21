# 编写适应度函数，以数组传入
import numpy as np
import pandas as pd
import os
import re

from utils.config.config import Config
from utils.newcalc.parse_expr import *
# 适应度函数
# 此处是2个目标，即2个适应度函数

#此处用的numpy的ndarry，一般四则运算是应用在元素级的标量运算，如in_[0] - in_[1] 表示  第1列的元素 - 第2列的元素，  两个100个元素的列表相减，得到的还是100个元素的新列表
#那么fit_1相当于   对每行作运算（元素为 该行的 各元素）， 得到新的1行；  同理fit_2
#最终返回： [fit_1, fit_2] ，是一个新的 100行* 2列的2维矩阵， 存储着每个粒子的 2个目标函数值
def fitness_(in_):
    degree_45 =((in_[0] - in_[1]) ** 2 / 2) ** 0.5
    degree_135 = ((in_[0] + in_[1]) ** 2 / 2) ** 0.5

    # 原代码中的函数
    fit_1 = 1 - np.exp( -(degree_45) ** 2 /0.5) * np.exp( - (degree_135 - np.sqrt(200)) ** 2 / 250 )
    fit_2 = 1 - np.exp(-(degree_45) ** 2 /5) * np.exp( -(degree_135) ** 2 / 350)

    #新代码中的函数: x2 + y2, x + y 貌似没有显示在图3中
    # fit_1 = np.sqrt(in_[0]) + np.sqrt(in_[1])
    # fit_2 = in_[0] + in_[1]
    return  [fit_1, fit_2]


def my_fitness(in_, expr_list, fields_set):
    print("fields_set------------------------------")
    print(fields_set)
    array_result = calc_fitness_out_nparray(in_, expr_list, fields_set)
    return array_result



#---以上为我的适应度函数，根据用户传入的表达式和输入数据计算所得（通过解析表达式的方法）











"""
my code below -----------------------------------------------------------------
"""

# def t_numpy(arr1, arr2):
#     return  arr1 - arr2


#由于表达式中有不容易区分 运算符 和 破折号， 所以还需要用“正则表达式”解析之
#一个简单的方式，只针对本例， 先把"CZ3-"全部替换为"CZ3"  ————————

# 更完全的步骤是，要把”运算符”和“字段名”分别解析出来:
#     1.使用栈：
#         接收一个字符串形式的“表达式”,如果字符串的开头为字母，则认定为字段名的一部分， 然后从给定的字段名集合中，匹配集合中的字段，找到一个， 然后从该字符串中删除，得到一个新的字符串；
#         如果开头不是字母，而是“+、-、*、/”这种字符， 则将其定义为运算符
#     2.循环直到“表达式”长度为0
def parse_expr(expr="", one_set=set() ):
    print("expr is -------------------------------")
    print(expr)
    print("one_set is------------------------------")
    print(list(one_set))
    expr_result = []

    for i in one_set:
        if expr.find(i) == -1:
            continue
        else:
            #将发现的字符串全部替换为df[字符串]
            expr = expr.replace(i, "df[\"" + i + "\"]")

    # pattern = "\+|\-|\*|\/"
    # expr_list = re.split(pattern, expr)
    #
    # expr_new_array = []
    # while len(expr) > 0:
    #     for i in range(expr_list):
    #         if expr.startswith(expr_list(i)):
    #             expr_new_array

    #
    # while len(expr) > 0:
    #     if expr[0] in ['+', '-', '*', '/']:
    #         #删除第一个字符char
    #         expr_result.append(expr[0])
    #         expr = expr[1:]
    #     else:
    #         for i in one_set:
    #             # print(i)
    #             # print(expr.find(i))
    #             #第一次找到直接退出，添加表达式，进入下一次循环解析
    #             #不一定是从开始找到的
    #             if expr.startswith(i):
    #                 expr_result.append(i)
    #                 expr = expr[len(i):]
    #                 print("parse_expr-------------------------------------")
    #                 print(expr_result)
    #                 print(expr)
    #                 continue
    #             # if expr.find(i) != -1:
    #             #     expr_result.append(i)
    #             #     cut = expr.index(i)
    #             #     size = len(expr)
    #             #     expr = expr[0:cut] + expr[cut + len(i) - 1]
    #             #     print("expr is "+str(expr))
    #             #     # expr = expr.replace(i,"")
    #             #     break
    #             # else:
    #             #     print("Expression is wrong!Please check")
    # print("expr_result is ------------------------------------")
    # print(expr_result)
    # return expr_result
    return expr

#将list中字段名转成np.array对象，形成新的list
def list_transform(df,list1):
    list2 = []
    for i in list1:
        if i in ['+','-','*','/']:
            list2.append(i)
        else:
            list2.append(np.array(df[i]))
    return  list2
#
# def calc_list_include_nparray(list1):
#     init_array = list1[0]
#     for i in range(1, len(list1)):
#         init_array
#
# def find_first(expr="", one_set=set() ):
#     for i in range(len(expr) - 1):
#         if i.

#使用map高阶函数，map接收一个func作为参数，对迭代器里的每个元素使用该func，建议使用lambda匿名函数，可以灵活定义各种函数，如同scala中的map
def replace_new_with_list(list1, replace_old, replace_new):
    return list(map(lambda x: str(x).replace(replace_old,replace_new), list1))


# print(remove_str("djsdjfff",'dj'))
# print("--")
# print(replace_new_with_list(['CZ3-FC7002', 'CZ3-FC7002', 'CZ3-FC7002'], "CZ3-","CZ3@"))

def replace_field_with_ops(list1, replace):
    for i in list1:
        if replace in i:
            pass

def expr_transform1(expr, df_name):
    for i in range(len(expr)):
        if expr[i] not in ['+','-','*','/']:
            expr[i] = "".join([df_name,"['",expr[i],"']"])
    return expr




#eval——————字符串转成计算表达式，只要是变量均可——见eval用法：http://www.runoob.com/python/python-func-eval.html
def eval_expr(expr,old,new,dataframe):
    #需要先替换含运算符的字段名
    # expr = replace_new_with_list(expr, old, new)
    fields1 = dataframe.columns
    print("df.columns---------------------------------------------------------")
    print(fields1)
    expr1 = expr.replace(old, new)
    #解析成含运算符和字段名的列表
    expr2 = parse_expr(expr1, fields1)
    print("first-------------------------------------------------------------")
    print(expr1)
    print("second-------------------------------------------------------------")
    print(expr2)
    #替换列表中“含+-*/运算符”的字段， 如"CZ3-FI7007"替换成"CZ3FI7007"
    expr_str = "".join(expr2)
    #转换成新的expr的数组
    new_expr = expr_transform1(expr2, "df")
    print("new_expr------------------------------")
    print(new_expr)
    #new_expr转成字符串表达式
    new_expr_str = "".join(new_expr)
    #计算
    print("new_expr_str--------------------------------")
    print(new_expr_str)
    #@warn 返回：df['CZ@TI7003']/df['CZ@TI7003']*df['CZ@TI7003']  ，怎么会使一样的字段名呢？？？？？？？
    new_array = eval(new_expr_str)
    return new_array


# expr = "CZ3-FC7007/CZ3-FC7002*100"
# set1 = ["CZ3-FC7002","CZ3-TI7001","CZ3-TI7003","CZ3-TI7005","CZ3-TI7007","CZ3-TI7041","CZ3-TI7043","CZ3-PI7008","CZ3-PI7101","CZ3-FI7017","CZ3-FI7018","CZ3-FI7019","CZ3-FI7020","CZ3-FC7002","CZ3-TI7001","CZ3-TI7003","CZ3-TI7005","CZ3-TI7007","CZ3-FC7007","CZ3-FIC7009","CZ3-FI7006"]
#
#
# a = parse_expr(expr, set1)
# print(a)
# a1 = np.array(['1','3','4'])
# a2 = np.array(['1','2','3'])

#def array_ops(array1, array2, ops):
# def fitness_calc(df, expr=[]):
#     df1 = None
#     expr1 = []
#     for i in range(expr):
#         if expr
#     def (expr)
#
#     if expr[0] == "-":
#         expr = expr


# def operator(num1=0.0, num2=0.0, ops="+"):
#     return eval(str(num1)+ops+str(num2))

# a1 = np.array([1,3,4])
# a2 = np.array([1,2,3])


# print(a1 + a2)
# # print( eval(a1+a[1]+a2))
# a3 = list(map(lambda x,y:operator(x,y,a[1]) , a1,a2))
# print(a3)
# print(operator("1","2","-"))
# print(a3)




#
# # print(eval(a1))
# path = r'E:\bonc\工业第四期需求\数据\out\去除负9和0等异常值的DF0904.csv'
# df = pd.read_csv(path, header=0)
# fields = df.columns
#
# #将表达式中的
# old = "CZ3-"
# new = "CZ@"
# expr_a = "CZ3-FC7007/CZ3-FC7002*100"
# #先把字段名转正确
# # df的字段名也替换成不含运算符 的字段名
# df.columns = replace_new_with_list(fields, old, new)
# # expr = parse_expr(expr, fields)
#
# # print(expr_transform1(expr, "df"))
# # new_expr = "".join(expr_transform1(expr, "df"))
# # print(new_expr)
#
#
# print("good--------------")
# new_array1 = eval_expr(expr_a,old,new,df)
# print(new_array1)
# print("done---------------")
#
# # df[df['goal1']] = df[expr]
# # print(df)
# #
# # df1 = df[expr]
# # print(df1)


