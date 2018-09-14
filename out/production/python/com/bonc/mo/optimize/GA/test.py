import math
from GA.NsgaII import  *
import pandas as pd
from pandas import *

print(1)
# df = pd.DataFrame([[7, 8], [8, 7], [12, 6], [14, 5], [6, 1], [2, 3], [5, 4], [3, 4], [6, 2]])
# row_size =len(df)
# col_size = len(df)
front = []

def compare_dominate(series1,series2):
    #返回Series对象，里面全为True或者False
    series3 = series2 >= series1
    # print(series3)
    #若series3中包含False，则为True,表示只要series2非占优于series1，则series1为非占优的
    #series3.values很重要，不接values，则表示False 是否在index的集合中，而不是在值的集合中————因series2 >= series1此时是一个k/v对了，默认in表示是否在k的集合中
    if False not in series3.values:
        return False
    else:
        return True

#print(df)


def non_dominated_sort(df,iteration_num):
    group_num = 0
    group = []
    # df1 = pd.DataFrame()
    row_size = len(df)
    #后面会截取行号，保留行号，所以此处不能简单实用range（len(df)),而应该是range(start,start+len(df))
    # start = list(df.iloc[0:1,:].index)[0]
    index_raw = df.index.tolist()
    start = df.index[0]
    end = start + len(df)
    #print("start and end-------------------")
    #print(start)
    #print(end)
    #不要使用range，因为
    # for i in range(start,end):
    for i in index_raw:
        is_dominated = []
        # for j in range(start, end):
        for j in index_raw:
            if i != j:
                is_dominated.append(compare_dominate(df.loc[i], df.loc[j]))
        # 如果第i个行与剩余其他行比较，没有False(即总为True)，则添加到group中
        if False not in pd.Series(is_dominated).values:
            group.append(i)
    front.append(group)
    #print("df.index--")
    #print(df.index)
    current_df_index = df.index
    #这里的删除有问题——-----------------------------------------------------------------------------
    #group中，元素在df.index中的位置
    removeIndex = [np.array(current_df_index).tolist().index(group[i]) for i in range(len(group))]
    #print(removeIndex)
    others = np.delete( np.array(df.index) ,removeIndex ,axis=0 ).tolist()
    #print("group------------------")
    #print(group)
    #print("df--------------------")
    #print(df)
    #print(df.index.tolist())
    #print("others---------")
    #print(others)
    #print(type(others))
    #这里不要用iloc，会有奇怪的问题
    df = df.ix[others,:]
    #print("others11111---------")
    #print(df)

    # df = pd.DataFrame.reindex(df,[i for i in range(len(df))])
    #print("what")

    if len(df) == 0 or group_num == iteration_num:
        return front
    else:
        group_num += 1
        return non_dominated_sort(df,group_num)
#
# if __name__ == '__main__':
#     # path = r""
#     # df = pd.read_csv(path, header=0, encoding='UTF-8')
#     df = pd.DataFrame([[7, 8], [8, 7], [12, 6], [14, 5], [6, 1], [2, 3], [5, 4], [3, 4], [6, 2]])
#     front = []
#     f = non_dominated_sort(df)
#     print(f)



#
# group = []
# front = []
# df1 = pd.DataFrame()
# for i in range(row_size):
#
#     is_dominated = []
#     for j in range(row_size):
#         if i != j:
#             is_dominated.append(compare_dominate(df.loc[i],df.loc[j]))
#     #如果第i个行与剩余其他行比较，没有False(即总为True)，则添加到group中
#     if False not in pd.Series(is_dominated).values :
#         group.append(i)
# front.append(group)
# df1 = df.iloc[group]
#
# print(front)


# def sort_by_dominate(dataframe):




# def index_of(a,list):
#     for i in range(0,len(list)):
#         if list[i] == a:
#             return i
#     return -1
#
# #Function to sort by values
# def sort_by_values(list1, values):
#     sorted_list = []
#     while(len(sorted_list)!=len(list1)):
#         if index_of(min(values),values) in list1:
#             sorted_list.append(index_of(min(values),values))
#         values[index_of(min(values),values)] = math.inf
#     return sorted_list

#
# list0 = [1,5,6,0,5,7,4,21]
# # sorted = sorted(list0)
# [print(i) for i in sorted ]

#
# if __name__ == '__main__':
#     list0 = [7,8,12,14,6,2,5,3,6]
#     list1 = [8,7,6,5,1,3,4,4,2]
#
#     list2 = [ [list0[i],list1[i] ] for i in range(len(list0))]
#     print(list2)
#     a = fast_non_dominated_sort(list0,list1)
#     print(a)
#     #
#     # 结果：
#     # [[0, 1], [2], [5, 7], [6, 3], [4]]
