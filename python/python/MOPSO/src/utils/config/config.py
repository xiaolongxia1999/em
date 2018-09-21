import os
import json
import pandas as pd
import sys

class Config():
    def __init__(self, data_path, json_path):
        self.data_path = data_path
        self.json_path = json_path
        self.goal_size = 0
        self.goal_info = []         #a list of tuple , tuple(goal_id , goal_decision_variables_list, goal_fitness_expression )
        self.dict_ = dict()
        self.data = pd.DataFrame()


    #获取json字符串和data————每次需要先调用init,才会有值
    def init(self):
        #读取conf/fitness_list.json————————此处是有序的，因为是列表，得到的fitness是按用户输入的顺序获取的
        #file_path = os.path.abspath(os.path.join(os.getcwd(), "../../../conf")) +os.sep+ "fitness_list.json"
        file_path = self.json_path
        f = open(file_path,"r")
        self.dict_ = json.load(f)
        self.goal_size = len(self.dict_['goals_function'])
        # self.data = pd.read_csv(self.data_path, header=0)

    def get_goal_info(self):
        list1 = self.dict_['goals_function']
        for i in range(self.goal_size):
            id = list1[i]['goal']['id']
            decision_variables = list1[i]['goal']['decision_variables']
            expr = list1[i]['goal']['expr']
            self.goal_info.append((id, decision_variables, expr))

    def read_data(self):
        self.data = pd.read_csv(self.data_path, header=0)
        # return self.data
    def process(self):

        self.init()
        self.read_data()
        self.get_goal_info()

    #自定义json_path
    def set_json_path(self,json_path):
        self.json_path = json_path
    #自定义data_path
    def set_data_path(self,data_path):
        self.data_path = data_path

    # @warn 我的代码
    def get_conf(self):
        # json_path = os.path.abspath(os.path.join(os.getcwd(), "../../conf")) + os.sep + "fitness_list.json"
        # data_path = os.path.abspath(os.path.join(os.getcwd(), "../../data")) + os.sep + "dataset.csv"
        # conf = Config(data_path, json_path)
        # print(data_path)
        # conf.process()
        # self.process()
        # raw_data = conf.read_data()
        # dict1 = conf.dict_
        fields_set = self.data.columns
        goal_info = self.goal_info
        print("goal info is:\n ")
        print(goal_info)

        expr_list = []
        for i in range(len(goal_info)):
            expr_list.append(goal_info[i][2])
        return fields_set, expr_list


#
# if __name__ == '__main__':
#     # data_path = sys.argv[0]
#     # json_path = sys.argv[1]
#
#     data_path = os.path.abspath(os.path.join(os.getcwd(), "../../../data")) + os.sep + "dataset.csv"
#     json_path = os.path.abspath(os.path.join(os.getcwd(), "../../../conf")) + os.sep + "fitness_list.json"
#     # conf = Config(data_path, json_path)
#     # conf.process()
#     # print(conf.data)
#     # print(conf.goal_info)
#     conf = Config(data_path,json_path)
#     conf.process()
#     print(conf.data)
#     print(conf.goal_info)
