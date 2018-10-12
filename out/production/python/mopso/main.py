
#(!) -*- coding: utf-8 -*-
import sys
sys.path.append("D:\\Anaconda3\\Lib\\site-packages")
#需要添加当前mopso所在的父目录为路径，python里面可以跑
sys.path.append("E:\IdeaWorkSpace\EnergyManagement4\EnergyManagement\python\python\MOPSO\src")
import  numpy as np
from mopso.mopsoC import  *
from utils.config import config
from mopso.pareto import Pareto_
#跑出来和原代码不一样，问题出在archiving.py中的clear_archiving类
# 见：https://blog.csdn.net/m0_38097087/article/details/79818348

#代码有错误的地方
#@ cl: 错误一
#@warn & error: 注意，此例子中，更新个体引导者时，占优符号的基于“大于”的， 即越大越优————————和求pareto时，正好冲突，是相反的，求pareto时，越小越好，——————二者必须要有一个要修改
#@cl : 即pareto.compare_()  和 update.compare_pbest()是不一致的，要修改其中一个——————（前者是目标函数值越小越优，后者是越大越优
def main():
    # 原始参数————在我的应用中，由于适应度函数，存在较大偏差，更多还是偏向于“历史出现过的数据”,因此，惯性权重要整大一点，相当于c1和c2都要大很多，才能减少误差导致的搜索方向偏差
    #w很大的缺点————收敛速度较慢，因为粒子速度太慢了，位置更新较慢
    # w  = 0.8
    # c1 = 0.1
    # c2 = 0.2

    # @cl调整参数，测试
    w  = 0.8
    c1 = 0.05
    c2 = 0.05

    # particles = 100
    cycle_ = 2
    mesh_div = 10           #网格参数，线性空间10等分
    thresh = 300            #外部储备集阀值，最多保留300个非占优解
    # min_ = np.array([0, 0])             #min_是n维线性空间的下界，此处维度为2,
    # max_ = np.array([10, 10])           #max_是其上界，此处维度为2

    # data_path = os.path.abspath(os.path.join(os.getcwd(), "../../data")) + os.sep + "dataset.csv"

    #python获取绝对路径: https://blog.csdn.net/junbujianwpl/article/details/75332141
    script_path = os.path.abspath(sys.argv[0])
    data_path = script_path + "../../data" + os.sep + "dataset.csv"

    print(data_path)
    json_path = os.path.abspath(os.path.join(os.getcwd(), "../../conf")) + os.sep + "fitness_list.json"

    save_path = os.path.abspath(os.path.join(os.getcwd(), "../../data")) + os.sep + "model"

    #读取data
    # json_path = os.path.abspath(os.path.join(os.getcwd(), "../../conf")) +os.sep+ "fitness_list.json"
    # data_path = os.path.abspath(os.path.join(os.getcwd(), "../../data")) +os.sep+ "fit_in.csv"
    # conf = config.Config(data_path, json_path)
    # conf.init()


    #加载json和data
    conf = load_conf(data_path,json_path)

    # #初始定一个无意义的边界，主要是为了调用evaluation_fitness()方法
    # min_ = np.arange(len(conf.goal_info))
    # max_ = np.arange(len(conf.goal_info))
    # mopso_ = Mopso(particles, w, c1, c2, max_, min_, thresh, mesh_div,conf)

    # @warn:mybounds确定每个向量分量的边界bounds，也即上面的min_,max_
    min_, max_, fit_in_origin, fitness_in_origin  = get_bounds(conf)
    #初始进行一次Pareto，加快搜索速度
    fit_in, fitness_in = Pareto_(fit_in_origin, fitness_in_origin).pareto()
    np.savetxt("./img_txt/first_pareto_fitness0919.txt", fitness_in)
    #粒子个数：按照最初的pareto解得到的粒子个数为准
    particles = fit_in.shape[0]

    mopso_1 = Mopso(particles, w, c1, c2, max_, min_, thresh, mesh_div,conf,fit_in, fitness_in)

    # pareto_in, pareto_fitness = mopso_.done(cycle_)
    pareto_in, pareto_fitness = mopso_1.done(cycle_)

    #序列化
    print("save model-------------------------------")
    mopso_1.save(save_path)
    print("load model-------------------------------")
    abc = mopso_1.load(save_path)
    # print(abc[0])


    np.savetxt("./img_txt/pareto_in.txt", pareto_in)
    np.savetxt("./img_txt/pareto_fitness.txt", pareto_fitness)
    print("done------------------------")

def load_conf(data_path,json_path):
    conf = Config(data_path, json_path)
    conf.process()
    return conf

def get_bounds(conf):
    fit_data_init = conf.data.values
    fitness_data_init = Mopso.evaluatin_fitness_first(conf)
    # 此处有点问题————在更新阶段的，期望的min_和max_传入的是位置空间，而不是目标空间
    # 另外，计算网格id貌似，还是应该根据位置空间计算。。。。
    #粒子位置边界，还是应该按照fitness_data_init来
    min_ =  np.min(fit_data_init, axis=0)
    max_ =  np.max(fit_data_init, axis=0)
    return min_, max_, fit_data_init,fitness_data_init

if __name__ == "__main__":
    main()