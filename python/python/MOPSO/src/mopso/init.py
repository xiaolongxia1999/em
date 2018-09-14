import random
import numpy as np
from mopso.archiving import *
from mopso.pareto import *

#初始化例子， 得到例子的初始坐标——n个决策变量，则为n个坐标分量——此处是“随机化”,我们是根据历史数据，获取初始化的“历史可行解（淘汰掉被占优解，及劣解”
# 本例中使用的是二维空间，x和y都是[0,10]的线性空间
def init_designparams(particles, in_min, in_max):
    #示例中in_dim=2
    in_dim = len(in_max)
    #particles = 100, in_temp = 100 * 2 的二维矩阵， 也即100个 二维向量（因为有两个决策变量）
    in_temp = np.zeros((particles, in_dim))
    for i in range(particles):
        for j in range(in_dim):
            # 初始化位置，生成100个粒子，每个粒子的坐标分量，都是服从0-1均匀分布的点————在我的例子中，应该是“历史可行解"中的非占优解集，作为初始种群
            in_temp[i, j] = random.uniform(0, 1) * (in_max[j] - in_min[j]) + in_min[j]
    return in_temp

#初始化速度——即“学习率”,代表粒子的搜索速度
#本例中，在mppsoC类中__init__中定义，是0.05倍（max_ - min_) ，也即0.05 * （10 -0） = 0.5
def init_v(particles, v_max, v_min):
    #此处是2维空间，长度为2
    v_dim = len(v_max)
    #速度矩阵，也是100个粒子，每个粒子是二维的速度方向
    v_ = np.zeros((particles, v_dim))
    for i in range(particles):
        for j in range(v_dim):
            # 初始速度，每个粒子的两个速度分量，都是 [min_v, max_v] 之间的随机数
            v_[i, j] = random.uniform(0, 1) * (v_max[j] -v_min[j]) + v_min[j]
    return v_

#初始化个体最优——个体引导者， 就是粒子自己本身（第一次个体引导者就是粒子自己本身，没毛病）
#返回一个元组，有2个元素： 均为 100*2 的二维矩阵， 分别是 初始位置矩阵、初始适应度函数矩阵
#“历史最优解”, 初始是粒子本身，迭代一次后，粒子的新位置，会和原位置作“占优比较”————（1：其中一个占优，则其作为当代p_best;2：二者非占优，参看第二章）
#此处是通过update.py中完成，每迭代一次，都会更新p_best
def init_pbest(in_, fitness_):
    return in_, fitness_
#初始化外部最优解集
#此例子中，初始粒子的初始化中没有”占优解“这一步，我会在初始化粒子群时，进行”非占优解集“的计算，同时作为“初始粒子群、及初始外部解集”
def init_archive(in_, fitness_):
    pareto_c = Pareto_(in_, fitness_)
    curr_archiving_in, curr_archiving_fit = pareto_c.pareto()
    return curr_archiving_in, curr_archiving_fit

#初始化全局最优解——网格法（其实可以使用“粒子最小角度法”————每一个粒子都会有一个“全局最优解”, 每个“全局最优解”都各有多个粒子，叫“子粒子群”。）
#@cl此处是基于密度，而非基于角度的
def init_gbest(curr_archiving_in, curr_archiving_fit, mesh_div, min_, max_, particles):
    get_g = get_gbest(curr_archiving_in, curr_archiving_fit, mesh_div, min_, max_, particles)
    return get_g.get_gbest()
























