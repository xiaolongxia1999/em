import numpy as np
from mopso.fitness_funs import *
import mopso.init as init
import mopso.update as update
from mopso.plot import *


class Mopso:
    # def __init__(self, particles, w, c1, c2, max_, min_, thresh,conf, mesh_div=10):
    def __init__(self, particles, w, c1, c2, max_, min_, thresh, mesh_div,conf, fit_good_in, fitness_good_in):
        self.w, self.c1, self.c2 = w, c1, c2
        self.mesh_div = mesh_div
        self.particles = particles
        self.thresh = thresh
        self.max_ = max_
        self.min_ = min_
        self.max_v = (max_ - min_) * 0.05  #速度下限？？初始为 （[10,10] - [0,0]) * 0.05 = [0.5,0.5]
        self.min_v = (max_ - min_) * 0.05 * (-1)  #此处为[-0.5,-0.5]
        self.plot_ = Plot_pareto()

        self.conf = conf
        self.fit_good_in = fit_good_in
        self.fitness_good_in = fitness_good_in
    # #计算适应度
    # def evaluation_fitness(self):
    #     fitness_curr = []
    #     #一个n维矩阵matrix, shape接收的参数表示维度， 如，matrix.shape[0]， 表示matrix的第1维（axis=0)的元素个数，此处就是100，即100个粒子
    #     # self.in_表示输入的例子位置矩阵，100条2维的
    #     for i in range((self.in_).shape[0]):
    #         fitness_curr.append(fitness_(self.in_[i]))
    #     self.fitness_ = np.array(fitness_curr)

    #我的计算适应度函数，基于numpy和dataframe，以及我的表达式expr的解析
    def evaluation_fitness(self):
        fields_set,expr_list = self.conf.get_conf()
        self.fitness_ = my_fitness(self.in_, expr_list, fields_set)

    #@my 自定义的第一次计算适应度，静态方法
    def evaluatin_fitness_first(conf):
        fields_set,expr_list = conf.get_conf()
        in_ = conf.data.values
        fitness_ = my_fitness(in_, expr_list, fields_set)
        return fitness_

    def initialize(self):
        #self.in_ = init.init_designparams(self.particles, self.min_, self.max_)
        self.in_ = self.fit_good_in
        self.v_ = init.init_v(self.particles, self.min_v, self.max_v)
        self.evaluation_fitness()
        self.in_p, self.fitness_p = init.init_pbest(self.in_, self.fitness_)
        self.archive_in, self.archive_fitness = init.init_archive(self.in_, self.fitness_)
        print("self.archive_fitness-----------------------------------------------------------")
        print(self.archive_fitness)
        print(self.archive_fitness.shape)
        self.in_g, self.fitness_g = init.init_gbest(self.archive_in, self.archive_fitness, self.mesh_div, self.min_, self.max_, self.particles)

    def update_(self):
        self.v_ = update.update_v(self.v_, self.min_, self.max_v, self.in_, self.in_p, self.in_g, self.w, self.c1, self.c2)
        self.in_ = update.update_in(self.in_, self.v_, self.min_, self.max_)
        self.evaluation_fitness()
        self.in_p, self.fitness_p = update.update_pbest(self.in_, self.fitness_, self.in_p, self.fitness_p)
        self.archive_in, self.archive_fitness = update.update_archive(self.in_, self.fitness_, self.archive_in, self.archive_fitness, self.thresh, self.mesh_div, self.min_, self.max_, self.particles)
        self.in_g, self.fitness_g = update.update_gbest(self.archive_in, self.archive_fitness, self.mesh_div, self.min_, self.max_, self.particles)

    def done(self, cycle_):
        self.initialize()
        self.plot_.show(self.in_, self.fitness_, self.archive_in, self.archive_fitness, -1)
        for i in range(cycle_):
            self.update_()
            self.plot_.show(self.in_, self.fitness_, self.archive_in, self.archive_fitness, i)
        return self.archive_in, self.archive_fitness














