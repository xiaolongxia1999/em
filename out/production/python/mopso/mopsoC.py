import numpy as np
from mopso.fitness_funs import *
import mopso.init as init
import mopso.update as update
from mopso.plot import *
import pickle


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

        self.first_pareto = ()

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
        #历史可行解————用于作为初始建议
        self.first_pareto = self.in_, self.fitness_

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
        # self.plot_.show(self.in_, self.fitness_, self.archive_in, self.archive_fitness, -1)
        for i in range(cycle_):
            print("第%d次迭代:-----------------------------------------------------------" %i)
            self.update_()
            #self.plot_.show(self.in_, self.fitness_, self.archive_in, self.archive_fitness, i)
        #最后一次显示图形
        self.plot_.show(self.in_, self.fitness_, self.archive_in, self.archive_fitness, i)
        return self.archive_in, self.archive_fitness

    # def save(self, path, mode=None, form='pickle' ):
    def save(self, path, form='pickle' ):
        data = (self.archive_in, self.archive_fitness)
        try:
            if form == None:
                form == 'pickle'

            if  form == "pickle":
                path1 = path + ".pickle"
                print(path1)
                f = open(path1, 'wb')
                pickle.dump(data, file=f)
                f.close()
            elif form == 'json':
                path1 = path + ".json"
                f = open(path1, 'w')
                json.dump(data, fp=f)
                f.close()
            print("save model success------------")
        except:
            print("form error: form supports 'pickle ' or 'json' only")

        # if form == None or form == 'pickle':
        #     path = path + 'pickle'
        #     print(path)
        #     f = open(path, 'wb')
        #     pickle.dump(data, file=f)
        # elif form == 'json':
        #     path =  path + 'json'
        #     f = open(path, 'w')
        #     json.dump(data, f)

    def load(self, path, form='pickle'):
        print("path is %s" %path)
        if form ==None or form == 'pickle':

            path = path + '.pickle'
            print("path is %s" %path)
            f = open(path, 'rb')
            obj1 = pickle.load(f)
            print(type(obj1))
            return obj1
        elif form == 'json':
            path = path + '.json'
            f = open(path, 'r' , encoding='utf-8')
            obj1 = json.load(f)
            # f.close()
            return obj1
        else:
            print("form should be only 'pickle' or 'json', default 'pick'")


#专用于预测
class MopsoModel(Mopso):
    def __init__(self, particles, w, c1, c2, max_, min_, thresh, mesh_div,conf, fit_good_in, fitness_good_in):
        super(MopsoModel, self).__init__(particles, w, c1, c2, max_, min_, thresh, mesh_div,conf, fit_good_in, fitness_good_in)

    def self_learn(self, data=(), theory_pareto=()):
        #历史最优解
        history_pareto = self.fit_good_in, self.fitness_good_in
        #获取self.in_和self.fitness_
        self.in_ = data
        #此处为self.fitness_赋值
        self.evaluation_fitness()

        #目前，只要实时数据“data”没有被“历史最优解”占优，就加入“历史最优解”——作为历史经验的根据（而不用于外部储备集的更新，交给第二天的训练去做）
        # all_fit_in = np.vstack(data[0], self.fit_good_in)
        # all_fitness_in = np.vstack(data[1], self.fitness_good_in)



        #@0925待实现
        # ----------------------------------------------------------
        # ----------------------------------------------------------
        # 此处思路整理下：————假定实时流（一条数据或一小批数据[一小批数据更通用]为A， 当天历史最优解为B， 当天理论最优解为C
        # 1.写入csv,维护2个csv——一个“当天的历史最优解.csv”, 一个“当天的理论最优解archive_in.csv”
        #     if  A>B , A追加到B(即使B中一部分被剔除了，说明A本身就较优，根据最小角度的全局粒子，就是其本身，然后就不作优化推荐而已)
        #     if  A>C, A追加到C中
        # 2.提供“优化方案”
        #     2.1 初始方案：根据“最小粒子角度”，A和B进行Pareto比较，找到所有对A占优的解B*(如果为空，则初始方案为A本身，维持现状)
        #         获取与A角度最小的“历史可行解”（为自身或者B中某一粒子b), 然后以之为“初始方案”
        #             其中，如果A比b好，则不作优化，仍为A， 如果b比A好，则初始方案为b粒子
        #     2.2 进一步优化方案————用户可调参数α，介于（0,1）之间：
        #         # 一、根据“最小粒子角度”,找出和A角度最小的C中某个粒子c，
        #         一、 A和C进行Pareto比较， 找到所有对A占优的解C*， 从C*中，找到与A小角度的粒子c
        #         if b和c都存在：
        #             if b > c:
        #                 则另α = 0
        #             else:
        #                 则α = α
        #         根据公式：推荐的位置-- recommend_particles =  b + α* （c - b)
        #                   可能的优化比率——recommend_optimize_level = fitness(recommend_particles) - fitness(A)






        # if pa.judge_one_against_other(data[1], self.fitness_good_in):
        #     new_in = np.vstack(data, history_pareto[0])
        #     new_fitness = np.vstack(self.fitness_, history_pareto[1])
        #     new_front = pa.Pareto_(new_in, new_fitness)
        #     #获取data和历史最优解比较后的粒子
        #     updated_front_in, updated_front_fitness = new_front.pareto()
        #
        #
        #
        #
        #
        #     # new_particles_in = np.vstack(data[0], self.fit_good_in)
        #     # np.vstack(data[1], self.fitness_good_in)
        #     #获取字段名，生成新dataframe，写入csv
        #     fields_set = self.conf.get_conf()[0]
        #     data_path = self.data_path
        #     #将新粒子位置，加入到
        #     df = pd.DataFrame(updated_front_in, columns=fields_set)
        #     #覆盖原csv
        #     df.to_csv(data_path, header=0, mode='w')


























