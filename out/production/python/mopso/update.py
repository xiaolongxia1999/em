import numpy as np
import random
from mopso.pareto import *
from mopso.archiving import  *

#v:速度
def update_v(v_, v_min, v_max, in_, in_pbest, in_gbest, w ,c1, c2):
    #粒子速度更新公式
    v_temp = w * v_ + c1 * (in_pbest - in_) + c2 * (in_gbest - in_)

    #@warn 为何有上述操作？？？
    #难道是限制粒子跑出“搜索空间”,导致bug?
    for i in range(v_temp.shape[0]):
        for j in range(v_temp.shape[1]):
            #@cl 如果粒子的新速度大于最大值，则置为最大值；小于最小值，则置为最小值————没毛病，还是有这种可能的，如果设定了“最小最大速度”,则基于此限制粒子速度在某一范围内
            if v_temp[i, j] < v_min[j]:
                v_temp[i, j] = v_min[j]
                if v_temp[i, j] > v_max[j]:
                    v_temp[[i, j]] = v_max[j]
    return v_temp

def update_in(in_, v_, in_min, in_max):
    #@cl每更新一轮，时间步长t增加1，得到新的例子位置
    in_temp = in_ + v_
    #@cl如同上面的速度更新公式，粒子的坐标也有范围限制，超出范围，则需要修正为临界点
    for i in range(in_temp.shape[0]):
        for j in range(in_temp.shape[1]):
            if in_temp[i, j] < in_min[j]:
                in_temp[i, j] = in_min[j]
            if in_temp[i, j] > in_max[j]:
                in_temp[i, j] = in_max[j]
    return in_temp

#@warn 注意，此例子中，占优符号的基于“大于”的， 即越大越优————————和求pareto时，正好冲突，是相反的，求pareto时，越小越好，——————二者必须要有一个要修改
#@cl
def compare_pbest(in_indiv, pbest_indiv):
    num_greater = 0
    num_less = 0
    #@cl 对于每个粒子的fitness的分量作比较，如果当前的in_indiv > 上一代的pbest_indiv, 则num_greater 加1， 否则num_less加1
    for i in range(len(in_indiv)):
        if in_indiv[i] > pbest_indiv[i]:
            num_greater = num_greater + 1
        if in_indiv[i] < pbest_indiv[i]:
            num_less = num_less + 1


    #原代码
    """
    if (num_greater > 0 and num_less == 0):
        return True
    elif (num_greater == 0 and num_less > 0):
        return False 
    """
    #
    #@warn经与作者交流，其确实写错了，所以此处修改过来——————in_indiv的分量全小于pbest_indiv的分量则占优，返回True;全大于则返回False————————下面已经修改过来
    #此处由3种情形：a占优b，b占优a，a和b互不占优
    #@cl 如果in_indiv 的分量，都大于pbest_indiv，则返回True————即前者完全占优
    if (num_greater > 0 and num_less == 0):
        return False
    #@cl 如果in_indiv的分量，全小于pbest_indiv，则返回False————即前者完全被占优，说明比不过最小者
    elif (num_greater == 0 and num_less > 0):
        return True
    #@cl 如果二者互不占优，则取随机数， 也即五五开————————此处可以参照第2章的，个体粒子引导者更新（是基于密度的，全局最优解的子粒子群个数作为密度）
    else:
        random_ = random.uniform(0.0, 1.0)
        if random_ > 0.5:
            return True
        else:
            return False

#@cl 更新“历史最优解pbest”——当前代的粒子，和上一代的pbest，作占优比较（就不用把所有的历史最优解作比较，因为每一次更新都和之前比较过）
#@cl 核心compare_best————
#@cl 代码逻辑——如果当前粒子占优，则更新为当前粒子，否则还是以原pbest作为该轮p_best
#@cl 此处维护了一个in_pbest和out_best的元组，各自均为一维向量，维护每一代中，各粒子的个体粒子引导者
#@cl 此处fitness_输入是和”新一代”，而out_pbest是指上一代中的pbest————如果新一代fitness_未被占优，则更新，否则不更新，仍为上一代pbest
def update_pbest(in_, fitness_, in_pbest, out_pbest):
    for i in range(out_pbest.shape[0]):
        if compare_pbest(fitness_[i], out_pbest[i]):
            out_pbest[i] = fitness_[i]
            in_pbest[i] = in_[i]
    return in_pbest, out_pbest

#@cl 更新外部储备集————将当前粒子的“坐标、适应度”矩阵，和上一代的“外部储备集”作连接（用np），再执行Pareto求占优解——逻辑比较简单
def update_archive(in_, fitness_, archive_in, archive_fitness, thresh, mesh_div, min_, max_, particles):
    pareto_1 = Pareto_(in_, fitness_)
    curr_in, curr_fit = pareto_1.pareto()
    in_new = np.concatenate((archive_in, curr_in), axis=0)
    fitness_new = np.concatenate((archive_fitness, curr_fit), axis=0)
    pareto_2 = Pareto_(in_new, fitness_new)
    curr_archiving_in, curr_archiving_fit = pareto_2.pareto()

    if((curr_archiving_in).shape[0] > thresh):
        clear_ = clear_archiving(curr_archiving_in, curr_archiving_fit, mesh_div, min_, max_, particles)
        curr_archiving_in, curr_archiving_fit = clear_.clear_(thresh)
    return curr_archiving_in, curr_archiving_fit

#@cl 仍然是archiving里的获取g_best方法
def  update_gbest(archiving_in, archiving_fit, mesh_div, min_, max_, particles):
    get_g = get_gbest(archiving_in, archiving_fit, mesh_div, min_, max_, particles)
    return get_g.get_gbest()




































