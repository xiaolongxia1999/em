import  numpy as np
from mopso.mopsoC import  *

#跑出来和原代码不一样，问题出在archiving.py中的clear_archiving类
# 见：https://blog.csdn.net/m0_38097087/article/details/79818348

#代码有错误的地方
#@ cl: 错误一
#@warn & error: 注意，此例子中，更新个体引导者时，占优符号的基于“大于”的， 即越大越优————————和求pareto时，正好冲突，是相反的，求pareto时，越小越好，——————二者必须要有一个要修改
#@cl : 即pareto.compare_()  和 update.compare_pbest()是不一致的，要修改其中一个——————（前者是目标函数值越小越优，后者是越大越优
def main():
    # 原始参数
    w  = 0.8
    c1 = 0.1
    c2 = 0.2

    # @cl调整参数，测试
    # w  = 0.4
    # c1 = 0.3
    # c2 = 0.3

    particles = 100
    cycle_ = 30
    mesh_div = 10           #网格参数，线性空间10等分
    thresh = 300            #外部储备集阀值，最多保留300个非占优解
    min_ = np.array([0, 0])             #min_是n维线性空间的下界，此处维度为2,
    max_ = np.array([10, 10])           #max_是其上界，此处维度为2
    mopso_ = Mopso(particles, w, c1, c2, max_, min_, thresh, mesh_div)
    pareto_in, pareto_fitness = mopso_.done(cycle_)
    np.savetxt("./img_txt/pareto_in.txt", pareto_in)
    np.savetxt("./img_txt/pareto_fitness.txt", pareto_fitness)
    print("done------------------------")

if __name__ == "__main__":
    main()