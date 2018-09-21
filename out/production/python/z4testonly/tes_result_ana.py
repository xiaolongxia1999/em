import numpy as np
import pandas as pd
from mopso.pareto import Pareto_
import os

def new_df(path1, path2):
    arr1 = np.loadtxt(path1, dtype=np.float32)
    arr2 = np.loadtxt(path2, dtype=np.float32)

    size1 = arr1.shape[0]
    size2 = arr2.shape[0]
    fields_size = arr1.shape[1]

    new_arr = np.vstack((arr1,arr2))

    id_arr = np.arange(size1 + size2)

    new_arr_with_id = np.hstack((id_arr,new_arr))

    new_columns = []
    for i in range(fields_size + 1):
        if i == 0:
            new_columns.append("id")
        else:
            new_columns.append("goal_%d" %i)

    df_ = pd.DataFrame(new_arr_with_id, columns=new_columns)
    return size1, size2, df_


if __name__ == '__main__':
    # df = pd.read_csv(r"E:\bonc\工业第四期需求\数据\out\粒子群计算结果-分析0919.csv", header=0)
    # print(df)
    path1 = os.path.abspath(os.path.join(os.getcwd(), "../mopso/img_txt")) + os.sep + "pareto_fitness.txt"
    path2 = os.path.abspath(os.path.join(os.getcwd(), "../mopso/img_txt")) + os.sep + "first_pareto_fitness0919.txt"
    df = new_df(path1, path2)[2]


    fit_in, fitness_in = (df.iloc[:,0:1].values, df.iloc[:,1:].values)
    # print(fit_in)
    # print(fitness_in)

    new_fit, new_fitness = Pareto_(fit_in, fitness_in).pareto()

    print(new_fit)
    print("shape")
    print(new_fit.shape)

    theory_result_size = fit_in[fit_in < df[0]]
    histort_result_size = fit_in [ fit_in.shape[0] - theory_result_size]

    print("理论最优解最终未被历史最优解占优个数:%d" %theory_result_size)
    print("历史最优解最终未被理论最优解占优个数:%d" %histort_result_size)


