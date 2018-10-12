import numpy as np
import pandas as pd
from mopso.pareto import Pareto_
import os

def new_df(path1, path2):
    arr1 = np.loadtxt(path1, dtype=np.float32)
    arr2 = np.loadtxt(path2, dtype=np.float32)
    print(arr1.shape[1])
    print(arr2.shape[1])
    size1 = arr1.shape[0]
    size2 = arr2.shape[0]
    fields_size = arr1.shape[1]

    new_arr = np.vstack((arr1,arr2))

    #由于new_arr也必须是2维（在stack时，两个数组的维度必须一致）的，因此id_arr要reshape一下，reshape(-1,1)表示：指定axis=1的长度为1，即1列，行数自动计算
    id_arr = np.arange(size1 + size2).reshape(-1,1)

    print(id_arr.shape[0])
    print(new_arr.shape[0])
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
    df_info = new_df(path1, path2)
    size1 = df_info[0]
    df =df_info[2]


    fit_in, fitness_in = (df.iloc[:,0:1].values, df.iloc[:,1:].values)
    # print(fit_in)
    # print(fitness_in)

    new_fit, new_fitness = Pareto_(fit_in, fitness_in).pareto()

    print(new_fit)
    print("shape")
    print(new_fit.shape)
    print(new_fitness.shape)

    fit_in = fit_in.reshape(-1)
    theory_result_size = new_fit[new_fit < size1].size
    histort_result_size = new_fit.size - theory_result_size

    print("理论最优解总数:%d" %new_fitness.shape[0])
    print("理论最优解最终未被历史最优解占优个数:%d" %theory_result_size)
    print("历史最优解最终未被理论最优解占优个数:%d" %histort_result_size)


