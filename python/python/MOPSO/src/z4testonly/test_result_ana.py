import numpy as np
import pandas as pd
from mopso.pareto import Pareto_

df = pd.read_csv(r"E:\bonc\工业第四期需求\数据\out\粒子群计算结果-分析0919.csv", header=0)
print(df)

fit_in, fitness_in = (df.iloc[:,0:1].values, df.iloc[:,1:].values)
# print(fit_in)
# print(fitness_in)

new_fit, new_fitness = Pareto_(fit_in, fitness_in).pareto()

print(new_fit)
print("shape")
print(new_fit.shape)