from mopso import pareto as pa
import  numpy as np
import pandas as pd
import matplotlib.pyplot as plt

in_ = np.array(
    [[0,1,3],
    [1,4,2],
    [1,3,1],
    [2,5,1],
    [1,4,0],
     [0,1,0],
     [0,0,1]]
)



fit_ =  np.array(
    [[0,1,3],
    [1,4,2],
    [1,3,1],
    [2,5,1],
    [1,4,0],
     [0,1,0],
     [0,0,1]]
)


# Pareto类的pareto方法，测试结果完全正确
# 注意：Pareto_.compare_方法， "if fitness_curr[i] < fitness_ref[i]:"这一行中的符号，   小于号，表示所有目标函数都是越小越好， 大于号，则表示所有目标函数都是越大越好，二者得到的结果截然不同
# 这里，我们统一以“越小越好”为准
obj = pa.Pareto_(in_, fit_)
tuple1 = obj.pareto()
print("result----------------------------------")
print(tuple1[1])
# print(tuple1[0].shape[0])


# 测试csv
path = r"E:\bonc\工业第四期需求\数据\out\去除负9和0等异常值的DF0904.csv"
df = pd.read_csv(path, header=0)
df1 = df[["CZ3-FC7002","CZ3-TI7001","CZ3-TI7003","CZ3-TI7005","CZ3-TI7007"]]
df1 = pd.DataFrame.as_matrix(df1)
print(type(df1))

df1 = df1[:, 0:2]
obj1 = pa.Pareto_(df1, df1)
# print(df1)
tuple2 = obj1.pareto()
front = tuple2[1]

print(tuple2[1])
print(len(tuple2[1]))

# 作图查看pareto解集的样子（应该是一条前沿面曲线）

## 所有历史数据
x0 = df1[:,0]
y0 = df1[:,1]
ax1 = plt.subplot(111)
# 非占优解集
x1 = front[:,0]
y1 = front[:,1]
ax2 = plt.subplot(111)
#
ax1.scatter(x0, y0, c = 'b', marker = '.', s = 10)
ax2.scatter(x1, y1, c = 'r', marker = 'o')
plt.show()