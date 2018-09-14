import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

path = r"E:\bonc\工业第四期需求\数据\out\去除负9和0等异常值的DF0904.csv"
df = pd.read_csv(path, header=0)
# print(df)

fields = df.columns
# print(type(fields))
# print(fields)

df1 = df[["CZ3-FC7002","CZ3-TI7001","CZ3-TI7003","CZ3-TI7005","CZ3-TI7007",'label1']]
# print(df1)


x1 = df1.iloc[:,0:1]
x2 = df1.iloc[:,1:2]
x3 = df1.iloc[:,2:3]
x4 = df1.iloc[:,3:4]
x5 = df1.iloc[:,4:5]

y = df1.iloc[:,5:6]


# print(x1)
# print(y)
# fig = plt.figure



#5张图，如下：

# # ax1 = plt.subplot(231)
# ax1 = plt.subplot(111)
# ax1.scatter(x1, y, c = 'b', marker = '.', s = 10)

# plt.axis([150,200,70,110])

# ax2 = plt.subplot(232)
# ax2 = plt.subplot(111)
# ax2.scatter(x2, y, c = 'r', marker = '.', s=10)
#
# # ax3 = plt.subplot(233)
# ax3 = plt.subplot(111)
# ax3.scatter(x3, y, c = 'r', marker = '.', s=10)
#
# # ax4 = plt.subplot(234)
# ax4 = plt.subplot(111)
# ax4.scatter(x4, y, c = 'r', marker = '.', s=10)
#
# # ax5 = plt.subplot(235)
ax5 = plt.subplot(111)
ax5.scatter(x5, y, c = 'r', marker = '.', s=10)

plt.show()