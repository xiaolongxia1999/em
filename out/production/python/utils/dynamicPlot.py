# _*_ coding: utf-8 _*_

"""
python_visual_animation.py by xianhu
"""

import numpy as np
import matplotlib
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm
from mpl_toolkits.mplot3d import Axes3D

# 解决中文乱码问题
# myfont = fm.FontProperties(fname="/Library/Fonts/Songti.ttc", size=14)
matplotlib.rcParams["axes.unicode_minus"] = False

#动态图
# 见:https://blog.csdn.net/fk1174/article/details/79379261
#主要是开启交互模式
def simple_plot():
    """
    simple plot
    """
    # 生成画布
    plt.figure(figsize=(8, 6), dpi=80)

    # 打开交互模式
    plt.ion()

    # 循环
    for index in range(100):
        # 清除原有图像
        plt.cla()

        # 设定标题等
        plt.title("动态曲线图")
        plt.grid(True)

        # 生成测试数据
        x = np.linspace(-np.pi + 0.1*index, np.pi+0.1*index, 256, endpoint=True)
        y_cos, y_sin = np.cos(x), np.sin(x)

        # 设置X轴
        plt.xlabel("X轴")
        plt.xlim(-4 + 0.1*index, 4 + 0.1*index)
        plt.xticks(np.linspace(-4 + 0.1*index, 4+0.1*index, 9, endpoint=True))

        # 设置Y轴
        plt.ylabel("Y轴")
        plt.ylim(-1.0, 1.0)
        plt.yticks(np.linspace(-1, 1, 9, endpoint=True))

        # 画两条曲线
        plt.plot(x, y_cos, "b--", linewidth=2.0, label="cos示例")
        plt.plot(x, y_sin, "g-", linewidth=2.0, label="sin示例")

        # 设置图例位置,loc可以为[upper, lower, left, right, center]
        plt.legend(loc="upper left", shadow=True)

        # 暂停
        #重点在于该暂停——————是图像更新时间间隔（看起来像个滑动窗口）————plt.pause(2)表示是2秒更新一次：
        #此示例中————xlim的坐标的上下限一直在变动，但宽度不变， ylim则是固定的区间
        plt.pause(2)
        # 原码是0.1间隔，看起来就像个动图
        # plt.pause(0.1)

    # 关闭交互模式
    plt.ioff()

    # 图形显示
    plt.show()
    return

simple_plot()