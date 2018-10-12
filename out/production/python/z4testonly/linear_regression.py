#source from:https://github.com/aymericdamien/TensorFlow-Examples/blob/master/notebooks/2_BasicModels/linear_regression.ipynb
#说明：此代码涵盖————tf的线性回归、作图
#过程参照github上tensorflow的线性回归示例————https://github.com/pkmital/tensorflow_tutorials/blob/master/python/02_linear_regression.py

import tensorflow as tf
import numpy
import matplotlib.pyplot as plt

rng = numpy.random

#@params
learning_rate = 0.001
training_epochs = 10000
display_step = 50

#@data
train_X = numpy.asarray([3.3,4.4,5.5,6.71,6.93,4.168,9.779,6.182,7.59,2.167,
                         7.042,10.791,5.313,7.997,5.654,9.27,3.1])

#2个自变量
# train_X = numpy.asarray([[3.3,4.4,5.5,6.71,6.93,4.168,9.779,6.182,7.59,2.167,
#                          7.042,10.791,5.313,7.997,5.654,9.27,3.1],[3.3,4.4,5.5,6.551,6.93,4.568,9.779,6.182,7.59,2.167,
#                          7.042,10.791,5.313,7.997,5.654,9.27,3.1]])

#@warn————需要对数据进行“标准化”,可以加速收敛，防止某些目标变量占据了loss function的绝大部分（量级不一致时）
#@source:https://www.cnblogs.com/hunttown/p/6844672.html
# Now we use tensorflow to get similar results.
# Before we put the x_data into tensorflow, we need to standardize it
# in order to achieve better performance in gradient descent;
# If not standardized, the convergency speed could not be tolearated.
# Reason:  If a feature has a variance that is orders of magnitude larger than others,
# it might dominate the objective function
# and make the estimator unable to learn from other features correctly as expected.
#具体处理如下：
# scaler = preprocessing.StandardScaler().fit(x_data)
# print (scaler.mean_, scaler.scale_)
# x_data_standard = scaler.transform(x_data)


train_Y = numpy.asarray([1.7,2.76,2.09,3.19,1.694,1.573,3.366,2.596,2.53,1.221,
                         2.827,3.465,1.65,2.904,2.42,2.94,1.3])
n_samples = train_X.shape[0]

X = tf.placeholder("float")
Y = tf.placeholder("float")


W = tf.Variable(rng.randn(), name = 'weight')
b = tf.Variable(rng.randn(), name = 'bias')

pred = tf.add(tf.multiply(X, W), b)

cost = tf.reduce_sum(tf.pow(pred - Y, 2)) / (2 * n_samples)
optimizer = tf.train.GradientDescentOptimizer(learning_rate).minimize(cost)

#定义模型初始化参数-貌似必加
init = tf.global_variables_initializer()

#@tf.session
with tf.Session() as sess:
    #初始化运行
    sess.run(init)
    for epoch in range(training_epochs):
        #@warn ,应为zip((train_X, train_Y))
        for (x,y) in zip(train_X, train_Y):
            sess.run(optimizer, feed_dict={X:x, Y:y})

        if (epoch + 1)% display_step == 0:
            c = sess.run(cost, feed_dict={X: train_X, Y:train_Y})
            print("Epoch:", '%04d'%(epoch+1), "cost=", "{:.9f}".format(c),\
                  "W=", sess.run(W), "b=", sess.run(b))
    print("Optimization Finished!")
    training_cost = sess.run(cost, feed_dict={X: train_X, Y:train_Y})
    print("Traing cost=",training_cost, "W=", sess.run(W),"b=",sess.run(b),'\n')

    # plt.plot(train_X, train_Y, 'ro', label='Origin_data')
    # plt.plot(train_X, sess.run(W) * train_X + sess.run(b), label='Fitted line')
    # plt.legend()
    # plt.show()