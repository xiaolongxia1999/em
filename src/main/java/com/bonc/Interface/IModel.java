package com.bonc.Interface;

/**
 * Created by Administrator on 2018/7/5 0005.
 */

/**
 * 模型类接口
 * 训练阶段：设置参数setParam、训练train、保存模型saveModel
 * 预测阶段：加载模型loadModel、预测predict、模型评估getRMSE
 * @param <T>
 */

public interface IModel<T> {
    Integer train(T[] trainDataRaw, Integer[] inputColIndex, Integer[] outputColIndex);

    T[] predict(T[] predictSetRaw);

    Integer setParam(String params);

    Integer saveModel(String savePath);

    Integer loadModel(String savePath);

    Double getRMSE(T[] ts, Integer integer);
}
