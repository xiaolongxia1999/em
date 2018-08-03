package com.bonc.interfaceRaw;

//原nblab接口
public interface IModel<T>{
	public Integer train(T[] inputData, Integer[] inputColIndex, Integer[] labelColIndex);
	//参数：输入数据及标签数据，返回值：错误码
	public T[] predict(T[] inputData);						
	//预测，返回值：预测数据
	public Integer setParam(String params);  							
	//设置参数（json格式描述），返回值错误码
	public Integer saveModel(String modelFilePath);						
	//模型序列化，返回值错误码
	public Integer loadModel(String modelFilePath);						
	//模型反序列化，返回值错误码
	public Double  getRMSE(T[] inputData, Integer labelColIndex);
	//获取验证误差（均方差）
}
