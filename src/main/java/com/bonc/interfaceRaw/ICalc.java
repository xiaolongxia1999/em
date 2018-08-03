package com.bonc.interfaceRaw;

//原nblab接口
public interface ICalc<T> {
	public T[] calc(T[] inputs);
	public Integer setParam(String params);	//设置参数（json格式描述），返回值错误码
}


