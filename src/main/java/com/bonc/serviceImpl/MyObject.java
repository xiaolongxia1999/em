package com.bonc.serviceImpl;

import com.dtw.TimeWarpInfo;
import com.timeseries.TimeSeries;
import com.timeseries.TimeSeriesPoint;
import com.util.DistanceFunction;
import com.util.DistanceFunctionFactory;

import java.io.Serializable;

public  class MyObject implements Serializable {
    public  static double dtw(double[][] ts1,double[][] ts2,String distanceName){
        TimeSeries tim1 = new TimeSeries( ts1[0].length);
        TimeSeries tim2 = new TimeSeries( ts2[0].length);
        for(int i = 0; i < ts1.length; i++){
            tim1.addLast(i,new TimeSeriesPoint(ts1[i]));
        }
        for(int i = 0; i < ts2.length; i++){
            tim2.addLast(i,new TimeSeriesPoint(ts2[i]));
        }
        DistanceFunction distFn = DistanceFunctionFactory.getDistFnByName(distanceName);
        TimeWarpInfo info1 = com.dtw.FastDTW.getWarpInfoBetween(tim1, tim2, 1, distFn);
        return info1.getDistance();
    }
}




