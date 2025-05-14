package com.lb.stream.realtime.bean;
import com.alibaba.fastjson.JSONObject;
/**
 * @ Package com.lb.stream.realtime.bean.DimJoinFunction
 * @ Author  liu.bo
 * @ Date  2025/5/14 21:48
 * @ description:
 * @ version 1.0
 */
public interface DimJoinFunction<T> {

    void addDims(T obj, JSONObject dimJsonObj) ;

    String getTableName() ;

    String getRowKey(T obj) ;
}
