package com.retailersv.func;

import com.alibaba.fastjson.JSONObject;
import com.retailersv.util.HbaseUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.hadoop.hbase.client.Connection;

/**
 * @ Package com.retailersv.func.MapUpdateHbaseDimTableFunc
 * @ Author  liu.bo
 * @ Date  2025/5/12 16:42
 * @ description:
 * @ version 1.0
 */
/**
 * 更新HBase维度表的Map函数
 * 该类用于处理JSON对象流，根据操作类型（如创建、删除表等）更新HBase中的维度表
 */
public class MapUpdateHbaseDimTableFunc extends RichMapFunction<JSONObject,JSONObject> {

    // HBase连接
    private Connection connection;
    // HBase命名空间
    private final String hbaseNameSpace;
    // Zookeeper主机列表
    private final String zkHostList;
    // HBase工具类实例
    private HbaseUtils hbaseUtils;

    /**
     * 构造函数
     * @param cdhZookeeperServer Zookeeper主机列表
     * @param cdhHbaseNameSpace HBase命名空间
     */
    public MapUpdateHbaseDimTableFunc(String cdhZookeeperServer, String cdhHbaseNameSpace) {
        this.zkHostList = cdhZookeeperServer;
        this.hbaseNameSpace = cdhHbaseNameSpace;
    }

    /**
     * 初始化资源
     * 在执行任何map操作之前，建立与HBase的连接
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        hbaseUtils = new HbaseUtils(zkHostList);
        connection = hbaseUtils.getConnection();
    }

    /**
     * 处理输入的JSON对象，根据操作类型更新HBase表
     * @param jsonObject 输入的JSON对象，包含操作信息和表的详细信息
     * @return 处理后的JSON对象
     * @throws Exception
     */
    @Override
    public JSONObject map(JSONObject jsonObject) throws Exception {
        // 获取操作类型
        String op = jsonObject.getString("op");
        if ("d".equals(op)){
            // 如果是删除操作，调用deleteTable方法删除表
            hbaseUtils.deleteTable(jsonObject.getJSONObject("before").getString("sink_table"));
        }else if ("r".equals(op) || "c".equals(op)){
            // 如果是读取或创建操作，检查表是否存在，不存在则创建
            // String[] columnName = jsonObject.getJSONObject("after").getString("sink_columns").split(",");
            String tableName = jsonObject.getJSONObject("after").getString("sink_table");
            if (!hbaseUtils.tableIsExists(hbaseNameSpace+":"+tableName)){
                hbaseUtils.createTable(hbaseNameSpace,tableName);
            }
        }else {
            // 对于其他操作，先删除再创建
            hbaseUtils.deleteTable(jsonObject.getJSONObject("before").getString("sink_table"));
            // String[] columnName = jsonObject.getJSONObject("after").getString("sink_columns").split(",");
            String tableName = jsonObject.getJSONObject("after").getString("sink_table");
            hbaseUtils.createTable(hbaseNameSpace,tableName);
        }
        return jsonObject;
    }

    /**
     * 释放资源
     * 在所有操作完成后，关闭与HBase的连接
     */
    @Override
    public void close() throws Exception {
        super.close();
        connection.close();
    }
}

