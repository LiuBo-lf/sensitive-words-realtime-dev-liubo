package com.retailersv.func;

import com.alibaba.fastjson.JSONObject;
import com.retailersv.domain.TableProcessDim;
import com.retailersv.util.ConfigUtils;
import com.retailersv.util.HbaseUtils;
import com.retailersv.util.JdbcUtils;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.MD5Hash;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @ Package com.retailersv.func.ProcessSpiltStreamToHBaseDimFunc
 * @ Author  liu.bo
 * @ Date  2025/5/12 16:43
 * @ description:
 * @ version 1.0
 */
/**
 * 处理分割流到HBase维度数据的函数
 * 该类继承了BroadcastProcessFunction，用于处理广播流和普通流的数据
 * 主要功能是将来自MySQL的数据转换后写入HBase
 */
public class ProcessSpiltStreamToHBaseDimFunc extends BroadcastProcessFunction<JSONObject,JSONObject,JSONObject> {

    // 描述符，用于定义广播状态的数据结构
    private MapStateDescriptor<String,JSONObject> mapStateDescriptor;

    // 存储配置信息的映射表
    private HashMap<String, TableProcessDim> configMap =  new HashMap<>();

    // HBase连接
    private org.apache.hadoop.hbase.client.Connection hbaseConnection;

    // HBase工具类实例
    private HbaseUtils hbaseUtils;

    /**
     * 初始化函数，用于打开函数实例时执行一些设置操作
     * @param parameters 配置参数
     * @throws Exception 如果初始化失败，抛出异常
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        // 获取MySQL连接
        Connection connection = JdbcUtils.getMySQLConnection(
                ConfigUtils.getString("mysql.url"),
                ConfigUtils.getString("mysql.user"),
                ConfigUtils.getString("mysql.pwd"));

        // 查询MySQL中配置表的数据
        String querySQL = "select * from realtime_v1_config.table_process_dim";
        List<TableProcessDim> tableProcessDims = JdbcUtils.queryList(connection, querySQL, TableProcessDim.class, true);

        // 将查询结果存入configMap，以便后续使用
        for (TableProcessDim tableProcessDim : tableProcessDims ){
            configMap.put(tableProcessDim.getSourceTable(),tableProcessDim);
        }

        // 关闭MySQL连接
        connection.close();

        // 初始化HBase工具类和连接
        hbaseUtils = new HbaseUtils(ConfigUtils.getString("zookeeper.server.host.list"));
        hbaseConnection = hbaseUtils.getConnection();
    }

    /**
     * 构造函数，用于创建ProcessSpiltStreamToHBaseDimFunc实例
     * @param mapStageDesc MapStateDescriptor实例，用于定义广播状态
     */
    public ProcessSpiltStreamToHBaseDimFunc(MapStateDescriptor<String, JSONObject> mapStageDesc) {
        this.mapStateDescriptor = mapStageDesc;
    }

    /**
     * 处理普通流中的元素
     * @param jsonObject 输入的JSON对象
     * @param readOnlyContext 只读上下文，用于访问广播状态
     * @param collector 用于输出数据的收集器
     * @throws Exception 如果处理失败，抛出异常
     */
    @Override
    public void processElement(JSONObject jsonObject, BroadcastProcessFunction<JSONObject, JSONObject, JSONObject>.ReadOnlyContext readOnlyContext, Collector<JSONObject> collector) throws Exception {
        // 获取广播状态
        ReadOnlyBroadcastState<String, JSONObject> broadcastState = readOnlyContext.getBroadcastState(mapStateDescriptor);

        // 提取数据源的表名
        String tableName = jsonObject.getJSONObject("source").getString("table");

        // 从广播状态中获取对应表名的配置信息
        JSONObject broadData = broadcastState.get(tableName);

        // 检查是否有对应的配置信息
        if (broadData != null || configMap.get(tableName) != null){
            // 检查配置信息是否匹配
            if (configMap.get(tableName).getSourceTable().equals(tableName)){
                // 如果操作不是删除操作，则执行插入或更新操作
                if (!jsonObject.getString("op").equals("d")){
                    // 获取数据中的after部分，即新数据
                    JSONObject after = jsonObject.getJSONObject("after");

                    // 构造HBase表名
                    String sinkTableName = configMap.get(tableName).getSinkTable();
                    sinkTableName = "realtime_v2:"+sinkTableName;

                    // 获取行键值
                    String hbaseRowKey = after.getString(configMap.get(tableName).getSinkRowKey());

                    // 获取HBase表实例
                    Table hbaseConnectionTable = hbaseConnection.getTable(TableName.valueOf(sinkTableName));

                    // 创建Put实例，用于插入或更新数据
                    Put put = new Put(Bytes.toBytes(MD5Hash.getMD5AsHex(hbaseRowKey.getBytes(StandardCharsets.UTF_8))));

                    // 遍历新数据，添加到Put实例中
                    for (Map.Entry<String, Object> entry : after.entrySet()) {
                        put.addColumn(Bytes.toBytes("info"),Bytes.toBytes(entry.getKey()),Bytes.toBytes(String.valueOf(entry.getValue())));
                    }

                    // 执行插入或更新操作
                    hbaseConnectionTable.put(put);

                    // 打印日志
                    System.err.println("put -> "+put.toJSON()+" "+ Arrays.toString(put.getRow()));
                }
            }
        }
    }

    /**
     * 处理广播流中的元素
     * @param jsonObject 输入的JSON对象
     * @param context 上下文，用于访问和修改广播状态
     * @param collector 用于输出数据的收集器
     * @throws Exception 如果处理失败，抛出异常
     */
    @Override
    public void processBroadcastElement(JSONObject jsonObject, BroadcastProcessFunction<JSONObject, JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
        // 获取可修改的广播状态
        BroadcastState<String, JSONObject> broadcastState = context.getBroadcastState(mapStateDescriptor);

        // 提取操作类型
        String op = jsonObject.getString("op");

        // 检查是否包含after部分，即新数据
        if (jsonObject.containsKey("after")){
            // 提取源表名
            String sourceTableName = jsonObject.getJSONObject("after").getString("source_table");

            // 根据操作类型执行对应的操作
            if ("d".equals(op)){
                // 如果是删除操作，从广播状态中移除对应表名的配置信息
                broadcastState.remove(sourceTableName);
            }else {
                // 如果是其他操作，将新数据更新到广播状态中
                broadcastState.put(sourceTableName,jsonObject);
            }
        }
    }

    /**
     * 关闭函数实例时执行的清理操作
     * @throws Exception 如果关闭失败，抛出异常
     */
    @Override
    public void close() throws Exception {
        super.close();
        // 关闭HBase连接
        hbaseConnection.close();
    }
}

