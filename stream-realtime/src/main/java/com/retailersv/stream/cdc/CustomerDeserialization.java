package com.retailersv.stream.cdc;

import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.HashMap;
import java.util.Map;

/**
 * @ Package com.retailersv.stream.cdc.CustomerDeserialization
 * @ Author  liu.bo
 * @ Date  2025/5/12 16:45
 * @ description:
 * @ version 1.0
 */
/**
 * CustomerDeserialization 类实现了 DebeziumDeserializationSchema 接口，
 * 用于反序列化 Change Data Capture (CDC) 事件数据为字符串形式。
 * 这主要用于从数据库变化中捕获事件，并将这些事件转换为可处理的格式。
 */
public class CustomerDeserialization implements DebeziumDeserializationSchema<String> {

    /**
     * 反序列化方法，将 SourceRecord 转换为字符串形式的 JSON 对象。
     *
     * @param sourceRecord 代表数据库变化事件的源记录，包含事件的所有信息。
     * @param collector 用于收集转换后的数据的收集器。
     * @throws Exception 如果在反序列化过程中发生错误。
     */
    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {
        // 初始化一个空的 JSON 对象来存储解析后的数据
        JSONObject parsedObject = new JSONObject();

        // 确定操作类型（如插入、更新、删除等）
        Envelope.Operation operation = Envelope.operationFor(sourceRecord);

        // 获取不同表的id值
        Struct key = (Struct) sourceRecord.key();
        if (key.toString() != null && key.toString().length() > 0) {
            // 提取并存储记录的标识符
            String id = key.toString().split("=")[1].substring(0, key.toString().split("=")[1].length() - 1);
            parsedObject.put("id", id);
        }

        // 存储操作类型
        parsedObject.put("op", operation.toString());

        // 处理主题名称以提取数据库和表名信息
        if (null != sourceRecord.topic()) {
            String[] splitTopic = sourceRecord.topic().split("\\.");
            if (splitTopic.length == 3) {
                parsedObject.put("database", splitTopic[1]);
                parsedObject.put("tableName", splitTopic[2]);
            }
        }

        // 解析记录的值，以获取变更事件的详细信息
        Struct value = (Struct) sourceRecord.value();
        String tsMs = value.get("ts_ms").toString();

        // 变更前后的数据位于value这个Struct中，名称分别为before和after
        Struct before = value.getStruct("before");
        Struct after = value.getStruct("after");

        // 处理变更前的数据
        if (null != before) {
            Map<String, Object> beforeMap = new HashMap<>();
            Schema beforeSchema = before.schema();
            for (Field field : beforeSchema.fields()) {
                beforeMap.put(field.name(), before.get(field));
            }
            parsedObject.put("before", beforeMap.toString());
        }

        // 处理变更后的数据
        if (null != after) {
            Map<String, Object> afterMap = new HashMap<>();
            Schema afterSchema = after.schema();
            for (Field field : afterSchema.fields()) {
                afterMap.put(field.name(), after.get(field));
            }
            parsedObject.put("after", afterMap.toString());
        }

        // 更新id，使其包含时间戳，以确保唯一性
        parsedObject.put("id", parsedObject.get("id") + "_" + tsMs);
        // 存储时间戳
        parsedObject.put("ts", tsMs);

        // 使用收集器收集解析后的JSON字符串
        collector.collect(parsedObject.toString());
    }

    /**
     * 返回此反序列化方案产生的数据类型信息。
     *
     * @return TypeInformation<String> 表示产生的数据类型为字符串。
     */
    @Override
    public TypeInformation<String> getProducedType() {
        return BasicTypeInfo.STRING_TYPE_INFO;
    }
}

