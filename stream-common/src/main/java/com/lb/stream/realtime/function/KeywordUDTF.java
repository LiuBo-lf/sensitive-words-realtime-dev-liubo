package com.lb.stream.realtime.function;

import com.lb.stream.realtime.utils.KeywordUtil;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

/**
 * @ Package com.lb.stream.realtime.function.KeywordUDTF
 * @ Author  liu.bo
 * @ Date  2025/5/14 22:05
 * @ description:
 * @ version 1.0
 */
@FunctionHint(output = @DataTypeHint("ROW<word STRING>"))
public class KeywordUDTF extends TableFunction < Row > {
    public void eval(String text) {
        for (String keyword : KeywordUtil.analyze(text)) {
            collect(Row.of(keyword));
        }
    }
}
