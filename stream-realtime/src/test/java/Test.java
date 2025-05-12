import com.retailersv.util.FlinkEnvUtils;
import lombok.SneakyThrows;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ Package PACKAGE_NAME
 * @ Author  liu.bo
 * @ Date  2025/5/12 15:49
 * @ description:
 * @ version 1.0
 */
/**
 * Test类包含程序的入口点，主要用于演示如何使用Flink进行数据流处理
 */
public class Test {

    /**
     * 程序的入口方法
     * 使用SneakyThrows注解来处理可能抛出的受检异常，避免显式地捕获或声明异常
     * @param args 命令行参数
     */
    @SneakyThrows
    public static void main(String[] args) {
        // 获取Flink执行环境
        StreamExecutionEnvironment env = FlinkEnvUtils.getFlinkRuntimeEnv();

        // 从指定的socket源创建数据流，这里假设数据在本地（127.0.0.1）的9999端口发送
        DataStreamSource<String> dataStreamSource = env.socketTextStream("127.0.0.1", 9999);

        // 将接收到的数据流打印到控制台
        dataStreamSource.print();

        // 打印Flink环境配置信息，用于调试和理解程序运行的上下文
        System.err.println(env.getConfig());

        // 执行Flink作业
        env.execute();
    }
}

