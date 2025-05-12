package com.retailersv.stream.utils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * @ Package com.retailersv.stream.utils.SensitiveWordsUtils
 * @ Author  liu.bo
 * @ Date  2025/5/12 16:55
 * @ description:
 * @ version 1.0
 */
/**
 * SensitiveWordsUtils 类用于处理敏感词相关操作
 */
public class SensitiveWordsUtils {

    /**
     * 从指定文件中读取敏感词列表
     *
     * @return 敏感词列表，如果文件读取失败或文件为空，则返回空列表
     */
    public static ArrayList<String> getSensitiveWordsLists(){
        ArrayList<String> res = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new FileReader("/Users/zhouhan/dev_env/work_project/java/stream-dev/stream-realtime/src/main/resources/Identify-sensitive-words.txt"))){
            String line ;
            while ((line = reader.readLine()) != null){
                res.add(line);
            }
        }catch (IOException ioException){
            ioException.printStackTrace();
        }
        return res;
    }

    /**
     * 随机选择列表中的一个元素
     *
     * @param list 输入的列表，可以是任何实现了List接口的类的实例
     * @param <T> 列表元素的类型
     * @return 如果列表为空或为null，则返回null；否则返回列表中的一个随机元素
     */
    public static <T> T getRandomElement(List<T> list) {
        if (list == null || list.isEmpty()) {
            return null;
        }
        Random random = new Random();
        int randomIndex = random.nextInt(list.size());
        return list.get(randomIndex);
    }

    /**
     * 主函数用于测试 getRandomElement 方法
     *
     * @param args 命令行参数
     */
    public static void main(String[] args) {
        System.err.println(getRandomElement(getSensitiveWordsLists()));
    }
}
