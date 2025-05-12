package com.retailersv.stream.utils;

import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @ Package com.retailersv.stream.utils.PaimonMinioUtils
 * @ Author  liu.bo
 * @ Date  2025/5/12 16:56
 * @ description:
 * @ version 1.0
 */
/**
 * PaimonMinioUtils类用于与MinIO对象存储服务交互，主要功能是创建目录和数据库
 * 该类利用Apache Flink的StreamTableEnvironment执行SQL语句来实现其功能
 */
public class PaimonMinioUtils {

    /**
     * 执行创建MinIO目录和数据库的操作
     * 此方法首先设置Hadoop用户为root，然后根据提供的catalogName和databaseName创建目录和数据库
     * 如果catalogName为空，则使用默认值"minio_paimon_catalog"
     *
     * @param tenv StreamTableEnvironment实例，用于执行SQL语句
     * @param catalogName 目录名称，如果为空将使用默认值
     * @param databaseName 数据库名称，将被创建或忽略（如果已存在）
     */
    public static void ExecCreateMinioCatalogAndDatabases(StreamTableEnvironment tenv, String catalogName, String databaseName){
        // 设置Hadoop用户名为root
        System.setProperty("HADOOP_USER_NAME","root");

        // 如果catalogName为空，赋予默认值
        if (catalogName.length() == 0){
            catalogName = "minio_paimon_catalog";
        }

        // 执行SQL语句创建目录，目录配置了MinIO的访问信息
        tenv.executeSql("CREATE CATALOG "+ catalogName +"                             " +
                "WITH                                                                   " +
                "  (                                                                    " +
                "    'type' = 'paimon',                                                 " +
                "    'warehouse' = 's3://paimon-data/',                                 " +
                "    's3.endpoint' = 'http://10.160.60.17:9000',                         " +
                "    's3.access-key' = 'X7pljEi3steavVn5h3z3',                          " +
                "    's3.secret-key' = 'KDaSxEyfSEmKiaJDBbJ6RpBxMBp6OwnRbkA8LnKL',      " +
                "    's3.connection.ssl.enabled' = 'false',                             " +
                "    's3.path.style.access' = 'true',                                   " +
                "    's3.impl' = 'org.apache.hadoop.fs.s3a.S3AFileSystem',              " +
                "    's3.aws.credentials.provider' = 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider' " +
                "  );");

        // 使用新创建或默认的目录
        tenv.executeSql("use catalog "+catalogName+";");

        // 创建数据库，如果数据库已存在则不执行创建操作
        tenv.executeSql("create database if not exists "+databaseName+";");
    }
}

