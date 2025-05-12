package com.retailersv.stream.utils;

import com.retailersv.util.ConfigUtils;
import org.apache.flink.table.catalog.hive.HiveCatalog;

/**
 * @ Package com.retailersv.stream.utils.HiveCatalogUtils
 * @ Author  liu.bo
 * @ Date  2025/5/12 16:56
 * @ description:
 * @ version 1.0
 */
public class HiveCatalogUtils {
    private static final String HIVE_CONF_DIR = ConfigUtils.getString("hive.conf.dir");

    public static HiveCatalog getHiveCatalog(String catalogName){
        System.setProperty("HADOOP_USER_NAME","root");
        return new HiveCatalog(catalogName, "default", HIVE_CONF_DIR);
    }
}