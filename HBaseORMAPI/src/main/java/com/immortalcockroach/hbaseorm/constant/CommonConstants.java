package com.immortalcockroach.hbaseorm.constant;

/**
 * Created by immortalCockRoach on 2016-06-22.
 */
public class CommonConstants {

    public static final String POST_METHOD = "POST";
    // 索引表主键、glocal表的idxs列的的字段分割符，可能需要转义

    /**
     * 代表rowkey的列，列名写死为rowkey
     */
    public static final String ROW_KEY = "rowkey";
    public static final String ENCODING = "UTF-8";
    public static final int SUCCESS_CODE = 200;
    public static final String DATA = "data";
    public static final String CODE = "code";
    public static final String READ = "read";
    public static final String SCAN = "scan";
    public static final String CREATE = "create";
    public static final String START = "start";
    public static final String STOP = "stop";
    public static final String PREFIX = "prefix";
    public static final String SPLIT_POLICY = "splitPolicy";
    public static final String UUID = "UUID";
    public static final String AUTO_INCREMENT = "smallAutoIncrement";
    public static final int SPLIT_COUNT = 3;
    private static final String DEFAULT_FORMAT = "yyyy-MM-dd HH:mm:ss";


    // 不可初始化
    private CommonConstants() {

    }

}
