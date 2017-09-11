package com.immortalcockroach.hbaseorm.param;


import java.io.Serializable;

/**
 * 根据查询的条件构建query，目前只有query只有单个扫描和多个扫描以及列选择，后续添加新功能
 * <p>
 * 1、目前查询的方式为 查询单个的时为startKey和endKey相等
 * 2、如果startKey或者endKey为null，则代表从头开始或者查询到末尾
 * </p>
 * Created by immortalCockRoach on 2016-06-22.
 */
public class QueryParam implements Serializable {

    private static final long serialVersionUID = 8195618777960273893L;
    private byte[] tableName;
    private String[] qualifiers;
    private byte[] startKey;
    private byte[] endKey;

    private QueryParam(QueryParamBuilder builder) {
        this.endKey = builder.endKey;
        this.startKey = builder.startKey;
        this.tableName = builder.tableName;
        this.qualifiers = builder.qualifiers;
    }

    /**
     * 作为序列化使用，不要调用
     */
    public QueryParam() {

    }

    public byte[] getTableName() {
        return tableName;
    }

    public String[] getQualifiers() {
        return qualifiers;
    }

    public byte[] getStartKey() {
        return startKey;
    }

    public byte[] getEndKey() {
        return endKey;
    }

    /**
     * 多参数构造器模型
     */
    public static class QueryParamBuilder {
        private final byte[] tableName;
        private String[] qualifiers;

        private byte[] startKey;
        private byte[] endKey;

        public QueryParamBuilder(byte[] tableName) {
            this.tableName = tableName;
        }

        public QueryParamBuilder qulifiers(String[] qualifiers) {
            this.qualifiers = qualifiers;
            return this;
        }

        public QueryParamBuilder endKey(byte[] endKey) {
            this.endKey = endKey;
            return this;
        }

        public QueryParamBuilder startKey(byte[] startKey) {
            this.startKey = startKey;
            return this;
        }

        public QueryParam build() {
            return new QueryParam(this);
        }
    }


}
