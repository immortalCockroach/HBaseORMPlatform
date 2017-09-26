package com.immortalcockroach.hbaseorm.param;


import com.immortalcockroach.hbaseorm.entity.query.Condition;

import java.io.Serializable;
import java.util.List;

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
    private Condition condition;

    private QueryParam(QueryParamBuilder builder) {
        this.tableName = builder.tableName;
        this.qualifiers = builder.qualifiers;
        this.condition = builder.condition;
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

    public Condition getCondition() {
        return condition;
    }

    public List<String> getQueryColumns() {
        return this.condition.getQueryColumns();
    }

    /**
     * 多参数构造器模型
     */
    public static class QueryParamBuilder {
        private final byte[] tableName;
        private String[] qualifiers;

        private Condition condition;

        public QueryParamBuilder(byte[] tableName) {
            this.tableName = tableName;
        }

        public QueryParamBuilder qulifiers(String[] qualifiers) {
            this.qualifiers = qualifiers;
            return this;
        }

        public QueryParamBuilder condition(Condition condition) {
            this.condition = condition;
            return this;
        }

        public QueryParam build() {
            return new QueryParam(this);
        }
    }


}
