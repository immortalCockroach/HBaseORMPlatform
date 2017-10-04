package com.immortalcockroach.hbaseorm.param;

import com.immortalcockroach.hbaseorm.entity.Column;

import java.io.Serializable;

/**
 * 创建表时的参数参数
 * Created by immortalCockroach on 8/29/17.
 */
public class CreateTableParam implements Serializable {

    private static final long serialVersionUID = 4096731978248692079L;
    private byte[] tableName;
    private Column[] columns;

    // 必须为CommonConstants中的UUID或者是(small)AutoIncrement;
    private String splitAlgorithms;

    private Integer lowerBound;
    // 当为AutoIncrement的时候必须带上的参数
    private Integer upperBound;
    // 切分的数量 不传由服务端确定
    private Integer splitNum;

    /**
     * 作为序列化使用，不要调用
     */
    public CreateTableParam() {

    }

    private CreateTableParam(CreateTableParamBuilder builder) {
        this.tableName = builder.tableName;
        this.splitAlgorithms = builder.splitAlgorithm;
        this.upperBound = builder.upperBound;
        this.lowerBound = builder.lowerBound;
        this.splitNum = builder.splitNum;
        this.columns = builder.columns;
    }

    public Integer getLowerBound() {
        return lowerBound;
    }

    public Integer getUpperBound() {
        return upperBound;
    }

    public byte[] getTableName() {
        return tableName;
    }

    public String getSplitAlgorithms() {
        return splitAlgorithms;
    }

    public Integer getSplitNum() {
        return splitNum;
    }

    public Column[] getColumns() {
        return columns;
    }

    /**
     * 多参数构造器模型
     */
    public static class CreateTableParamBuilder {
        private final byte[] tableName;
        private final Column[] columns;
        private String splitAlgorithm;
        private Integer upperBound;
        private Integer lowerBound;
        private Integer splitNum;

        public CreateTableParamBuilder(byte[] tableName, Column[] columns) {
            this.tableName = tableName;
            this.columns = columns;
        }

        public CreateTableParamBuilder splitAlgorithm(String splitAlgorithm) {
            this.splitAlgorithm = splitAlgorithm;
            return this;
        }

        public CreateTableParamBuilder upperBound(Integer upperBound) {
            this.upperBound = upperBound;
            return this;
        }

        public CreateTableParamBuilder lowerBound(Integer lowerBound) {
            this.lowerBound = lowerBound;
            return this;
        }

        public CreateTableParamBuilder splitNum(Integer splitNum) {
            this.splitNum = splitNum;
            return this;
        }

        public CreateTableParam build() {
            return new CreateTableParam(this);
        }
    }
}
