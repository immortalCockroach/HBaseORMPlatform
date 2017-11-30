package com.immortalcockroach.hbaseorm.param;

import java.io.Serializable;

/**
 * 建立二级索引相关的类
 * qualifiers中不要传rowkey
 * Created by immortalCockroach on 8/31/17.
 */
public class CreateIndexParam implements Serializable {

    private static final long serialVersionUID = -3177563640669857147L;

    private byte[] tableName;
    private String[] qualifiers;
    private int[] size;

    /**
     * 用于序列化使用，不要调用
     */
    public CreateIndexParam() {

    }

    private CreateIndexParam(CreateIndexParamBuilder builder) {
        this.tableName = builder.tableName;
        this.qualifiers = builder.qualifiers;
        this.size = builder.size;

    }

    public int[] getSize() {
        return size;
    }

    public byte[] getTableName() {
        return tableName;
    }

    public String[] getQualifiers() {
        return qualifiers;
    }

    public static class CreateIndexParamBuilder {
        private final byte[] tableName;
        private final String[] qualifiers;
        private int[] size;

        public CreateIndexParamBuilder(byte[] tableName, String[] qualifiers) {
            this.tableName = tableName;
            this.qualifiers = qualifiers;
        }

        public CreateIndexParamBuilder size(int[] size) {
            this.size = size;
            return this;
        }

        public CreateIndexParam build() {
            return new CreateIndexParam(this);
        }
    }
}
