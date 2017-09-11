package com.immortalcockroach.hbaseorm.param;

import java.io.Serializable;
import java.util.List;

public class DeleteParam implements Serializable {

    private static final long serialVersionUID = 749576580673912683L;

    private byte[] tableName;
    private List<byte[]> rowkeysList;

    /**
     * 作为序列化使用，不要调用
     */
    public DeleteParam() {

    }

    private DeleteParam(DeleteParam.DeleteParamBuilder builder) {
        this.tableName = builder.tableName;
        this.rowkeysList = builder.rowkeysList;
    }

    public List<byte[]> getRowkeysList() {
        return rowkeysList;
    }

    public byte[] getTableName() {
        return tableName;
    }

    /**
     * 多参数构造器模型
     */
    public static class DeleteParamBuilder {
        private final byte[] tableName;
        private final List<byte[]> rowkeysList;

        public DeleteParamBuilder(byte[] tableName, List<byte[]> rowkeysList) {
            this.tableName = tableName;
            this.rowkeysList = rowkeysList;
        }

        public DeleteParam build() {
            return new DeleteParam(this);
        }
    }
}
