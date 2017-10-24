package com.immortalcockroach.hbaseorm.param;

import com.immortalcockroach.hbaseorm.entity.query.Condition;

import java.io.Serializable;
import java.util.List;

public class DeleteParam implements Serializable {

    private static final long serialVersionUID = 749576580673912683L;

    private byte[] tableName;
    private Condition condition;

    /**
     * 作为序列化使用，不要调用
     */
    public DeleteParam() {

    }

    private DeleteParam(DeleteParam.DeleteParamBuilder builder) {
        this.tableName = builder.tableName;
        this.condition = builder.condition;
    }

    public Condition getCondition() {
        return condition;
    }

    public byte[] getTableName() {
        return tableName;
    }

    /**
     * 多参数构造器模型
     */
    public static class DeleteParamBuilder {
        private final byte[] tableName;
        private final Condition condition;

        public DeleteParamBuilder(byte[] tableName, Condition condition) {
            this.tableName = tableName;
            this.condition = condition;
        }

        public DeleteParam build() {
            return new DeleteParam(this);
        }
    }
}
