package com.immortalcockroach.hbaseorm.param;

import com.immortalcockroach.hbaseorm.entity.query.Condition;

import java.io.Serializable;
import java.util.Map;

public class UpdateParam implements Serializable {
    private static final long serialVersionUID = -1404135004858869450L;

    private byte[] tableName;
    private Condition condition;
    private Map<String, byte[]> updateValues;

    /**
     * 作为序列化使用，不要调用
     */
    public UpdateParam() {

    }

    private UpdateParam(UpdateParam.UpdateParamBuilder builder) {
        this.tableName = builder.tableName;
        this.condition = builder.condition;
        this.updateValues = builder.values;
    }

    public Map<String, Integer> getConditionColumnsType() {

        return this.condition.getQueryTypeOfColumns();
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
    public static class UpdateParamBuilder {
        private final byte[] tableName;
        private final Condition condition;
        private final Map<String, byte[]> values;

        public UpdateParamBuilder(byte[] tableName, Condition condition, Map<String, byte[]> values) {
            this.tableName = tableName;
            this.condition = condition;
            this.values = values;
        }

        public UpdateParam build() {
            return new UpdateParam(this);
        }
    }
}
