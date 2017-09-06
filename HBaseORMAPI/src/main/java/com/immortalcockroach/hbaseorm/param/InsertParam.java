package com.immortalcockroach.hbaseorm.param;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * Created by immortalCockroach on 8/31/17.
 */
public class InsertParam implements Serializable {
    private static final long serialVersionUID = 576210560278761147L;

    private byte[] tableName;
    private List<Map<String, byte[]>> valuesMap;
    private String[] qualifiers;

    /**
     * 作为序列化使用，不要调用
     */
    public InsertParam() {

    }

    private InsertParam(InsertParamBuilder builder) {
        this.tableName = builder.tableName;
        this.valuesMap = builder.valuesMap;
        this.qualifiers = builder.qualifiers;
    }

    public byte[] getTableName() {
        return tableName;
    }

    public List<Map<String, byte[]>> getValuesMap() {
        return valuesMap;
    }

    public String[] getQualifiers() {
        return qualifiers;
    }

    /**
     * 多参数构造器模型
     */
    public static class InsertParamBuilder {
        private final byte[] tableName;
        private final List<Map<String, byte[]>> valuesMap;
        private String[] qualifiers;

        public InsertParamBuilder(byte[] tableName, List<Map<String, byte[]>> valuesMap, String[] qualifiers) {
            this.tableName = tableName;
            this.valuesMap = valuesMap;
            this.qualifiers = qualifiers;
        }

        public InsertParam build() {
            return new InsertParam(this);
        }
    }

}
