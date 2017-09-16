package com.immortalcockroach.hbaseorm.param.enums;

public enum LogicalOperatorEnum {
    AND(0, "&&"),
    OR(1, "||"),
    NOT(2, "!");

    private int id;
    private String name;

    LogicalOperatorEnum(int id, String name) {
        this.id = id;
        this.name = name;
    }

    public static LogicalOperatorEnum getLogicalOperatorEnumFromId(int id) {
        for (LogicalOperatorEnum logicalOperatorEnum : LogicalOperatorEnum.values()) {
            if (logicalOperatorEnum.id == id) {
                return logicalOperatorEnum;
            }
        }
        return null;
    }
}
