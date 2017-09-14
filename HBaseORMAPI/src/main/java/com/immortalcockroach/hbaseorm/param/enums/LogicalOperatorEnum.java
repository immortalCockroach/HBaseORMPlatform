package com.immortalcockroach.hbaseorm.param.enums;

public enum LogicalOperatorEnum {
    EQ(0, "="),
    NEQ(1, "!="),
    LT(2, "<"),
    GT(3, ">"),
    LE(4, "="),
    GE(5, "=");

    private int id;
    private String name;

    LogicalOperatorEnum(int id, String name) {
        this.id = id;
        this.name = name;
    }
}
