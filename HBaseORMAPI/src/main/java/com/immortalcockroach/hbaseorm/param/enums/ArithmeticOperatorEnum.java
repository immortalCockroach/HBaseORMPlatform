package com.immortalcockroach.hbaseorm.param.enums;

public enum ArithmeticOperatorEnum {
    EQ(0, "="),
    NEQ(1, "!="),
    LT(2, "<"),
    GT(3, ">"),
    LE(4, "<="),
    GE(5, ">=");

    private int id;
    private String name;

    public int getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    ArithmeticOperatorEnum(int id, String name) {
        this.id = id;
        this.name = name;
    }
}
