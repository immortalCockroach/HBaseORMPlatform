package com.immortalcockroach.hbaseorm.param.enums;

public enum ArithmeticOperatorEnum {
    EQ(0, "="),
    NEQ(1, "!="),
    LT(2, "<"),
    GT(3, ">"),
    LE(4, "<="),
    GE(5, ">="),
    // 大于xx 小于xx
    BETWEEN(6, "> <"),
    // 大于等于xx 小于xx
    BETWEENL(7, ">= <"),
    BETWEENR(8, "> <="),
    BETWEENLR(9, ">= <=");

    private int id;
    private String name;

    ArithmeticOperatorEnum(int id, String name) {
        this.id = id;
        this.name = name;
    }

    public int getId() {
        return id;
    }

    public String getName() {
        return name;
    }
}
