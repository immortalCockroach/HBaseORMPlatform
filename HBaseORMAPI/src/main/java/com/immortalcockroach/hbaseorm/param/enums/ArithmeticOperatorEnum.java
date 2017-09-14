package com.immortalcockroach.hbaseorm.param.enums;

public enum ArithmeticOperatorEnum {
    AND(0, "&&"),
    OR(1, "||"),
    NOT(2, "!");

    private int id;
    private String name;

    ArithmeticOperatorEnum(int id, String name) {
        this.id = id;
        this.name = name;
    }

    public static ArithmeticOperatorEnum getArithmeticOperatorFromId(int id) {
        for (ArithmeticOperatorEnum arithmeticOperator : ArithmeticOperatorEnum.values()) {
            if (arithmeticOperator.id == id) {
                return arithmeticOperator;
            }
        }
        return null;
    }
}
