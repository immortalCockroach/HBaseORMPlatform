package com.immortalcockroach.hbaseorm.param.enums;

public enum ArithmeticOperatorEnum {
    EQ(0, "="),
    NEQ(1, "!="),
    LT(2, "<"),
    LE(4, "<="),
    GT(3, ">"),
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

    /**
     * 根据运算符的id得到范围
     *
     * @param id
     * @return
     */
    public static boolean isSingleRange(int id) {
        return id >= LT.getId() && id <= GE.getId();
    }

    public static boolean isDoubleRange(int id) {
        return id >= BETWEEN.getId() && id <= BETWEENLR.getId();
    }

    public static boolean isEqualQuery(Integer id) {
        if (id == null) {
            return false;
        }
        return EQ.getId() == id;
    }

    public static boolean isNotEqualQuery(Integer id) {
        if (id == null) {
            return false;
        }
        return NEQ.getId() == id;
    }

    public int getId() {
        return id;
    }

    public String getName() {
        return name;
    }
}
