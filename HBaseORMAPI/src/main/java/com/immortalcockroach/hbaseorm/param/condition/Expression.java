package com.immortalcockroach.hbaseorm.param.condition;

import java.io.Serializable;

public class Expression implements Serializable {
    private static final long serialVersionUID = 6799766180225702749L;
    private String column;
    private Integer arithmeticOperator;
    private byte[] values;

    public Expression() {

    }

    public Expression(String column, Integer arithmeticOperator, byte[] values) {

        this.column = column;
        this.arithmeticOperator = arithmeticOperator;
        this.values = values;
    }

    public String getColumn() {
        return column;
    }

    public void setColumn(String column) {
        this.column = column;
    }

    public Integer getArithmeticOperator() {
        return arithmeticOperator;
    }

    public void setArithmeticOperator(Integer arithmeticOperator) {
        this.arithmeticOperator = arithmeticOperator;
    }

    public byte[] getValues() {
        return values;
    }

    public void setValues(byte[] values) {
        this.values = values;
    }
}
