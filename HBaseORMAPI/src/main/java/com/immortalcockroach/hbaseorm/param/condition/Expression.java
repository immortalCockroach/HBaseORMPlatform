package com.immortalcockroach.hbaseorm.param.condition;

import java.io.Serializable;

public class Expression implements Serializable {
    private static final long serialVersionUID = 6799766180225702749L;
    private byte[] columns;
    private Integer arithmeticOperator;
    private byte[] values;

    public Expression() {

    }

    public Expression(byte[] columns, Integer arithmeticOperator, byte[] values) {

        this.columns = columns;
        this.arithmeticOperator = arithmeticOperator;
        this.values = values;
    }

    public byte[] getColumns() {
        return columns;
    }

    public void setColumns(byte[] columns) {
        this.columns = columns;
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
