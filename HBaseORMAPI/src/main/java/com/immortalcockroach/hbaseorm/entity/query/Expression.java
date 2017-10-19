package com.immortalcockroach.hbaseorm.entity.query;

import com.immortalcockroach.hbaseorm.param.enums.ArithmeticOperatorEnum;

import java.io.Serializable;

public class Expression implements Serializable {
    private static final long serialVersionUID = 6799766180225702749L;
    private String column;
    private Integer arithmeticOperator;
    private byte[] value;
    private byte[] optionValue;

    public Expression() {

    }

    public Expression(String column, Integer arithmeticOperator, byte[] value) {

        this.column = column;
        this.arithmeticOperator = arithmeticOperator;
        this.value = value;
    }

    public Expression(String column, Integer arithmeticOperator, byte[] value, byte[] optionValue) {

        this.column = column;
        this.arithmeticOperator = arithmeticOperator;
        this.value = value;
        this.optionValue = optionValue;
    }

    public byte[] getOptionValue() {
        return optionValue;
    }

    public void setOptionValue(byte[] optionValues) {
        this.optionValue = optionValues;
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

    public byte[] getValue() {
        return value;
    }

    public void setValue(byte[] value) {
        this.value = value;
    }

    public boolean isEqualsOperator() {
        return this.arithmeticOperator.equals(ArithmeticOperatorEnum.EQ.getId());
    }
}
