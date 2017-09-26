package com.immortalcockroach.hbaseorm.entity;

/**
 * 表创建时的列信息
 * Created by immortalCockroach on 9/26/17.
 */
public class Column {
    private String columnName;
    private Integer type;

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public Integer getType() {
        return type;
    }

    public void setType(Integer type) {
        this.type = type;
    }
}
