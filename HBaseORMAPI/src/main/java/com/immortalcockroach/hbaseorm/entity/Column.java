package com.immortalcockroach.hbaseorm.entity;

import java.io.Serializable;

/**
 * 表创建时的列信息
 * Created by immortalCockroach on 9/26/17.
 */
public class Column implements Serializable {
    private static final long serialVersionUID = -2419849785349401584L;
    private String columnName;
    private Integer type;

    public Column() {

    }

    public Column(String columnName, Integer type) {
        this.columnName = columnName;
        this.type = type;
    }

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
