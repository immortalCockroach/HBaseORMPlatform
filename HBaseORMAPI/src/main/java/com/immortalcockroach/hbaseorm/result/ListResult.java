package com.immortalcockroach.hbaseorm.result;

import com.alibaba.fastjson.JSONArray;

/**
 * 列表查询的结果
 * Created by immortalCockroach on 8/30/17.
 */
public class ListResult extends AbstractResult {
    private static final long serialVersionUID = 2710323272047114150L;

    private JSONArray data;

    public JSONArray getData() {
        return data;
    }

    public void setData(JSONArray data) {
        this.data = data;
    }

    public Integer getSize() {
        return size;
    }

    public void setSize(Integer size) {
        this.size = size;
    }
}
