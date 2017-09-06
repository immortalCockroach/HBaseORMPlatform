package com.immortalcockroach.hbaseorm.result;

import com.alibaba.fastjson.JSONObject;

/**
 * 代表单个查询的结果
 * Created by immortalCockroach on 8/30/17.
 */
public class PlainResult extends AbstractResult {
    private static final long serialVersionUID = 6420921482453500322L;

    private JSONObject data;

    public JSONObject getData() {
        return data;
    }

    public void setData(JSONObject data) {
        this.data = data;
    }

    @Override
    public void setSize(Integer size) {
        this.size = size;
    }

    @Override
    public Integer getSize() {
        return this.size;
    }
}
