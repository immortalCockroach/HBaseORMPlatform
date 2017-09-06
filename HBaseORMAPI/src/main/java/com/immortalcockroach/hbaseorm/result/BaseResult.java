package com.immortalcockroach.hbaseorm.result;

/**
 * 代表成功或者是失败
 * Created by immortalCockroach on 8/30/17.
 */
public class BaseResult extends AbstractResult {
    private static final long serialVersionUID = -3438740504029284409L;
    private Boolean data;

    public Boolean getData() {
        return data;
    }

    public void setData(Boolean data) {
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
