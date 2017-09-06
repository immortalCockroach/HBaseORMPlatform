package com.immortalcockroach.hbaseorm.result;

import java.io.Serializable;

/**
 * Result的基础类，code为200代表成功，其余均是失败的结果
 * 客户端目前不依赖具体的code值，即code值非200的情况下只代表失败，不代表具体的类型
 * Created by immortalCockroach on 8/30/17.
 */
public abstract class AbstractResult implements Serializable {
    private static final long serialVersionUID = 6260106495935791881L;
    private Boolean success;
    private Integer code;
    private String errorMsg;

    /**
     * 代表result中data的数量，如果是fail或者empty则为0，
     * PlainResult和BaseResult为1，ListResult为列表的长度
     */
    protected Integer size;

    public Boolean getSuccess() {
        return success;
    }

    public void setSuccess(Boolean success) {
        this.success = success;
    }

    public Integer getCode() {
        return code;
    }

    public void setCode(Integer code) {
        this.code = code;
    }

    public String getErrorMsg() {
        return errorMsg;
    }

    public void setErrorMsg(String errorMsg) {
        this.errorMsg = errorMsg;
    }

    public abstract void setSize(Integer size);
    public abstract Integer getSize();
}
