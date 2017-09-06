package com.immortalcockroach.hbaseorm.util;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.immortalcockroach.hbaseorm.result.BaseResult;
import com.immortalcockroach.hbaseorm.result.ListResult;
import com.immortalcockroach.hbaseorm.result.PlainResult;

/**
 * Result结果的Util类，用于生成常用的Result结果
 * Created by immortalCockroach on 8/30/17.
 */
public class ResultUtil {
    public static BaseResult getSuccessBaseResult() {
        BaseResult result = new BaseResult();
        result.setData(null);
        result.setCode(200);
        result.setSuccess(true);
        result.setSize(1);
        return result;
    }

    public static BaseResult getFailedBaseResult(String errMsg) {
        BaseResult result = new BaseResult();
        result.setData(null);
        result.setErrorMsg(errMsg);
        result.setCode(500);
        result.setSuccess(false);
        result.setSize(0);
        return result;
    }

    public static PlainResult getSuccessPlainResult(JSONObject data) {
        PlainResult result = new PlainResult();
        result.setData(data);
        result.setCode(200);
        result.setSuccess(true);
        result.setSize(1);
        return result;
    }

    /**
     * 代表查询失失败
     *
     * @param errMsg 异常的消息
     * @return
     */
    public static PlainResult getFailedPlainResult(String errMsg) {
        PlainResult result = new PlainResult();
        result.setData(null);
        result.setCode(500);
        result.setSuccess(false);
        result.setErrorMsg(errMsg);
        result.setSize(0);
        return result;
    }

    /**
     * 查询成功但是没有数据
     *
     * @param
     * @return
     */
    public static PlainResult getEmptyPlainResult() {
        PlainResult result = new PlainResult();
        result.setData(null);
        result.setCode(200);
        result.setSuccess(true);
        result.setSize(0);
        return result;
    }

    public static ListResult getSuccessListResult(JSONArray data) {
        ListResult result = new ListResult();
        result.setData(data);
        result.setCode(200);
        result.setSuccess(true);
        result.setSize(data.size());
        return result;
    }

    /**
     * 代表查询失失败
     *
     * @param errMsg 异常的消息
     * @return
     */
    public static ListResult getFailedListResult(String errMsg) {
        ListResult result = new ListResult();
        result.setData(null);
        result.setCode(500);
        result.setSize(0);
        result.setSuccess(false);
        result.setErrorMsg(errMsg);
        return result;
    }

    /**
     * 查询成功但是没有数据
     *
     * @return
     */
    public static ListResult getEmptyListResult() {
        ListResult result = new ListResult();
        result.setData(null);
        result.setCode(200);
        result.setSize(0);
        result.setSuccess(true);
        return result;
    }
}
