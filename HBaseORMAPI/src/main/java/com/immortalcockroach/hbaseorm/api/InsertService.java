package com.immortalcockroach.hbaseorm.api;

import com.immortalcockroach.hbaseorm.param.InsertParam;
import com.immortalcockroach.hbaseorm.result.BaseResult;

/**
 * 插入、更新统一的接口
 * Created by immortalCockroach on 8/31/17.
 */
public interface InsertService {
    BaseResult insert(InsertParam param);
}
