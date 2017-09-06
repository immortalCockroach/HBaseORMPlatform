package com.immortalcockroach.hbaseorm.api;

import com.immortalcockroach.hbaseorm.param.InsertParam;
import com.immortalcockroach.hbaseorm.result.BaseResult;

/**
 * Created by immortalCockroach on 8/31/17.
 */
public interface InsertService {
    BaseResult insert(InsertParam param);
}
