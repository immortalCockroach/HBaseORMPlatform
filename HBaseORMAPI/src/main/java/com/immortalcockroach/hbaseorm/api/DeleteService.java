package com.immortalcockroach.hbaseorm.api;

import com.immortalcockroach.hbaseorm.param.DeleteParam;
import com.immortalcockroach.hbaseorm.result.BaseResult;

public interface DeleteService {
    BaseResult delete(DeleteParam param);
}
