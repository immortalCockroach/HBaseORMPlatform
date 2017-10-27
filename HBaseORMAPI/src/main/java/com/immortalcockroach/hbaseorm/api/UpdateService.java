package com.immortalcockroach.hbaseorm.api;

import com.immortalcockroach.hbaseorm.param.UpdateParam;
import com.immortalcockroach.hbaseorm.result.BaseResult;

public interface UpdateService {
    BaseResult update(UpdateParam updateParam);
}
