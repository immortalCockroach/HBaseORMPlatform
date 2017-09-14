package com.immortalcockroach.hbaseorm.api;

import com.immortalcockroach.hbaseorm.param.QueryParam;
import com.immortalcockroach.hbaseorm.result.AbstractResult;

/**
 * Created by immortalCockroach on 8/27/17.
 */
public interface QueryService {
    AbstractResult query(QueryParam queryParam);
}
