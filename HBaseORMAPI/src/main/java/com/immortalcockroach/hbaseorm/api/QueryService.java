package com.immortalcockroach.hbaseorm.api;

import com.immortalcockroach.hbaseorm.param.QueryParam;
import com.immortalcockroach.hbaseorm.result.ListResult;

/**
 * Created by immortalCockroach on 8/27/17.
 */
public interface QueryService {
    ListResult query(QueryParam queryParam);
}
