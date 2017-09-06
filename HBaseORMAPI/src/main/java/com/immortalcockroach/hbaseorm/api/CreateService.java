package com.immortalcockroach.hbaseorm.api;

import com.immortalcockroach.hbaseorm.param.CreateIndexParam;
import com.immortalcockroach.hbaseorm.param.CreateTableParam;
import com.immortalcockroach.hbaseorm.result.BaseResult;

/**
 * Created by immortalCockroach on 8/29/17.
 */
public interface CreateService {
    /**
     * 创建表的API
     *
     * @param createTableParam
     * @return
     */
    BaseResult createTable(CreateTableParam createTableParam);

    /**
     * 创建索引的API
     * @param createIndexParam
     * @return
     */
    BaseResult createIndex(CreateIndexParam createIndexParam);
}
