package client.controller;

import com.alibaba.fastjson.JSON;
import com.immortalcockroach.hbaseorm.api.CreateService;
import com.immortalcockroach.hbaseorm.api.InsertService;
import com.immortalcockroach.hbaseorm.api.QueryService;
import com.immortalcockroach.hbaseorm.constant.CommonConstants;
import com.immortalcockroach.hbaseorm.entity.Column;
import com.immortalcockroach.hbaseorm.param.CreateIndexParam;
import com.immortalcockroach.hbaseorm.param.CreateTableParam;
import com.immortalcockroach.hbaseorm.param.InsertParam;
import com.immortalcockroach.hbaseorm.result.BaseResult;
import com.immortalcockroach.hbaseorm.util.Bytes;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by immortalCockroach on 8/28/17.
 */
@RestController
@RequestMapping("testInsert/")
public class TestInsert {
    private static final int batchSize = 5000;
    @Resource
    private QueryService queryService;
    @Resource
    private CreateService createService;
    @Resource
    private InsertService insertService;

    private static String[] getIndexByCount(Integer indexCount) {
        String[] res = new String[indexCount];
        for (int i = 0; i <= indexCount - 1; i++) {
            res[i] = "col" + (i + 1);
        }
        return res;
    }

    public void testInsert(Integer count) {

        for (int i = 1; i <= count; i += batchSize) {

            List<Map<String, byte[]>> content = new ArrayList<>();
            int size = 0;
            while (size < batchSize) {
                Map<String, byte[]> map = new HashMap<>();
                map.put(CommonConstants.ROW_KEY, Bytes.toBytes(i + size + ""));
                map.put("col1", Bytes.toBytes((i + size + 1) + ""));
                map.put("col2", Bytes.toBytes((i + size + 2) + ""));
                map.put("col3", Bytes.toBytes((i + size + 3) + ""));
                content.add(map);
                size++;
            }
            InsertParam.InsertParamBuilder builder = new InsertParam.InsertParamBuilder(
                    Bytes.toBytes("testCreateAuto"), content, new String[]{"col1", "col2", "col3"});
            BaseResult result = insertService.insert(builder.build());
        }


    }

    public void testCreateIndex(Integer indexCount) {
        if (indexCount == null || indexCount == 0) {
            return;
        }
        String[] indexes = getIndexByCount(indexCount);
        if (indexes.length == 2) {
            CreateIndexParam.CreateIndexParamBuilder builder = new CreateIndexParam.CreateIndexParamBuilder(
                    Bytes.toBytes("testCreateAuto"), new String[]{"col1", "col2"});
            CreateIndexParam param = builder.build();
            BaseResult result = createService.createIndex(param);
        } else {
            for (String index : indexes) {

                CreateIndexParam.CreateIndexParamBuilder builder = new CreateIndexParam.CreateIndexParamBuilder(
                        Bytes.toBytes("testCreateAuto"), new String[]{index});
                CreateIndexParam param = builder.build();
                BaseResult result = createService.createIndex(param);
            }
        }


    }

    public String testCreateTable() {
        CreateTableParam.CreateTableParamBuilder builder = new CreateTableParam.CreateTableParamBuilder(Bytes.toBytes("testCreateAuto"),
                new Column[]{new Column("col1", 0), new Column("col2", 0), new Column("col3", 0)});
        CreateTableParam param = builder.build();
        BaseResult result = createService.createTable(param);
        return JSON.toJSONString(result);
    }

    @RequestMapping(value = "/testInsert", method = RequestMethod.GET)
    @ResponseBody
    public String testInsert(@RequestParam("count") Integer count, @RequestParam("indexCount") Integer indexCount) {
        testCreateTable();
        testCreateIndex(indexCount);
        long start = System.currentTimeMillis();
        testInsert(count);
        return (System.currentTimeMillis() - start) + "";

    }
}
