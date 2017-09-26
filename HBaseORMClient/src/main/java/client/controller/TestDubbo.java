package client.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.immortalcockroach.hbaseorm.api.CreateService;
import com.immortalcockroach.hbaseorm.api.InsertService;
import com.immortalcockroach.hbaseorm.api.QueryService;
import com.immortalcockroach.hbaseorm.constant.CommonConstants;
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
@RequestMapping("dubbo/")
public class TestDubbo {
    @Resource
    private QueryService queryService;

    @Resource
    private CreateService createService;

    @Resource
    private InsertService insertService;

    @RequestMapping(value = "/test", method = RequestMethod.GET)
    @ResponseBody
    public JSONObject test(@RequestParam(value = "name") String name) {
        JSONObject o = new JSONObject();
        o.put("name", name);
        long start = System.currentTimeMillis();
        //JSONObject res =  queryService.query(o);
        o.put("time", System.currentTimeMillis() - start);
        //o.put("size", res.size());
        return o;
    }

    @RequestMapping(value = "/testPut", method = RequestMethod.GET)
    @ResponseBody
    public JSONObject testPut() {
        return null;
    }

 /*   @RequestMapping(value = "/testCreateUUID", method = RequestMethod.GET)
    @ResponseBody
    public String testCreateUUID() {
        CreateTableParam.CreateTableParamBuilder builder = new CreateTableParam.CreateTableParamBuilder(Bytes.toBytes("testCreateUUID"));
        builder.splitAlgorithm(CommonConstants.UUID);
        CreateTableParam param = builder.build();
        BaseResult result = createService.createTable(param);
        return JSON.toJSONString(result);
    }

    @RequestMapping(value = "/testCreateAuto", method = RequestMethod.GET)
    @ResponseBody
    public String testCreateAuto() {
        CreateTableParam.CreateTableParamBuilder builder = new CreateTableParam.CreateTableParamBuilder(Bytes.toBytes("testCreateAuto"));
        builder.splitAlgorithm(CommonConstants.AUTO_INCREMENT);
        builder.lowerBound(1);
        builder.upperBound(1000);
        builder.splitNum(3);
        CreateTableParam param = builder.build();
        BaseResult result = createService.createTable(param);
        return JSON.toJSONString(result);
    }*/

    @RequestMapping(value = "/testInsert", method = RequestMethod.GET)
    @ResponseBody
    public String testInsert() {
        List<Map<String, byte[]>> content = new ArrayList<>();
        for (int i = 1; i <= 50; i++) {
            Map<String, byte[]> map = new HashMap<>();
            map.put(CommonConstants.ROW_KEY, Bytes.toBytes(i + ""));
            if (i % 10 == 0) {
                //map.put("col1", Bytes.toBytes(i + "a"));
                map.put("col2", Bytes.toBytes(i));
                map.put("col3", Bytes.toBytes(i));
            } else if (i % 11 == 0) {
                map.put("col1", Bytes.toBytes(i));
                // map.put("col2", Bytes.toBytes(i + "b"));
                map.put("col3", Bytes.toBytes(i));
            } else if (i % 12 == 0) {
                map.put("col1", Bytes.toBytes(i));
                map.put("col2", Bytes.toBytes(i));
                //map.put("col3", Bytes.toBytes(i + "c"));
            } else {
                map.put("col1", Bytes.toBytes(i));
                map.put("col2", Bytes.toBytes(i));
                map.put("col3", Bytes.toBytes(i));
            }

            content.add(map);
        }
        InsertParam.InsertParamBuilder builder = new InsertParam.InsertParamBuilder(
                Bytes.toBytes("testCreateAuto"), content, new String[]{"col1", "col2", "col3"});
        BaseResult result = insertService.insert(builder.build());
        return JSON.toJSONString(result);
    }

    @RequestMapping(value = "/testCreateOneIndex", method = RequestMethod.GET)
    @ResponseBody
    public String testCreateIndex() {
        CreateIndexParam.CreateIndexParamBuilder builder = new CreateIndexParam.CreateIndexParamBuilder(
                Bytes.toBytes("testCreateAuto"), new String[]{"col1"});
        CreateIndexParam param = builder.build();
        BaseResult result = createService.createIndex(param);
        return JSON.toJSONString(result);
    }

    @RequestMapping(value = "/testCreateMultiIndex", method = RequestMethod.GET)
    @ResponseBody
    public String testCreateMultiIndex() {
        CreateIndexParam.CreateIndexParamBuilder builder = new CreateIndexParam.CreateIndexParamBuilder(
                Bytes.toBytes("testCreateAuto"), new String[]{"col2", "col3"});
        CreateIndexParam param = builder.build();
        BaseResult result = createService.createIndex(param);
        return JSON.toJSONString(result);
    }

    @RequestMapping(value = "/testCreateMultiIndexWithNull", method =
            RequestMethod.GET)
    @ResponseBody
    public String testCreateMultiIndexWithNull() {
        CreateIndexParam.CreateIndexParamBuilder builder = new CreateIndexParam.CreateIndexParamBuilder(
                Bytes.toBytes("testCreateAuto"), new String[]{"col2", "col3"});
        CreateIndexParam param = builder.build();
        BaseResult result = createService.createIndex(param);
        return JSON.toJSONString(result);
    }
}
