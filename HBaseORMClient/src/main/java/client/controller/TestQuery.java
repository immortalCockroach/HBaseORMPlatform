package client.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.immortalcockroach.hbaseorm.api.CreateService;
import com.immortalcockroach.hbaseorm.api.InsertService;
import com.immortalcockroach.hbaseorm.api.QueryService;
import com.immortalcockroach.hbaseorm.constant.CommonConstants;
import com.immortalcockroach.hbaseorm.entity.Column;
import com.immortalcockroach.hbaseorm.entity.query.Condition;
import com.immortalcockroach.hbaseorm.entity.query.Expression;
import com.immortalcockroach.hbaseorm.param.CreateIndexParam;
import com.immortalcockroach.hbaseorm.param.CreateTableParam;
import com.immortalcockroach.hbaseorm.param.InsertParam;
import com.immortalcockroach.hbaseorm.param.QueryParam;
import com.immortalcockroach.hbaseorm.param.enums.ArithmeticOperatorEnum;
import com.immortalcockroach.hbaseorm.result.BaseResult;
import com.immortalcockroach.hbaseorm.result.ListResult;
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

@RestController
@RequestMapping("testQuery/")
public class TestQuery {

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

    public String testInsert() {
        List<Map<String, byte[]>> content = new ArrayList<>();
        for (int i = 1; i <= 10; i++) {
            Map<String, byte[]> map = new HashMap<>();
            map.put(CommonConstants.ROW_KEY, Bytes.toBytes(i + ""));
            map.put("col1", Bytes.toBytes((i + 1) + ""));
            map.put("col2", Bytes.toBytes((i + 2) + ""));
            map.put("col3", Bytes.toBytes((i + 3) + ""));

            content.add(map);
        }
        InsertParam.InsertParamBuilder builder = new InsertParam.InsertParamBuilder(
                Bytes.toBytes("testCreateAuto"), content, new String[]{"col1", "col2", "col3"});
        BaseResult result = insertService.insert(builder.build());
        return JSON.toJSONString(result);
    }

    @RequestMapping(value = "/testCreateIndex", method = RequestMethod.GET)
    @ResponseBody
    public String testCreateIndex(@RequestParam("indexCount") Integer indexCount) {
        if (indexCount == null || indexCount == 0) {
            return "ok";
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
        return "ok";

    }

    public String testCreateTable() {
        CreateTableParam.CreateTableParamBuilder builder = new CreateTableParam.CreateTableParamBuilder(Bytes.toBytes("testCreateAuto"),
                new Column[]{new Column("col1", 0), new Column("col2", 0), new Column("col3", 0)});
        CreateTableParam param = builder.build();
        BaseResult result = createService.createTable(param);
        return JSON.toJSONString(result);
    }

    @RequestMapping(value = "/testInsertData", method =
            RequestMethod.GET)
    @ResponseBody
    public String testInsertData(@RequestParam("indexCount") Integer indexCount) {
        testCreateTable();
        testCreateIndex(indexCount);
        return JSON.toJSONString(testInsert());
    }

    @RequestMapping(value = "/testEqual", method = RequestMethod.GET)
    @ResponseBody
    public String testCorrectEQ(@RequestParam("col1") Integer col1) {
        QueryParam.QueryParamBuilder builder = new QueryParam.QueryParamBuilder(
                Bytes.toBytes("testCreateAuto"));
        builder.qulifiers(new String[]{"col1", "col2", "col3"}).condition(new Condition(new Expression("col1", ArithmeticOperatorEnum.EQ.getId(), Bytes.toBytes(col1 + ""))));
        QueryParam param = builder.build();
        long start = System.currentTimeMillis();
        ListResult result = queryService.query(param);
        long end = System.currentTimeMillis();
        JSONArray array = result.getData();
        for (int i = 0; i <= array.size() - 1; i++) {
            JSONObject o = array.getJSONObject(i);
            for (Map.Entry<String, Object> entry : o.entrySet()) {
                o.put(entry.getKey(), Bytes.toString((byte[]) entry.getValue()));
            }
        }
        return JSON.toJSONString(result) + "\n" + (end - start);
    }

    @RequestMapping(value = "/testGE", method = RequestMethod.GET)
    @ResponseBody
    public String testCorrectGE(@RequestParam("col1") String col1) {
        QueryParam.QueryParamBuilder builder = new QueryParam.QueryParamBuilder(
                Bytes.toBytes("testCreateAuto"));
        builder.qulifiers(new String[]{"col2", "col3"}).condition(new Condition(new Expression("col1", ArithmeticOperatorEnum.GE.getId(), Bytes.toBytes(col1))));
        QueryParam param = builder.build();
        long start = System.currentTimeMillis();
        ListResult result = queryService.query(param);
        long end = System.currentTimeMillis();
        JSONArray array = result.getData();
        for (int i = 0; i <= array.size() - 1; i++) {
            JSONObject o = array.getJSONObject(i);
            for (Map.Entry<String, Object> entry : o.entrySet()) {
                o.put(entry.getKey(), Bytes.toString((byte[]) entry.getValue()));
            }
        }
        return JSON.toJSONString(result) + "\n" + (end - start);
    }

    @RequestMapping(value = "/testLE", method = RequestMethod.GET)
    @ResponseBody
    public String testCorrectLE(@RequestParam("col1") String col1) {
        QueryParam.QueryParamBuilder builder = new QueryParam.QueryParamBuilder(
                Bytes.toBytes("testCreateAuto"));
        builder.qulifiers(new String[]{"col2", "col3"}).condition(new Condition(new Expression("col1", ArithmeticOperatorEnum.LE.getId(), Bytes.toBytes(col1))));
        QueryParam param = builder.build();
        ListResult result = queryService.query(param);
        JSONArray array = result.getData();
        for (int i = 0; i <= array.size() - 1; i++) {
            JSONObject o = array.getJSONObject(i);
            for (Map.Entry<String, Object> entry : o.entrySet()) {
                o.put(entry.getKey(), Bytes.toString((byte[]) entry.getValue()));
            }
        }
        return JSON.toJSONString(result);
    }

    @RequestMapping(value = "/testComposite", method = RequestMethod.GET)
    @ResponseBody
    public String testCorrectComposite(@RequestParam("col1") String col1, @RequestParam("col2") String col2) {
        QueryParam.QueryParamBuilder builder = new QueryParam.QueryParamBuilder(
                Bytes.toBytes("testCreateAuto"));
        Condition condition = new Condition(new Expression("col1", ArithmeticOperatorEnum.EQ.getId(), Bytes.toBytes(col1)));
        condition.add(new Expression("col2", ArithmeticOperatorEnum.EQ.getId(), Bytes.toBytes(col2)));
        builder.qulifiers(new String[]{"col1", "col2", "col3"}).condition(condition);
        QueryParam param = builder.build();
        long start = System.currentTimeMillis();
        ListResult result = queryService.query(param);
        long end = System.currentTimeMillis();
        JSONArray array = result.getData();

        for (int i = 0; i <= array.size() - 1; i++) {
            JSONObject o = array.getJSONObject(i);
            for (Map.Entry<String, Object> entry : o.entrySet()) {
                o.put(entry.getKey(), Bytes.toString((byte[]) entry.getValue()));
            }
        }
        return JSON.toJSONString(result) + "\n" + (end - start);
    }
}
