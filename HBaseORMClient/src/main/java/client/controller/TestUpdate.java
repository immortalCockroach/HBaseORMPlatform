package client.controller;

import com.alibaba.fastjson.JSON;
import com.immortalcockroach.hbaseorm.api.CreateService;
import com.immortalcockroach.hbaseorm.api.InsertService;
import com.immortalcockroach.hbaseorm.api.QueryService;
import com.immortalcockroach.hbaseorm.api.UpdateService;
import com.immortalcockroach.hbaseorm.constant.CommonConstants;
import com.immortalcockroach.hbaseorm.entity.Column;
import com.immortalcockroach.hbaseorm.entity.query.Condition;
import com.immortalcockroach.hbaseorm.entity.query.Expression;
import com.immortalcockroach.hbaseorm.param.CreateIndexParam;
import com.immortalcockroach.hbaseorm.param.CreateTableParam;
import com.immortalcockroach.hbaseorm.param.InsertParam;
import com.immortalcockroach.hbaseorm.param.UpdateParam;
import com.immortalcockroach.hbaseorm.param.enums.ArithmeticOperatorEnum;
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

@RestController
@RequestMapping("testUpdate/")
public class TestUpdate {

    @Resource
    private QueryService queryService;

    @Resource
    private CreateService createService;

    @Resource
    private InsertService insertService;

    @Resource
    private UpdateService updateService;

    public String testInsert() {
        List<Map<String, byte[]>> content = new ArrayList<>();
        for (int i = 1; i <= 10; i++) {
            Map<String, byte[]> map = new HashMap<>();
            map.put(CommonConstants.ROW_KEY, Bytes.toBytes(i));
            map.put("col1", Bytes.toBytes(i + 1));
            map.put("col2", Bytes.toBytes(i + 2));
            map.put("col3", Bytes.toBytes(i + 3));

            content.add(map);
        }
        InsertParam.InsertParamBuilder builder = new InsertParam.InsertParamBuilder(
                Bytes.toBytes("testCreateAuto"), content, new String[]{"col1", "col2", "col3"});
        BaseResult result = insertService.insert(builder.build());
        return JSON.toJSONString(result);
    }

    public String testCreateIndex(Integer indexCount) {
        if (indexCount == null || indexCount == 0) {
            return "ok";
        }
        CreateIndexParam.CreateIndexParamBuilder builder = new CreateIndexParam.CreateIndexParamBuilder(
                Bytes.toBytes("testCreateAuto"), new String[]{"col1"});
        builder.size(new int[]{4});
        CreateIndexParam param = builder.build();
        BaseResult result = createService.createIndex(param);
        if (indexCount == 2) {
            CreateIndexParam.CreateIndexParamBuilder builder2 = new CreateIndexParam.CreateIndexParamBuilder(
                    Bytes.toBytes("testCreateAuto"), new String[]{"col1", "col2"});
            builder2.size(new int[]{4, 4});
            CreateIndexParam param2 = builder2.build();
            BaseResult result2 = createService.createIndex(param2);
        }
        return "ok";

    }

    public String testCreateTable() {
        CreateTableParam.CreateTableParamBuilder builder = new CreateTableParam.CreateTableParamBuilder(Bytes.toBytes("testCreateAuto"),
                new Column[]{new Column("col1", 3), new Column("col2", 3), new Column("col3", 3)});
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

    @RequestMapping(value = "/testUpdateEqual", method = RequestMethod.GET)
    @ResponseBody
    public String testCorrectEQ(@RequestParam("col1") Integer col1) {
        Map<String, byte[]> updateValues = new HashMap<>();
        updateValues.put("col1", Bytes.toBytes(21));
        updateValues.put("col3", Bytes.toBytes(23));
        Condition condition = new Condition(new Expression("col1", ArithmeticOperatorEnum.EQ.getId(), Bytes.toBytes(col1)));
        UpdateParam.UpdateParamBuilder builder = new UpdateParam.UpdateParamBuilder(
                Bytes.toBytes("testCreateAuto"), condition, updateValues);

        UpdateParam param = builder.build();
        long start = System.currentTimeMillis();
        BaseResult result = updateService.update(param);
        long end = System.currentTimeMillis();

        return JSON.toJSONString(result) + "\n" + (end - start);
    }

    @RequestMapping(value = "/testUpdateLE", method = RequestMethod.GET)
    @ResponseBody
    public String testDeleteLE(@RequestParam("col1") Integer col1) {
        Map<String, byte[]> updateValues = new HashMap<>();
        updateValues.put("col1", Bytes.toBytes(21));
        updateValues.put("col3", Bytes.toBytes(23));
        Condition condition = new Condition(new Expression("col1", ArithmeticOperatorEnum.LE.getId(), Bytes.toBytes(col1)));
        UpdateParam.UpdateParamBuilder builder = new UpdateParam.UpdateParamBuilder(
                Bytes.toBytes("testCreateAuto"), condition, updateValues);


        UpdateParam param = builder.build();
        long start = System.currentTimeMillis();
        BaseResult result = updateService.update(param);
        long end = System.currentTimeMillis();

        return JSON.toJSONString(result) + "\n" + (end - start);
    }

    @RequestMapping(value = "/testUpdateGE", method = RequestMethod.GET)
    @ResponseBody
    public String testDeleteGE(@RequestParam("col1") Integer col1) {
        Map<String, byte[]> updateValues = new HashMap<>();
        updateValues.put("col1", Bytes.toBytes(21));
        updateValues.put("col3", Bytes.toBytes(23));
        Condition condition = new Condition(new Expression("col1", ArithmeticOperatorEnum.GE.getId(), Bytes.toBytes(col1)));
        UpdateParam.UpdateParamBuilder builder = new UpdateParam.UpdateParamBuilder(
                Bytes.toBytes("testCreateAuto"), condition, updateValues);


        UpdateParam param = builder.build();
        long start = System.currentTimeMillis();
        BaseResult result = updateService.update(param);
        long end = System.currentTimeMillis();

        return JSON.toJSONString(result) + "\n" + (end - start);
    }

    @RequestMapping(value = "/testComposite", method = RequestMethod.GET)
    @ResponseBody
    public String testCorrectComposite(@RequestParam("col1") Integer col1, @RequestParam("col2") Integer col2) {

        Map<String, byte[]> updateValues = new HashMap<>();
        updateValues.put("col1", Bytes.toBytes(21));
        updateValues.put("col3", Bytes.toBytes(23));
        Condition condition = new Condition(new Expression("col1", ArithmeticOperatorEnum.EQ.getId(), Bytes.toBytes(col1)));
        condition.add(new Expression("col2", ArithmeticOperatorEnum.EQ.getId(), Bytes.toBytes(col2)));
        UpdateParam.UpdateParamBuilder builder = new UpdateParam.UpdateParamBuilder(
                Bytes.toBytes("testCreateAuto"), condition, updateValues);


        UpdateParam param = builder.build();
        long start = System.currentTimeMillis();
        BaseResult result = updateService.update(param);
        long end = System.currentTimeMillis();

        return JSON.toJSONString(result) + "\n" + (end - start);
    }
}
