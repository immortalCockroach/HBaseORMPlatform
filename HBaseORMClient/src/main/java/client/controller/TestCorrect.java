package client.controller;

import com.alibaba.fastjson.JSON;
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
@RequestMapping("testCorrect/")
public class TestCorrect {

    @Resource
    private QueryService queryService;

    @Resource
    private CreateService createService;

    @Resource
    private InsertService insertService;


    public String testInsert() {
        List<Map<String, byte[]>> content = new ArrayList<>();
        for (int i = 1; i <= 50; i++) {
            Map<String, byte[]> map = new HashMap<>();
            map.put(CommonConstants.ROW_KEY, Bytes.toBytes(i + ""));
            map.put("col1", Bytes.toBytes((i + 1) + ""));

            content.add(map);
        }
        InsertParam.InsertParamBuilder builder = new InsertParam.InsertParamBuilder(
                Bytes.toBytes("testCreateAuto"), content, new String[]{"col1"});
        BaseResult result = insertService.insert(builder.build());
        return JSON.toJSONString(result);
    }

    public String testCreateIndex() {
        CreateIndexParam.CreateIndexParamBuilder builder = new CreateIndexParam.CreateIndexParamBuilder(
                Bytes.toBytes("testCreateAuto"), new String[]{"col1"});
        CreateIndexParam param = builder.build();
        BaseResult result = createService.createIndex(param);
        return JSON.toJSONString(result);
    }

    public String testCreateTable() {
        CreateTableParam.CreateTableParamBuilder builder = new CreateTableParam.CreateTableParamBuilder(
                Bytes.toBytes("testCreateAuto"), new Column[]{new Column("Col1", 0)});
        CreateTableParam param = builder.build();
        BaseResult result = createService.createTable(param);
        return JSON.toJSONString(result);
    }

    @RequestMapping(value = "/testCorrectEqual", method =
            RequestMethod.GET)
    @ResponseBody
    public String testCorrectEqual(@RequestParam("col1") String col1) {
        testCreateTable();
        testCreateIndex();
        testInsert();
        QueryParam.QueryParamBuilder builder = new QueryParam.QueryParamBuilder(
                Bytes.toBytes("testCreateAuto"));
        builder.qulifiers(new String[]{"col1"}).condition(new Condition(new Expression("col1", ArithmeticOperatorEnum.GE.getId(), Bytes.toBytes(col1))));
        QueryParam param = builder.build();
        ListResult result = queryService.query(param);
        return JSON.toJSONString(result);
    }
}
