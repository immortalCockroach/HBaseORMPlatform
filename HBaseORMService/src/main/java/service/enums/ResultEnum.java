package service.enums;


import com.immortalcockroach.hbaseorm.constant.CommonConstants;
import service.utils.JsonUtils;

import java.util.HashMap;
import java.util.Map;

/**
 * 查询结果的常量，如查询得到的结果为空,服务端异常等
 * Created by immortalCockRoach on 2016/5/31.
 */

public enum ResultEnum {
    SUCCESS(200, ""),
    HBASE_CONNECTION_FAILED(301, "连接数据库异常，请联系开发人员"),
    SERVER_ERROR(302, "服务端异常，请联系开发人员");

    private int id;
    private String desc;

    ResultEnum(int id, String desc) {
        this.id = id;
        this.desc = desc;
    }

    public int getId() {
        return id;
    }

    public String getDesc() {
        return desc;
    }

    @Override
    public String toString() {
        Map<String, Object> response = this.toMap();

        return JsonUtils.toJsonString(response);
    }

    /**
     * RPC接口返回值专用
     * @return
     */
    public Map<String, Object> toMap() {
        Map<String, Object> response = new HashMap<>();
        response.put(CommonConstants.CODE, this.id);
        response.put(CommonConstants.DATA, this.desc);
        return response;
    }
}
