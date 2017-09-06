package service.enums;



import com.immortalcockroach.hbaseorm.constant.CommonConstants;
import service.utils.JsonUtils;

import java.util.HashMap;
import java.util.Map;

/**
 * 查询的参数校验结果，100为OK，其余的分别对应具体的DESC信息
 * Created by immortalCockRoach on 2016/5/31.
 */

public enum QueryParametersEnum {
    PARAMETERS_OK(100, ""),
    METHOD_NOT_SUPPORT(101, "不支持使用该方法进行查询，请使用\"POST\""),
    PATH_NOT_EXISTS(102, "URI_PATH不合法, 请指定为\"read(对应get)\"或者\"scan\""),
    SERVLET_NOT_EXISTS(103, "Servlet路径不存在"),
    EMPTY_ENTITY(104, "\"POST\"方法的参数体为空"),
    CANNOT_PARSE_TO_JSON(105, "参数不符合json格式"),
    TABLE_NOT_EXIST(106, "查询的表不存在"),
    PARAMETER_NOT_LEGAL(107, "参数值为空或者缺少必要的参数,如表名,列名为空或者read时没有指定rowkey"), ;


    private int id;
    private String desc;

    QueryParametersEnum(int id, String desc) {
        this.id = id;
        this.desc = desc;
    }

    @Override
    public String toString() {
        Map<String, Object> response = this.toMap();

        return JsonUtils.toJsonString(response);
    }

    public Map<String, Object> toMap() {
        Map<String, Object> response = new HashMap<>();
        response.put(CommonConstants.CODE, this.id);
        response.put(CommonConstants.DATA, this.desc);
        return response;
    }
}
