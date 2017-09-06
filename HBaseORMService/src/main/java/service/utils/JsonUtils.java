package service.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import org.apache.commons.lang.StringUtils;

/**
 * json工具类
 * Created by immortalCockRoach on 2016/5/31.
 */
public class JsonUtils {
    /**
     * 字符串转化为JSONObject，当不能完成正常的转化时均返回null
     *
     * @param jsonString
     * @return
     */
    public static JSONObject toJsonObject(String jsonString) {
        if (StringUtils.isBlank(jsonString)) {
            return null;
        }
        JSONObject rtn;
        try {
            rtn = JSON.parseObject(jsonString);
        } catch (ClassCastException e) {
            return null;
        }

        return rtn;
    }

    /**
     * 将对象转换为JSON形式的字符串，不能正常转换的情况返回null
     *
     * @param o
     * @return
     */
    public static String toJsonString(Object o) {
        if (o == null) {
            return null;
        }
        String rtn;
        try {
            rtn = JSON.toJSONString(o);
        } catch (Exception e) {
            return null;
        }

        return rtn;
    }

    /**
     * 将对象转化为JSON形式的字符串，保留value为null的值对
     * @param o
     * @return
     */
    public static String toJsonStringWithNullValue(Object o) {
        if (o == null) {
            return null;
        }
        String rtn;
        try {
            rtn = JSON.toJSONString(o, SerializerFeature.WriteMapNullValue);
        } catch (Exception e) {
            return null;
        }

        return rtn;
    }
}
