package service.utils;

import org.apache.commons.lang.StringUtils;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * 日期工具类
 * Created by immortalCockRoach on 2016/5/30.
 */
public class DateUtils {

    public static String getDateString(Date date, String format) {
        if (date == null || StringUtils.isBlank(format)) {
            return "";
        }

        SimpleDateFormat sdf = new SimpleDateFormat(format);

        return sdf.format(date);
    }
}
