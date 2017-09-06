package service.utils;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;

/**
 * Stream工具类，用于字节流和其他数据类型，如byte[],String,基本类型之间的相互转化
 * Created by immortalCockRoach on 2016/6/3.
 */
public class StreamUtils {
    private static final Logger logger = Logger.getLogger(StreamUtils.class);

    /**
     * 将一个inputStream中的内容转换为byte[] 如果inputStream为null 或者读取发生异常，或者inputStream中没有内容 则返回null
     * @param in
     * @return
     */
    public static byte[] copyBytesFromStream(InputStream in) {
        if (in == null) {
            return null;
        }
        ByteArrayOutputStream bo = new ByteArrayOutputStream();
        int count;
        try {
            count = IOUtils.copy(in, bo);
        } catch (IOException e) {
            logger.warn(e.getMessage(), e);
            return null;
        }

        return count == 0 ? null : bo.toByteArray();
    }
}
