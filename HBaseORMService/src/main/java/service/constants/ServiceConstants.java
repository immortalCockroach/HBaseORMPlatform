package service.constants;

import com.immortalcockroach.hbaseorm.util.Bytes;

/**
 * 全局常量部分
 * Created by immortalCockRoach on 2016/5/31.
 */
public class ServiceConstants {
    @Deprecated
    public static final byte EOT = 4; // 分隔符
    @Deprecated
    public static final byte ESC = 27; // 转义符
    @Deprecated
    public static final byte NULL = 0; // null值替代符


    public static final String TABLE = "table";

    public static final String INDEX_SUFFIX = "_idx";

    // 用于维护表中的索引列
    public static final String GLOBAL_INDEX_TABLE = "global_idx";
    public static final byte[] GLOBAL_INDEX_TABLE_BYTES = Bytes.toBytes("global:global_idx");

    // 索引表的列信息，用于维护该表中的索引信息
    public static final String GLOBAL_INDEX_TABLE_COL = "idxs";
    // global_idx联合索引的内部column分割符
    public static final String GLOBAL_INDEX_TABLE_INDEX_INNER_SEPARATOR = ",";
    // global_idx索引之间的分割符
    public static final String GLOBAL_INDEX_TABLE_INDEX_SEPARATOR = ";";

    // 用于维护表中的列信息
    public static final String GLOBAL_DESC_TABLE = "global_desc";
    public static final byte[] GLOBAL_DESC_TABLE_BYTES = Bytes.toBytes("global:global_desc");

    // desc表的列信息，用于维护该表中的列信息
    public static final String GLOBAL_DESC_TABLE_COL = "descs";

    // global_desc联合索引的内部column分割符
    public static final String GLOBAL_DESC_TABLE_INNER_SEPARATOR = ",";
    // global_desc索引之间的分割符
    public static final String GLOBAL_DESC_TABLE_SEPARATOR = ";";

    // 每次以5000作为插入或者删除的行数
    public static final Integer THRESHOLD = 5000;


    public static final String QUALIFIER = "qualifiers";
    public static final String COLUMN_FAMILY = "CF";
    public static final byte[] BYTES_COLUMN_FAMILY = Bytes.toBytes("CF");

    // 是否使用索引(查询，插入等) 可以作为测试时使用
    public static final Boolean USE_INDEX = true;

    public static final int MAX_TABLE_INDEX_COUNT = 128;

    public static final int DEFAULT_VARCHAR_LENGTH = 20;


}
