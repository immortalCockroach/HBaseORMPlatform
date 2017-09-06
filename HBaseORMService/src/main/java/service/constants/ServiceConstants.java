package service.constants;

import com.immortalcockroach.hbaseorm.util.Bytes;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.protobuf.generated.FilterProtos;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 全局常量部分
 * Created by immortalCockRoach on 2016/5/31.
 */
public class ServiceConstants {
    public static final byte EOT = 4; // 分隔符
    public static final byte ESC = 27; // 转义符
    // 索引表null值的转义符 可能暂时不需要
    public static final byte NUL = 0; //


    public static final String TABLE = "table";

    public static final String INDEX_SUFFIX = "_idx";

    // 用于维护表中的索引列
    public static final String GLOBAL_INDEX_TABLE = "global_idx";
    public static final byte[] GLOBAL_INDEX_TABLE_BYTES = Bytes.toBytes("global_idx");

    public static final String GLOBAL_INDEX_TABLE_COL = "idxs";
    // global_idx联合索引的内部column分割符
    public static final String GLOBAL_INDEX_TABLE_INDEX_INNER_SEPARATOR = ",";
    // global_idx索引的分割符
    public static final String GLOBAL_INDEX_TABLE_INDEX_SEPARATOR = "_";

    // 每次以1000作为插入的行数
    public static final Integer THRESHOLD = 1000;


    public static final String QUALIFIER = "qualifiers";
    public static final String COLUMN_FAMILY = "CF";
    public static final byte[] BYTES_COLUMN_FAMILY = Bytes.toBytes("CF");

    // 是否使用索引(查询，插入等) 可以作为测试时使用
    public static final Boolean USE_INDEX = true;
}
