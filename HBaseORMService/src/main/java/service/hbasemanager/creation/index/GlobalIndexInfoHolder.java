package service.hbasemanager.creation.index;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.immortalcockroach.hbaseorm.constant.CommonConstants;
import com.immortalcockroach.hbaseorm.result.ListResult;
import com.immortalcockroach.hbaseorm.result.PlainResult;
import com.immortalcockroach.hbaseorm.util.Bytes;
import service.constants.ServiceConstants;
import service.hbasemanager.entity.HitIndex;
import service.hbasemanager.entity.Index;
import service.hbasemanager.insert.TableInsertService;
import service.hbasemanager.read.TableGetService;
import service.hbasemanager.read.TableScanService;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 全局索引表的维护
 * 全局索引表的信息为
 * tableName——col1_col2_col3,col4(一个表的索引数量不能超过128)
 * 内存map的信息和上面类似，只是索引的信息放在Set中
 * Created by immortalCockroach on 9/1/17.
 */
public class GlobalIndexInfoHolder {
    @Resource
    private TableScanService scanner;

    @Resource
    private TableInsertService inserter;

    @Resource
    private TableGetService getter;

    // 全局索引表，String为数据表名字
    private ConcurrentHashMap<String, List<Index>> globalIndexMap;

    /**
     * Spring注入后使用该方法加载数据表的索引信息
     */
    public void init() {
        if (ServiceConstants.USE_INDEX) {
            ListResult result = scanner.scan(ServiceConstants.GLOBAL_INDEX_TABLE_BYTES, new String[]{ServiceConstants.GLOBAL_INDEX_TABLE_COL});
            if (!result.getSuccess()) {
                return;
            }

            JSONArray rows = result.getData();
            int size = result.getSize();
            globalIndexMap = new ConcurrentHashMap<>();
            for (int i = 0; i <= size - 1; i++) {
                // global_idx表的结构为tableName - index
                JSONObject row = rows.getJSONObject(i);
                byte[] rowkey = row.getBytes(CommonConstants.ROW_KEY);
                String[] indexesColumns = Bytes.toString(row.getBytes
                        (ServiceConstants.GLOBAL_INDEX_TABLE_COL)).split(ServiceConstants.GLOBAL_INDEX_TABLE_INDEX_SEPARATOR);

                List<Index> indexes = new ArrayList<>(indexesColumns.length);
                for (int j = 0; j <= indexesColumns.length; j++) {
                    indexes.add(new Index(indexesColumns[j], j));
                }

                globalIndexMap.put(Bytes.toString(rowkey), indexes);
            }
        }
    }

    /**
     * 判断索引信息是否存在，前缀匹配也算是存在
     *
     * @param qualifiers
     * @return 代表当前索引的序号, 从0开始，为了多级索引查询的便利首部， -1代表已经存在
     */
    public int indexExists(byte[] tableName, String[] qualifiers) {
        String index = getCombinedIndex(qualifiers);
        List<Index> indexes = globalIndexMap.get(Bytes.toString(tableName));
        if (indexes == null) {
            return 0;
        }

        // 如果当前索引为某个联合索引的前缀 则也算存在
        for (Index existedIndex : indexes) {
            if (existedIndex.duplicate(index)) {
                return -1;
            }
        }

        return indexes.size() > 128 ? -1 : indexes.size();
    }

    /**
     * 当添加了索引后将其同步到map和global_idx表中
     * global_idx表的结构为：
     * rowkey ------------ column(idxs)
     * tableName-----------col1_col2_col3,col4
     * 其中col1, col2为单列索引，col3,col4为联合索引
     *
     * @param tableName  数据表的表明
     * @param qualifiers 索引列
     */
    public void updateGlobalTableIndex(byte[] tableName, String[] qualifiers) {
        PlainResult result = getter.read(ServiceConstants.GLOBAL_INDEX_TABLE_BYTES, tableName, new String[]{ServiceConstants.GLOBAL_INDEX_TABLE_COL});
        // 该表的第一个index
        if (result.getSize() == 0) {
            String combinedIndex = getCombinedIndex(qualifiers);

            Map<String, byte[]> lineMap = new HashMap<>();
            lineMap.put(CommonConstants.ROW_KEY, tableName);
            lineMap.put(ServiceConstants.GLOBAL_INDEX_TABLE_COL, Bytes.toBytes(combinedIndex));
            inserter.insert(ServiceConstants.GLOBAL_INDEX_TABLE_BYTES, lineMap);
        } else {
            // 该表已经有索引，此处将其拼接到后面
            String combinedIndex = getCombinedIndex(qualifiers);

            byte[] origin = result.getData().getBytes(ServiceConstants.GLOBAL_INDEX_TABLE_COL);
            String originIndex = Bytes.toString(origin);
            String newIndex = originIndex + ServiceConstants.GLOBAL_INDEX_TABLE_INDEX_SEPARATOR + combinedIndex;

            Map<String, byte[]> lineMap = new HashMap<>();
            lineMap.put(CommonConstants.ROW_KEY, tableName);
            lineMap.put(ServiceConstants.GLOBAL_INDEX_TABLE_COL, Bytes.toBytes(newIndex));
            inserter.insert(ServiceConstants.GLOBAL_INDEX_TABLE_BYTES, lineMap);
        }
        // 更新ndex当前global_idx表维护的索引信息
        List<Index> indexes = globalIndexMap.get(Bytes.toString(tableName));
        if (indexes == null) {
            indexes = new ArrayList<>();
            globalIndexMap.put(Bytes.toString(tableName), indexes);
        }

        indexes.add(new Index(getCombinedIndex(qualifiers), indexes.size()));
    }

    private String getCombinedIndex(String[] qualifiers) {
        StringBuilder builder = new StringBuilder();
        for (String qualifier : qualifiers) {
            builder.append(qualifier + ServiceConstants.GLOBAL_INDEX_TABLE_INDEX_INNER_SEPARATOR);
        }
        return builder.substring(0, builder.length() - 1);
    }

    /**
     * 获得一个表的所有索引信息
     *
     * @param tableName 数据表的表名
     * @return
     */
    public List<Index> getTableIndexes(byte[] tableName) {
        return globalIndexMap.get(Bytes.toString(tableName));
    }

    /**
     * 根据表中的索引和插入对应的qualifiers，判断生效的索引
     * 对tableName对应的globalMap中的索引集合中的每个索引做考察
     * 如果索引的索引列前缀在qualifiers中，则代表hit
     *
     * @param tableName
     * @param qualifiers
     * @return
     */
    public List<HitIndex> getHitIndexesWithinQualifiersWhenInsert(byte[] tableName, String[] qualifiers) {

        List<Index> existedIndexes = getTableIndexes(tableName);
        if (existedIndexes == null || existedIndexes.size() == 0) {
            return new ArrayList<>();
        }

        List<HitIndex> res = new ArrayList<>();
        // qualifiers的集合
        Set<String> operColumns = new HashSet<>();
        for (String qualifier : qualifiers) {
            operColumns.add(qualifier);
        }

        for (Index existedIndex : existedIndexes) {
            List<String> indexColumns = existedIndex.getIndexColumnList();

            int hitNum = 0;
            // 判断每个索引的最大命中前缀，如果大于0则代表命中，需要更新
            for (int i = 0; i <= indexColumns.size() - 1; i++) {
                if (operColumns.contains(indexColumns.get(i))) {
                    hitNum++;
                } else {
                    break;
                }
            }
            if (hitNum > 0) {
                res.add(new HitIndex(indexColumns, existedIndex.getIndexNum(), hitNum));
            }

        }
        return res;
    }
}
