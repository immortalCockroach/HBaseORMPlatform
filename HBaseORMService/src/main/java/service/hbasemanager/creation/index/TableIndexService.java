package service.hbasemanager.creation.index;


import com.alibaba.fastjson.JSONArray;
import com.immortalcockroach.hbaseorm.constant.CommonConstants;
import com.immortalcockroach.hbaseorm.result.BaseResult;
import com.immortalcockroach.hbaseorm.result.ListResult;
import com.immortalcockroach.hbaseorm.result.PlainResult;
import com.immortalcockroach.hbaseorm.util.Bytes;
import com.immortalcockroach.hbaseorm.util.ResultUtil;
import org.springframework.stereotype.Service;
import service.constants.ServiceConstants;
import service.hbasemanager.entity.index.Index;
import service.hbasemanager.insert.TableInsertService;
import service.hbasemanager.read.TableGetService;
import service.hbasemanager.read.TableScanService;
import service.utils.ByteArrayUtils;
import service.utils.IndexUtils;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 表索引的创建
 * Created by immortalCockroach on 8/31/17.
 */
@Service
public class TableIndexService {

    @Resource
    private TableInsertService inserter;

    @Resource
    private TableGetService getter;

    @Resource
    private TableScanService scanner;

    @Resource
    private GlobalIndexInfoHolder indexInfoHolder;

    /**
     * 根据数据表创建索引表中的索引信息，具体的步骤如下
     * 1、扫描数据表(取qualifiers中的列)
     * 2、如果数据表中没有数据，则直接返回
     * 3、如果有，则取出rowkey + qualifiers，并以列名+列值+数据表rowkey的形式写入
     * （对于多列的索引为列名1 + 列名2 + 列值1 + 列值2 + 主表rowkey）
     * 4、更新global_idx表
     *
     * @param indexTableName
     * @param qualifiers     包含rowkey的qualifers
     * @return
     */
    public BaseResult createIndex(byte[] tableName, byte[] indexTableName, String[] qualifiers) {
        int indexNum;
        if ((indexNum = indexInfoHolder.indexExists(tableName, qualifiers)) == -1) {
            return ResultUtil.getFailedBaseResult("该索引已经存在或者为某个索引的前缀索引或者索引数量超过128");
        }
        ListResult res = scanner.scan(tableName, qualifiers);
        if (!res.getSuccess()) {
            return ResultUtil.getFailedBaseResult("索引建立失败，请稍后重试");
        }
        int size = res.getSize();

        // 没有数据则在global_idx表中创建对对应的信息即可
        if (size == 0) {
            this.updateIndexTable(indexTableName, qualifiers);
            return ResultUtil.getSuccessBaseResult();
        } else {
            // 更新内存map的数据
            this.updateIndexTable(indexTableName, qualifiers);
            JSONArray rows = res.getData();
            List<Map<String, byte[]>> valuesList = new ArrayList<>(size);
            // 将数据转换为对应的形式然后put到index表中
            for (int i = 0; i <= size - 1; i++) {
                // 将每一行中，qualifiers中的内容进行转义字符的预处理并拼接成index表的rowKey
                byte[] indexRowKey = ByteArrayUtils.generateIndexRowKey(rows.getJSONObject(i), qualifiers, ServiceConstants.EOT, ServiceConstants.ESC, ServiceConstants.NUL, (byte) indexNum);

                // index表
                Map<String, byte[]> lineMap = new HashMap<>(2);
                lineMap.put(CommonConstants.ROW_KEY, indexRowKey);
                // 由于put的数据必须有列，所以此处指定一个，后续不使用col数据
                lineMap.put(ServiceConstants.GLOBAL_INDEX_TABLE_COL, Bytes.toBytes("1"));
                valuesList.add(lineMap);
            }
            // 调用批量插入的方法插入global_idx表
            BaseResult createResult = inserter.insertBatch(indexTableName, valuesList);

            return createResult;

        }
    }

    /**
     * 新建索引时更新对应的索引表
     * @param indexTableName
     * @param qualifiers
     */
    private void updateIndexTable(byte[] indexTableName, String[] qualifiers) {
        PlainResult result = getter.read(ServiceConstants.GLOBAL_INDEX_TABLE_BYTES, indexTableName, new String[]{ServiceConstants.GLOBAL_INDEX_TABLE_COL});
        // 该表的第一个index
        if (result.getSize() == 0) {
            String combinedIndex = IndexUtils.getCombinedIndex(qualifiers);

            Map<String, byte[]> lineMap = new HashMap<>();
            lineMap.put(CommonConstants.ROW_KEY, indexTableName);
            lineMap.put(ServiceConstants.GLOBAL_INDEX_TABLE_COL, Bytes.toBytes(combinedIndex));
            inserter.insert(ServiceConstants.GLOBAL_INDEX_TABLE_BYTES, lineMap);
        } else {
            // 该表已经有索引，此处将其拼接到后面
            String combinedIndex = IndexUtils.getCombinedIndex(qualifiers);

            byte[] origin = result.getData().getBytes(ServiceConstants.GLOBAL_INDEX_TABLE_COL);
            String originIndex = Bytes.toString(origin);
            String newIndex = originIndex + ServiceConstants.GLOBAL_INDEX_TABLE_INDEX_SEPARATOR + combinedIndex;

            Map<String, byte[]> lineMap = new HashMap<>();
            lineMap.put(CommonConstants.ROW_KEY, indexTableName);
            lineMap.put(ServiceConstants.GLOBAL_INDEX_TABLE_COL, Bytes.toBytes(newIndex));
            inserter.insert(ServiceConstants.GLOBAL_INDEX_TABLE_BYTES, lineMap);
        }
        indexInfoHolder.updateGlobalMap(indexTableName, qualifiers);
    }

    /**
     * 当数据插入时根据命中的索引信息更新对应的索引表
     *
     * @param tableName
     * @param valuesMap
     * @param hitIndexes
     * @return
     */
    public BaseResult updateIndexWhenInsert(byte[] tableName, List<Map<String, byte[]>> valuesMap, List<Index>
            hitIndexes) {
        byte[] indexTableName = ByteArrayUtils.getIndexTableName(tableName);

        // 更新索引表的数据信息
        List<Map<String, byte[]>> valuesList = new ArrayList<>(hitIndexes.size() * valuesMap.size());
        for (Index hitIndex : hitIndexes) {
            // 根据每个索引的信息更新索引表
            String[] qualifiers = hitIndex.getIndexColumnList().toArray(new String[]{});

            for (Map<String, byte[]> row : valuesMap) {
                // 将每一行的数据按照当前索引qualifiers进行拼接
                byte[] indexRowKey = ByteArrayUtils.generateIndexRowKey(row, qualifiers, ServiceConstants.EOT,
                        ServiceConstants.ESC, ServiceConstants.NUL, (byte) hitIndex.getIndexNum());

                Map<String, byte[]> lineMap = new HashMap<>(2);
                lineMap.put(CommonConstants.ROW_KEY, indexRowKey);
                // 由于put的数据必须有列，所以此处指定一个，后续不使用col数据
                lineMap.put(ServiceConstants.GLOBAL_INDEX_TABLE_COL, Bytes.toBytes("1"));
                valuesList.add(lineMap);
            }

        }
        BaseResult updateIndexResult = inserter.insertBatch(indexTableName, valuesList);
        return updateIndexResult;

    }

    /**
     * 根据表中的索引和插入对应的qualifiers，判断需要更新的索引
     * 如果某个索引中包含qualifers的列，则代表hit
     *
     * @param tableName
     * @param qualifiers
     * @return
     */
    public List<Index> getHitIndexesWithinQualifiersWhenInsert(byte[] tableName, String[] qualifiers) {

        List<Index> existedIndexes = indexInfoHolder.getTableIndexes(tableName);
        if (existedIndexes == null || existedIndexes.size() == 0) {
            return new ArrayList<>();
        }
        return IndexUtils.getHitIndexesWithinQualifiersWhenInsert(tableName, qualifiers, existedIndexes);
    }

}
