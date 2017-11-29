package service.utils;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.immortalcockroach.hbaseorm.constant.CommonConstants;
import com.immortalcockroach.hbaseorm.param.enums.ArithmeticOperatorEnum;
import com.immortalcockroach.hbaseorm.result.ListResult;
import service.constants.ServiceConstants;
import service.hbasemanager.entity.index.Index;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 用于判定增删改查时索引的命中与更新
 */
public class IndexUtils {

    /**
     * 将qualifiers通过','连接 返回联合索引的格式（单列索引也适合）
     *
     * @param qualifiers
     * @return
     */
    public static String getCombinedIndex(String[] qualifiers) {
        StringBuilder builder = new StringBuilder();
        for (String qualifier : qualifiers) {
            builder.append(qualifier + ServiceConstants.GLOBAL_INDEX_TABLE_INDEX_INNER_SEPARATOR);
        }
        return builder.substring(0, builder.length() - 1);
    }


    public static List<Index> getHitIndexesWithinQualifiersWhenUpdate(String[] qualifiers, List<Index> existedIndexes) {
        List<Index> res = new ArrayList<>();
        // qualifiers的集合
        Set<String> operColumns = new HashSet<>();
        for (String qualifier : qualifiers) {
            operColumns.add(qualifier);
        }

        for (Index existedIndex : existedIndexes) {
            String[] indexColumns = existedIndex.getIndexColumnList();

            int hitNum = 0;

            for (int i = 0; i <= indexColumns.length - 1; i++) {
                if (operColumns.contains(indexColumns[i])) {
                    hitNum++;
                    break;
                }
            }
            // 有一个命中就算命中了
            if (hitNum > 0) {
                res.add(existedIndex);
            }

        }
        return res;
    }

    /**
     * 给定表的所有索引以及查询列，求最优的索引结构
     *
     * @param existedIndex
     * @param queryTypeMap
     * @return
     */
    public static int[] getHitIndexWhenQuery(List<Index> existedIndex, Map<String, Integer> queryTypeMap) {
        int indexCount = 0;
        if (!ServiceConstants.USE_INDEX || existedIndex == null || (indexCount = existedIndex.size()) == 0 || queryTypeMap == null || queryTypeMap.size() == 0) {
            return new int[indexCount];
        }
        // 保存每个index最多被匹配的列数
        int[] maxRes = new int[indexCount];
        // rowkey不作索引的前缀匹配，在原始表查询前统一进行合并和过滤
        // 此处需要修改查询的列，索引使用单独的列
        Set<String> querySet = new HashSet<>(queryTypeMap.keySet());

        querySet.remove(CommonConstants.ROW_KEY);

        // 记录每个字符串的匹配次数
        Map<String, Integer> columnCountMap = new HashMap<>();
        List<String[]> indexes = new ArrayList<>();
        for (Index index : existedIndex) {
            indexes.add(index.getIndexColumnList());
        }
        int idx = 0;
        for (String[] index : indexes) {
            int hitCount = 0;
            // 计算每个index最多被queryColumns匹配的数量，保存到res中
            int size = index.length;
            for (int i = 0; i <= size - 1; i++) {
                String column = index[i];
                if (querySet.contains(column)) {
                    // 如果该查询不是!=且(是等值查询，或者前面一个是等值查询)，则加入;否则说明该索引列不可命中，直接break;
                    Integer queryType = queryTypeMap.get(column);
                    if (!ArithmeticOperatorEnum.isNotEqualQuery(queryType) && (ArithmeticOperatorEnum.isEqualQuery(queryType)
                            || (i == 0 || ArithmeticOperatorEnum.isEqualQuery(queryTypeMap.get(index[i - 1]))))) {
                        Integer c = columnCountMap.get(column);
                        if (c == null) {
                            c = 0;
                        }
                        columnCountMap.put(column, c + 1);
                        hitCount++;
                    } else {
                        break;
                    }

                } else {
                    break;
                }
            }
            maxRes[idx] = hitCount;
            idx++;
        }
        // 如果只命中了小于等于1个索引或者当前的命中已经合法(此时为最大可能的匹配)，则直接返回即可
        if (indexHitCount(maxRes) <= 1 || validate(columnCountMap)) {
            return maxRes;
        }
        int[] res = new int[indexCount];
        dfs(0, res, maxRes, indexes, columnCountMap);
        return res;
    }

    /**
     * 求解最优命中匹配数
     * 核心是求解每个索引的前缀的最后一个匹配至多包含一个非等值查询的情况下
     * 使得命中的列尽量多，在列一致的情况下，命中的索引数量尽量少的结果
     *
     * @param level
     * @param res
     * @param tmp
     * @param existedIndex
     * @param columnCountMap
     * @return
     */
    private static void dfs(int level, int[] res, int[] tmp, List<String[]> existedIndex, Map<String, Integer>
            columnCountMap) {
        // 遍历完毕则验证结果
        if (level == tmp.length) {
            if (validate(columnCountMap)) {

                int sumRes = sum(res);
                int sumTmp = sum(tmp);
                // 如果tmp命中的column大于原先的结果，或者两者相等并且tmp命中的索引数量较少
                if ((sumTmp > sumRes) || ((sumTmp == sumRes) && (indexHitCount(tmp) < indexHitCount(res)))) {
                    System.arraycopy(tmp, 0, res, 0, level);
                }
            }
        } else {
            // 获得该index命中的size，并依次前向递减并dfs
            int hitSize = tmp[level];
            String[] indexes = existedIndex.get(level);

            // 移除0~hitSize个元素进行dfs
            for (int i = hitSize; i >= 0; i--) {
                // i == hitSize的情况下为不移除任何列进行dfs
                if (i < hitSize) {
                    String removedColumn = indexes[i];
                    tmp[level]--;
                    Integer columnCount = columnCountMap.get(removedColumn);
                    columnCountMap.put(removedColumn, columnCount - 1);
                }
                dfs(level + 1, res, tmp, existedIndex, columnCountMap);
            }

            // 恢复原先的结果
            tmp[level] = hitSize;
            for (int i = 0; i <= hitSize - 1; i++) {
                String removedColumn = indexes[i];
                Integer columnCount = columnCountMap.get(removedColumn);
                if (columnCount == null) {
                    columnCount = 0;
                }
                columnCountMap.put(removedColumn, columnCount + 1);
            }
        }
    }

    /**
     * 验证map的合法性，此处的map为queryColumn的每一列被hit的次数
     * 因为queryColumn中列的唯一性，因此map的values至多是1
     *
     * @param columnCountMap
     * @return
     */
    private static boolean validate(Map<String, Integer> columnCountMap) {
        for (Integer count : columnCountMap.values()) {
            // 此处为大于1是因为有的列在dfs时变为0
            if (count > 1) {
                return false;
            }
        }
        return true;
    }

    private static int sum(int[] array) {
        int sum = 0;
        for (int e : array) {
            sum += e;
        }
        return sum;
    }

    /**
     * 计算2个结果的索引命中数量
     *
     * @param array
     * @return
     */
    private static int indexHitCount(int[] array) {
        int count = 0;
        for (int e : array) {
            if (e != 0) {
                count++;
            }
        }
        return count;
    }

    /**
     * 查询是否命中了索引
     *
     * @param hitNum
     * @return
     */
    public static boolean hitAnyIdex(int[] hitNum) {
        return sum(hitNum) != 0;
    }

    public static List<byte[]> buildIndexTableRowKey(ListResult updatedRows, List<Index> indexes) {
        int size = updatedRows.getSize();
        List<byte[]> rowkeys = new ArrayList<>(size * indexes.size());
        JSONArray array = updatedRows.getData();

        for (int i = 0; i <= size - 1; i++) {
            // 针对每一行，构建所有索引的索引表行健
            JSONObject row = array.getJSONObject(i);
            for (Index index : indexes) {
                String[] qualifiers = index.getIndexColumnList();
                rowkeys.add(ByteArrayUtils.generateIndexRowKey(row, qualifiers, (byte) index.getIndexNum()));

            }
        }
        return rowkeys;
    }
}
