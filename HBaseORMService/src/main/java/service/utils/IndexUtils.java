package service.utils;

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

    public static List<Index> getHitIndexesWithinQualifiersWhenInsert(byte[] tableName, String[] qualifiers, List<Index> existedIndexes) {
        List<Index> res = new ArrayList<>();
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
                    break;
                }
            }
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
     * @param queryColumns
     * @return
     */
    public static int[] getHitIndexWhenQuery(List<Index> existedIndex, List<String> queryColumns) {
        int indexCount = existedIndex.size();
        if (existedIndex == null || existedIndex.size() == 0 || queryColumns == null || queryColumns.size() == 0) {
            return new int[indexCount];
        }

        // 保存每个index最多被匹配的列数
        int[] maxRes = new int[indexCount];
        Set<String> querySet = new HashSet<>(queryColumns);
        int idx = 0;
        // 记录每个字符串的匹配次数
        Map<String, Integer> columnCountMap = new HashMap<>();
        List<List<String>> indexes = new ArrayList<>();
        for (Index index : existedIndex) {
            indexes.add(index.getIndexColumnList());
        }
        for (List<String> index : indexes) {
            int hitCount = 0;
            // 计算每个index最多被queryColumns匹配的数量，保存到res中
            for (String column : index) {
                if (querySet.contains(column)) {
                    Integer c = columnCountMap.get(column);
                    if (c == null) {
                        c = 0;
                    }
                    columnCountMap.put(column, c + 1);
                    hitCount++;
                } else {
                    break;
                }
            }
            maxRes[idx] = hitCount;
            idx++;
        }
        // 如果只命中了小于等于1个索引，则直接返回即可
        if (noZeroEleCount(maxRes) <= 1) {
            return maxRes;
        }
        int[] res = new int[indexCount];
        dfs(0, res, maxRes, indexes, queryColumns, columnCountMap);
        return res;
    }

    /**
     * 求解最优命中匹配数
     *
     * @param level
     * @param res
     * @param tmp
     * @param existedIndex
     * @param queryColumns
     * @param columnCountMap
     * @return
     */
    private static void dfs(int level, int[] res, int[] tmp, List<List<String>> existedIndex, List<String> queryColumns, Map<String, Integer> columnCountMap) {
        // 遍历完毕则验证结果
        if (level == tmp.length) {
            if (validate(columnCountMap)) {

                int sumRes = sum(res);
                int sumTmp = sum(tmp);
                // 如果tmp命中的column大于原先的结果，或者两者相等并且tmp命中的索引数量较少
                if ((sumTmp > sumRes) || ((sumTmp == sumRes) && (noZeroEleCount(tmp) < noZeroEleCount(res)))) {
                    System.arraycopy(tmp, 0, res, 0, level);
                }
            }
        } else {
            // 获得该index命中的size，并依次前向递减并dfs
            int hitSize = tmp[level];
            List<String> indexes = existedIndex.get(level);

            // 移除0~hitSize个元素进行dfs
            for (int i = hitSize; i >= 0; i--) {
                if (i < hitSize) {
                    String removedColumn = indexes.get(i);
                    tmp[level]--;
                    Integer columnCount = columnCountMap.get(removedColumn);
                    columnCountMap.put(removedColumn, columnCount - 1);
                }
                dfs(level + 1, res, tmp, existedIndex, queryColumns, columnCountMap);
            }

            // 恢复原先的结果
            tmp[level] = hitSize;
            for (int i = 0; i <= hitSize - 1; i++) {
                String removedColumn = indexes.get(i);
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
    private static int noZeroEleCount(int[] array) {
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
     * @param hitNum
     * @return
     */
    public static boolean hitAny(int[] hitNum) {
        return sum(hitNum) != 0;
    }
}
