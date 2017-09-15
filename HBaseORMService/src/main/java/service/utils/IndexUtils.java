package service.utils;

import service.constants.ServiceConstants;
import service.hbasemanager.entity.Index;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
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
                } else {
                    break;
                }
            }
            if (hitNum > 0) {
                res.add(existedIndex);
            }

        }
        return res;
    }
}
