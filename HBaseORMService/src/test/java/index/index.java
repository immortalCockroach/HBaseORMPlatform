package index;

public class index {
/*    *//**
     * @param existedIndex
     * @param queryColumns
     * @return
     *//*
    public static int[] getHitIndexWhenQuery(List<List<String>> existedIndex, List<String> queryColumns) {
        int indexCount = existedIndex.size();

        // 保存每个index最多被匹配的列数
        int[] maxRes = new int[indexCount];
        Set<String> querySet = new HashSet<>(queryColumns);
        int idx = 0;
        // 记录每个字符串的匹配次数
        Map<String, Integer> columnCountMap = new HashMap<>();
        for (List<String> index : existedIndex) {
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
        dfs(0, res, maxRes, existedIndex, queryColumns, columnCountMap);
        return res;
    }

    *//**
     * 求解最优命中匹配数
     *
     * @param level
     * @param res
     * @param tmp
     * @param existedIndex
     * @param queryColumns
     * @param columnCountMap
     * @return
     *//*
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

    *//**
     * 验证map的合法性，此处的map为queryColumn的每一列被hit的次数
     * 因为queryColumn中列的唯一性，因此map的values至多是1
     *
     * @param columnCountMap
     * @return
     *//*
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

    *//**
     * 计算2个结果的索引命中数量
     *
     * @param array
     * @return
     *//*
    private static int noZeroEleCount(int[] array) {
        int count = 0;
        for (int e : array) {
            if (e != 0) {
                count++;
            }
        }
        return count;
    }

    public static void main(String[] args) {

        List<List<String>> existedIndex = new ArrayList<>();
        List<String> list1 = new ArrayList<>();
        list1.add("a1");
        list1.add("d1");
        list1.add("b1");

        List<String> list2 = new ArrayList<>();
        list2.add("d1");
        list2.add("c1");
        list2.add("e1");

        existedIndex.add(list1);
        existedIndex.add(list2);

        List<String> queryColumns = new ArrayList<>();
        queryColumns.add("a1");
        queryColumns.add("d1");
        queryColumns.add("c1");
        queryColumns.add("b1");

        // long start = System.currentTimeMillis();
        int[] res = getHitIndexWhenQuery(existedIndex, queryColumns);
        // System.out.println(System.currentTimeMillis() - start);
        for (int e : res) {
            System.out.print(e + " ");
        }
        System.out.println();
    }*/

}
