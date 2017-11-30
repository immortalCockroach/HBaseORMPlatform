package com.immortalcockroach.hbaseorm.param.enums;

/**
 * Created by immortalCockroach on 9/26/17.
 */
public enum ColumnTypeEnum {
    VARCHAR(0, "String", 0),
    BYTE(1, "byte", 1),
    SMALL_INT(2, "short", 2),
    INT(3, "int", 4),
    BIGINT(4, "long", 8);

    private int id;
    private String name;
    // length代表字段的长度，-1为特殊值
    private int length;

    ColumnTypeEnum(int id, String name, int length) {
        this.id = id;
        this.name = name;
        this.length = length;
    }

    public static ColumnTypeEnum getTypeById(int id) {
        for (ColumnTypeEnum columnTypeEnum : ColumnTypeEnum.values()) {
            if (id == columnTypeEnum.getId()) {
                return columnTypeEnum;
            }
        }
        return null;
    }

    public static int getLengthById(int id) {
        for (ColumnTypeEnum columnTypeEnum : ColumnTypeEnum.values()) {
            if (id == columnTypeEnum.getId()) {
                return columnTypeEnum.getLength();
            }
        }
        return 0;
    }


    public static boolean isStringType(int type) {
        return type == VARCHAR.getId();
    }

    public int getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public int getLength() {
        return length;
    }
}
