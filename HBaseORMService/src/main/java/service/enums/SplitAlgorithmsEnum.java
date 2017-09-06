package service.enums;

import com.immortalcockroach.hbaseorm.constant.CommonConstants;
import org.apache.hadoop.hbase.util.RegionSplitter;
import service.hbasemanager.creation.table.splitalgorithms.AutoIncrementSplitor;
import service.hbasemanager.creation.table.splitalgorithms.UUIDStringSplitor;

/**
 * 表切分算法的枚举类
 * 目前只有UUID切分和short, int为自增主键的切分
 * Created by immortalCockroach on 8/29/17.
 */
public enum SplitAlgorithmsEnum {
    UUID_SPLITTER(CommonConstants.UUID, new UUIDStringSplitor()),
    AUTO_INCREMENT_SPLITTER(CommonConstants.AUTO_INCREMENT, new AutoIncrementSplitor());


    private String algorithmName;
    private RegionSplitter.SplitAlgorithm algorithm;

    SplitAlgorithmsEnum(String algorithmName,
                        RegionSplitter.SplitAlgorithm algorithm) {
        this.algorithmName = algorithmName;
        this.algorithm = algorithm;
    }

    public String getAlgorithmName() {
        return algorithmName;
    }

    public RegionSplitter.SplitAlgorithm getAlgorithm() {
        return algorithm;
    }
}
