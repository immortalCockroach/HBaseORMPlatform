package service.hbasemanager.creation.tabledesc;

import service.hbasemanager.entity.tabldesc.TableDescriptor;
import service.hbasemanager.read.TableScanService;

import javax.annotation.Resource;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by immortalCockroach on 9/26/17.
 */
public class GlobalTableDescInfoHolder {
    @Resource
    private TableScanService scanner;

    // 全局索引表，String为数据表名字
    private ConcurrentHashMap<String, TableDescriptor> globalTableDescMap;
}
