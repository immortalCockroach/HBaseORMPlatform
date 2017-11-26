package client.controller;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;

public class Test {
    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {

        ZooKeeper zk = new ZooKeeper("10.109.246.125:21811", 10000, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                System.err.println("asd asd");
            }
        });

        System.out.println(zk.getClass());
        if (zk.exists("/test", false) == null) {
            zk.create("/test", "znode1".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
        System.out.println(new String(zk.getData("/test", false, null)));
    }
}
