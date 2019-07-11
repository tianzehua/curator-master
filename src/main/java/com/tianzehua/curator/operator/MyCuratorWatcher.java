package com.tianzehua.curator.operator;

import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.zookeeper.WatchedEvent;

/**
 * MyCuratorWatcher
 * @author tianzehua
 * @date 2019/07/04
 */
public class MyCuratorWatcher implements CuratorWatcher {

    @Override
    public void process(WatchedEvent watchedEvent) throws Exception {
        System.out.println("触发watcher，节点路径为：" + watchedEvent.getPath());
    }
}
