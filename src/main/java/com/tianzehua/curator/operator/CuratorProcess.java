package com.tianzehua.curator.operator;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Stat;
import org.springframework.aop.scope.ScopedProxyUtils;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.List;

/**
 * Curator Process Class
 * @author tianzehua
 * @date 2019/07/03
 */
@Configuration
public class CuratorProcess {

    public CuratorFramework client = null;

    public static final String zkServerPath = "192.168.103.128:2181";


    @PostConstruct
    public void postConstruct(){
        /* 实例化 */
        CuratorOperator cto = new CuratorOperator(zkServerPath);
        boolean isZkCuratorStarted = cto.client.isStarted();
        client = cto.client;

        System.out.println("当前客户的状态：" + (isZkCuratorStarted ? "连接中" : "已关闭"));

        /* 1.创建节点 */
        /* 节点的路径 */
        String nodePath = "/curator/test/1";
        /* 节点的数据 */
        byte[] data = "tianzehua".getBytes();

        try {
            /* 调用创建节点的方法,如果需要，将创建父节点;选择模式：临时节点，持久节点等； */
            client.create().creatingParentsIfNeeded()
                    .withMode(CreateMode.PERSISTENT)
                    .withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE)
                    .forPath(nodePath,data);

            /* 2.更新节点数据 */
            byte[] newData = "batman".getBytes();
            cto.client.setData().withVersion(0).forPath(nodePath, newData);

            /* 3.删除节点 */
            client.delete()
                  .guaranteed()					// 如果删除失败，那么在后端还是继续会删除，直到成功
                  .deletingChildrenIfNeeded()	// 如果有子节点，就删除
                  .withVersion(0)
                  .forPath(nodePath);
            /* 更新和删除都需要当前的版本号，如果版本号不对的话，无法删除 */

            /* 4.读取节点数据 */
            /* 节点的状态信息,storingStatIn(stat) 得到节点的状态信息，通过状态信息可以得到· */
            Stat stat = new Stat();
            byte[] data1 = cto.client.getData().storingStatIn(stat).forPath(nodePath);
            System.out.println("节点" + nodePath + "的数据为: " + new String(data1));
            System.out.println("该节点的版本号为: " + stat.getVersion());


            // 5.查询子节点
		    List<String> childNodes = cto.client.getChildren()
											.forPath(nodePath);
	        System.out.println("开始打印子节点：");
            for (String s : childNodes) {
                System.out.println(s);
            }

            /* 6.判断节点是否存在,如果不存在则为空 */
		   Stat statExist = cto.client.checkExists().forPath(nodePath + "/abc");
		   System.out.println(statExist);

            /* 7.watcher 事件  当使用usingWatcher的时候，监听只会触发一次，监听完毕后就销毁 */
            /* usingWatcher 有两种创建方式，CuratorWatcher和 Watcher ，需要实现相应的接口 */
     		cto.client.getData().usingWatcher(new MyCuratorWatcher()).forPath(nodePath);
     		/*cto.client.getData().usingWatcher(new MyWatcher()).forPath(nodePath);*/

            /* 8.为节点添加watcher 不会一次销毁 */
            /* NodeCache: 监听数据节点的变更，会触发事件*/
		    final NodeCache nodeCache = new NodeCache(cto.client, nodePath);
		    /* buildInitial : 初始化的时候获取node的值并且缓存 start(true) */
		    nodeCache.start(true);
            if (nodeCache.getCurrentData() != null) {
                    System.out.println("节点初始化数据为：" + new String(nodeCache.getCurrentData().getData()));
                } else {
                    System.out.println("节点初始化数据为空...");
            }
		    nodeCache.getListenable().addListener(new NodeCacheListener() {
                public void nodeChanged() throws Exception {
                    if (nodeCache.getCurrentData() == null) {
                        System.out.println("空");
                        return;
                    }
                    String data = new String(nodeCache.getCurrentData().getData());
                    System.out.println("节点路径：" + nodeCache.getCurrentData().getPath() + "数据：" + data);
                }
		    });

            Thread.sleep(10000);
        }catch (Exception e){
            System.out.println(e);
        }
        /* 关闭连接 */
        closeZKClient();
        boolean isZkCuratorStarted2 = cto.client.isStarted();
        System.out.println("当前客户的状态：" + (isZkCuratorStarted2 ? "连接中" : "已关闭"));

    }

    /**
     *
     * @Description: 关闭zk客户端连接
     */
    public void closeZKClient() {
        if (client != null) {
            this.client.close();
        }
    }



}
