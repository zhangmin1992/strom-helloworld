package com.my.strom.shop;

import java.io.IOException;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

/**
 * 测试zk连接基础操作，测试等待重试步骤的MainTest
 * @author yp-tc-m-7129
 *
 */
public class MainTest {

	private  static ZooKeeper zookeeper;
	
	private static ZooKeeperSession zkSession;
	
	public static void main(String[] args) {
		
		/**
		 * 一直等待直到获取到锁
		 */
		/*try {
			int i = 9 /0;
		} catch (Exception e) {
			int count = 0;
			while(true) {
				try {
					Thread.sleep(1000); 
					int i = 9 /0;
				} catch (Exception e2) {
					count++;
					System.out.println("the " + count + " times try to acquire lock for taskid-list-lock......");
					continue;
				}
				System.out.println("success to acquire lock for taskid-list-lock after " + count + " times try......");
				break;
			}
		}*/
		
		/**
		 * 一直等待N次获取不到就不再重试
		 */
		/*try {
			int i = 9 /0;
		} catch (Exception e) {
			int count = 0;
			while(true) {
				try {
					if(count >=8) {
						break;
					}
					Thread.sleep(1000); 
					int i = 9 /0;
				} catch (Exception e2) {
					count++;
					System.out.println("the " + count + " times try to acquire lock for taskid-list-lock......");
					continue;
				}
				System.out.println("success to acquire lock for taskid-list-lock after " + count + " times try......");
				break;
			}
		}*/
		/*try {
			zookeeper = new ZooKeeper("127.0.0.1:2181", 5000,new MyZooKeeperWatcher());
			zookeeper.create("/test", "".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
			zookeeper.create(path, "".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
			System.out.println("ok");
			
			zkSession = ZooKeeperSession.getInstance();
			//zkSession.acquireDistributedLockForWait("/taskid-list-lock");
			Stat resStat = zkSession.isExists("/taskid-list");
			if(resStat != null){
				zkSession.deleteNode("/taskid-list");
	        }else {
	        	System.out.println("节点不存在");
	        }
			if(zkSession.isExists("/task-hot-product-list-5") != null){
				zkSession.deleteNode("/task-hot-product-list-5");
	        }
			if(zkSession.isExists("/task-hot-product-list-6") != null){
				zkSession.deleteNode("/task-hot-product-list-6");
	        }
			if(zkSession.isExists("/task-hot-product-list-4") != null){
				zkSession.deleteNode("/task-hot-product-list-4");
	        }
			if(zkSession.isExists("/task-hot-product-list-3") != null){
				zkSession.deleteNode("/task-hot-product-list-3");
	        }
			System.out.println(resStat);
			System.out.println("ok");
		} catch (Exception e) {
			System.out.println("error");
			e.printStackTrace();
		}*/
		
		/**
		 * 测试缓存预热部分
		 */
		new CachePrewarmThread().start();
	}

}
