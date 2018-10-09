package com.my.strom.shop;

import java.io.IOException;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

public class ZooKeeperSession {

	/**
	 * 封装单例的静态内部类
	 * 让静态内部类执行ZooKeeperSession() 方法
	 * @author Administrator
	 *
	 */
	private static class Singleton {
		
		private static ZooKeeperSession instance;
		
		static {
			instance = new ZooKeeperSession();
		}
		
		public static ZooKeeperSession getInstance() {
			return instance;
		}
		
	}
	private ZooKeeper zookeeper;
    
	public ZooKeeperSession() {
		try {
			//注意如果没有直接调用的话，这个是空的，拓扑图启动的时候会报错空指针
			this.zookeeper = new ZooKeeper("127.0.0.1:2181", 50000,new MyZooKeeperWatcher());
			System.out.println("-----zk连接成功------");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	/**
	 * 获取单例,方便其他类直接使用zk
	 * @return
	 */
	public static ZooKeeperSession getInstance() {
		return Singleton.getInstance();
	}
	
	/**
	 * 获取分布式锁就是判断想要创建的目录节点是否已经存在
	 * 只获取一次不会等待
	 */
	public boolean acquireDistributedLock(String path) {
		try {
			zookeeper.create(path, "".getBytes(), 
					Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
			System.out.println("success to acquire lock for " + path);  
			return true;
		} catch (Exception e) {
			System.out.println("fail to acquire lock for " + path);  
		}
		return false;
	}
	
	/**
	 * 获取分布式锁，未获取到会一直等待直到获取到
	 * @param productId
	 */
	public void acquireDistributedLockForWait(String path) {
		try {
			zookeeper.create(path, "".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
			System.out.println("success to acquire lock for taskid-list-lock");  
		} catch (Exception e) {
			// 1. 如果那个商品对应的锁的node，已经存在了，就是已经被别人加锁了，那么就这里就会报错--NodeExistsException
			int count = 0;
			while(true) {
				try {
					//2. 上面没有获取成功，这里等到1000后再次尝试获取
					Thread.sleep(1000); 
					zookeeper.create(path, "".getBytes(),Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
				} catch (Exception e2) {
					//3. 第2次还是没有获取成功，就计数加1，再次执行while条件，等待1000后重新获取
					count++;
					System.out.println("the " + count + " times try to acquire lock for taskid-list-lock......");
					continue;
				}
				//4.直到获取到锁后，跳出循环
				System.out.println("success to acquire lock for taskid-list-lock after " + count + " times try......");
				break;
			}
		}
	}
	
	/**
	 * 释放掉一个分布式锁
	 * @param productId
	 */
	public void releaseDistributedLock(String path ) {
		try {
			zookeeper.delete(path, -1); 
			System.out.println("release the lock for taskid-list-lock......"+ path);  
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public String getNodeData(String path) {
		try {
			return new String(zookeeper.getData(path, false, new Stat()));  
		} catch (Exception e) {
			e.printStackTrace();
		}
		return "";
	}
	
	public void setNodeData(String path, String data) {
		try {
			zookeeper.setData(path, data.getBytes(), -1);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public void createNode(String path) {
		try {
			zookeeper.create(path, "".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		} catch (Exception e) {
		}
	}
	
	public void deleteNode(String path) {
		try {
			zookeeper.delete(path, -1);
		} catch (Exception e) {
			// TODO: handle exception
		}
	}
	
	public Stat isExists(String path) {
		try {
			 return zookeeper.exists(path, true);
		} catch (Exception e) {
			return null;
		}
	}

}
