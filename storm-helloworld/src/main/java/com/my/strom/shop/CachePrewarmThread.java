package com.my.strom.shop;

import com.alibaba.fastjson.JSONArray;

/**
 * 缓存预热线程
 * @author yp-tc-m-7129
 *
 */
public class CachePrewarmThread extends Thread {

	private ZooKeeperSession zkSession = ZooKeeperSession.getInstance();
	
	@Override
	public void run() {
		//1.获取 taskidList
		String taskidList = zkSession.getNodeData("/taskid-list");
		System.out.println("String taskidList----"+ taskidList);
		
		//2.如果taskidList不为空，则尝试遍历每一个taskid
		if(taskidList !="" && taskidList!=null) {
			String[] taskIdArray = taskidList.split(",");
			for(String taskid : taskIdArray) {
				
				//3.获取每一个taskid的分布式锁
				String taskidLockPath = "/taskid-lock-" + taskid;
				boolean result = zkSession.acquireDistributedLock(taskidLockPath);
				
				//3.1如果获取锁失败，就不再等待，尝试获取下一个taskid的锁
				if(!result) {
					continue;
				}
				
				//4.即使拿到了锁，也检查一下锁的预热状态
				String taskidStatusLockPath = "/taskid-status-lock-" + taskid;
				zkSession.acquireDistributedLockForWait(taskidStatusLockPath);  
				
				//6.检查一下预热状态的值，如果为空说明没有预热过她,如果没有预热状态节点就创建一个
				String statusPath = "/taskid-status-" + taskid;
				if(zkSession.isExists(statusPath) == null) {
					zkSession.createNode(statusPath);
				}
				String taskidStatus = zkSession.getNodeData(statusPath);
				if("".equals(taskidStatus)) {
					
					//7.从zk中获取存储的topNlist的字符串，这个名字和你在strom中存放的名字是一致的
					String productidList = zkSession.getNodeData("/task-hot-product-list-" + taskid);
					JSONArray productidJSONArray = JSONArray.parseArray(productidList);
					for(int i = 0; i < productidJSONArray.size(); i++) {
						//8.拿到了排名次序中的productId，准备存放到redis中
						Long productId = productidJSONArray.getLong(i);
						System.out.println("redis中准备存放了数据" + productId);
					}
					
					//9.task预热状态锁标志为预热过了
					zkSession.setNodeData(statusPath, "success"); 
				}
				
				//5.释放所状态，释放锁
				zkSession.releaseDistributedLock(taskidStatusLockPath);
				zkSession.releaseDistributedLock(taskidLockPath);
			}
		}
	}
}
