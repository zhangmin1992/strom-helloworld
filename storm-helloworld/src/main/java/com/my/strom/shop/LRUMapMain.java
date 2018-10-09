package com.my.strom.shop;

import com.alibaba.fastjson.JSONObject;

import storm.trident.util.LRUMap;

public class LRUMapMain {

	public static void main(String[] args) {
		/**
		 * LRUMap只会保留最近访问的3次请求的计数
		 */
		LRUMap<String, Integer> countMap = new LRUMap<String, Integer>(3);
		countMap.put("zhangmin", 10);
		countMap.put("rr", 2);
		countMap.put("cc", 300);
		countMap.put("qq", 1);
		countMap.put("mm", 4);
		countMap.put("uu", 4);
		System.out.println(JSONObject.toJSONString(countMap));
	}

}
