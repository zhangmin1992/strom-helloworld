package com.my.strom.shop;

import com.alibaba.fastjson.JSONObject;

import storm.trident.util.LRUMap;

public class LRUMapMain {

	public static void main(String[] args) {
		/**
		 * LRUMap只会保留最近访问的3次请求的计数
		 */
		LRUMap<String, Integer> countMap = new LRUMap<String, Integer>(3);
		countMap.put("zhangmin", 1);
		countMap.put("zhangmin", 2);
		countMap.put("cc", 3);
		countMap.put("cc", 1);
		countMap.put("mm", 4);
		countMap.put("zhangmin", 4);
		System.out.println(JSONObject.toJSONString(countMap));
	}

}
