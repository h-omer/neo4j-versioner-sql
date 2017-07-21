package org.homer.versioner.sql.utils;

import java.util.*;

public class Utils {

	public static <T, K> Map<T, K> newHashMap(){
		return new HashMap<>();
	}

	public static <T, K> Map<T, K> newHashMap(T key, K value){

		Map<T, K> map = newHashMap();
		map.put(key, value);
		return map;
	}

	public static <T> List<T> newArrayList() {
		return new ArrayList<>();
	}

	@SafeVarargs
	public static <T> List<T> newArrayList(T ... elements) {
		List<T> list = newArrayList();
		Collections.addAll(list, elements);
		return list;
	}
}
