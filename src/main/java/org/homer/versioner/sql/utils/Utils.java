package org.homer.versioner.sql.utils;

import scala.collection.mutable.MultiMap;

import java.util.*;
import java.util.jar.Attributes;

public class Utils {

	public static <T, K> Map<T, K> newHashMap(){
		return new HashMap<>();
	}

	public static <T, K> Map<T, K> newHashMap(T key, K value){

		Map<T, K> map = newHashMap();
		map.put(key, value);
		return map;
	}

	public static <T, K> Map<T, K> newHashMap(T key, K value, T key2, K value2){

		Map<T, K> map = newHashMap(key, value);
		map.put(key2, value2);
		return map;
	}

	public static <T, K> Map<T, K> newHashMap(T key, K value, T key2, K value2, T key3, K value3){

		Map<T, K> map = newHashMap(key, value, key2, value2);
		map.put(key3, value3);
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

	public static boolean nullSafeEquals(String obj1, String obj2) {

		return (Objects.nonNull(obj1) && Objects.nonNull(obj2) && obj1.equals(obj2))
				|| (Objects.isNull(obj1) && Objects.isNull(obj2));
	}
}
