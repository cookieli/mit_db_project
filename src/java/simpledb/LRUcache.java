package simpledb;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Set;



public class LRUcache<K, V> {
	private int capacity;
	private LinkedHashMap<K, V> cache;
	
	public LRUcache(int capacity) {
		this.capacity = capacity;
		cache = new LinkedHashMap<>();
	}
	
	public boolean containsKey(K key) {
		if(cache.containsKey(key))
			return true;
		else
			return false;
	}
	
	public synchronized void put(K key, V value) {
		if(containsKey(key))
			cache.remove(key);
		if(size() == capacity) 
			evict();
		cache.put(key, value);
	}
	
	public synchronized V get(K key) {
		V value;
		if(containsKey(key)) {
			value = cache.get(key);
			put(key, value);
			return value;
		}
		return null;
	}
	
	public void remove(K key) {
		cache.remove(key);
	}
	
	public synchronized V evict() {
		K key;
		Iterator<K> it =  cache.keySet().iterator();
		if(it.hasNext()) {
			key= it.next();
			V value = cache.get(key);
			cache.remove(key);
			return value;
		} else {
			throw new NullPointerException("iterator don't have next");
		}
	}
	public Set<K> keySet() {
		return cache.keySet();
	}
	
	public int size() {
		return cache.size();
	}
}
