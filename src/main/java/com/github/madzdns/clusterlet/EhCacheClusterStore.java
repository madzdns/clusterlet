package com.github.madzdns.clusterlet;

import java.util.Iterator;

import net.sf.ehcache.Element;

public class EhCacheClusterStore extends EhCacheStore implements IClusterStore {
	
	public EhCacheClusterStore(String ehcache_conf_path) throws Exception {
		
		super(ehcache_conf_path);
	}
	
	@Override
	public void update(ClusterNode node) {
		
		Element e = new Element(node.getId(), node);
		cache.put(e);
	}
	
	@Override
	public ClusterNode get(Short id) {
		
		Element e = cache.get(id);
		
		if(e == null) {
			
			return null;
		}
		
		return (ClusterNode) e.getObjectValue();
	}
	
	@Override
	public void shutdown() {
		
		manager.shutdown();
	}
	
	@Override
	public void iterator(IClusterStoreIteratorCallback callbak) {
		
		for(Iterator<?> it = cache.getKeys().iterator(); it.hasNext();) {
			
			Element e = cache.get(it.next());
			
			if(e != null) {
				
				if(e.getObjectValue() instanceof ClusterNode) {
				
					callbak.next((ClusterNode)e.getObjectValue());
				}
			}
		}
	}
}
