package com.github.madzdns.clusterlet;

import java.util.Set;

import net.sf.ehcache.Element;

public class EhCacheMessageStore extends EhCacheStore implements IMessageStore {
	
	public EhCacheMessageStore(String ehcache_conf_path) throws Exception {
		
		super(ehcache_conf_path);
	}

	@Override
	public void updateAwareNodes(String key, long version, Set<Short> awareNodes) {
		
		Element e = cache.get(key);
		
		if(e == null) {
			
			e = new Element(key, new MessageWithNodes(version, awareNodes));
			cache.put(e);
			return;
		}
		
		MessageWithNodes x = ((MessageWithNodes)e.getObjectValue());
		
		if(x.version < version) {
			
			e = new Element(key, new MessageWithNodes(version, awareNodes));
		}
		else if(x.version == version) {
			
			((MessageWithNodes)e.getObjectValue()).awareNodes.addAll(awareNodes);
		}
		else {
			
			return;
		}
		
		cache.put(e);
	}

	@Override
	public Set<Short> getAwareNodes(String key, long version) {
		
		Element e = cache.get(key);
		
		if(e == null) {

			return null;
		}
		
		MessageWithNodes x = ((MessageWithNodes)e.getObjectValue());
		
		if(x.version == version) {
			
			return ((MessageWithNodes)e.getObjectValue()).awareNodes; 
		}
		
		return null;
	}

	@Override
	public void shutdown() {
		
		manager.shutdown();
	}
}
