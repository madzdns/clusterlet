package com.github.madzdns.clusterlet;

import java.io.IOException;
import java.io.Serializable;
import java.util.Set;

import org.apache.commons.jcs.engine.CacheElement;
import org.apache.commons.jcs.engine.behavior.ICacheElement;
import org.apache.commons.jcs.engine.control.CompositeCache;

public class JcsCacheMessageStore extends JcsCacheStore implements
		IMessageStore {
	
	private CompositeCache<Serializable, Serializable> cache;
	
	protected static final String CACHE_NAME = "RESOURCE";
	
	public JcsCacheMessageStore(String jcs_conf_path) throws Exception {
		
		super(jcs_conf_path);
		
		cache = ccm.getCache(CACHE_NAME);
	}

	@Override
	public void updateAwareNodes(String key, long version, Set<Short> awareNodes) {
		
		ICacheElement<Serializable, Serializable> e = cache.get(key);
		
		if(e == null) {
			
			e = new CacheElement<Serializable, Serializable>(CACHE_NAME, key, new MessageWithNodes(version, awareNodes));
			
			try {
				
				cache.update(e);
				
			} catch (IOException e1) {
			}
			
			return;
		}
		
		MessageWithNodes x = ((MessageWithNodes)e.getVal());
		
		if(x.version < version) {
			
			e = new CacheElement<Serializable, Serializable>(CACHE_NAME, key, new MessageWithNodes(version, awareNodes));
		}
		else if(x.version == version) {
			
			((MessageWithNodes)e.getVal()).awareNodes.addAll(awareNodes);
		}
		else {
			
			return;
		}
		
		try {
			
			cache.update(e);
			
		} catch (IOException e1) {
		}
	}

	@Override
	public Set<Short> getAwareNodes(String key, long version) {
		
		ICacheElement<Serializable, Serializable> e = cache.get(key);
		
		if(e == null) {

			return null;
		}
		
		MessageWithNodes x = ((MessageWithNodes)e.getVal());
		
		if(x.version == version) {
			
			return ((MessageWithNodes)e.getVal()).awareNodes; 
		}
		
		return null;
	}

	@Override
	public void shutdown() {
		
		cache.dispose();
	}

}
