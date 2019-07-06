package com.github.madzdns.clusterlet;

import java.io.IOException;
import java.io.Serializable;
import java.util.Iterator;

import org.apache.commons.jcs.engine.CacheElement;
import org.apache.commons.jcs.engine.behavior.ICacheElement;
import org.apache.commons.jcs.engine.control.CompositeCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JcsCacheClusterStore extends JcsCacheStore implements
		IClusterStore {
	
	private Logger log = LoggerFactory.getLogger(JcsCacheClusterStore.class);
	
	private CompositeCache<Serializable, Serializable> cache;
	
	protected static final String CACHE_NAME = "CLUSTER";
	
	public JcsCacheClusterStore(String jcs_conf_path) throws Exception {
		
		super(jcs_conf_path);
		
		cache = ccm.getCache(CACHE_NAME);
	}

	@Override
	public void update(Member node) {

		ICacheElement<Serializable, Serializable> element = new CacheElement<Serializable, Serializable>(CACHE_NAME, node.getId(), node);
		
		try {
			
			cache.update(element);
			
		} catch (IOException e) {
			
			log.error("",e);
		}
	}

	@Override
	public Member get(Short id) {
		
		ICacheElement<Serializable, Serializable> element = cache.get(id);
		
		if(element != null) {
			
			return (Member) element.getVal();
		}
		
		return null;
	}

	@Override
	public void shutdown() {

		cache.dispose();
	}

	@Override
	public void iterator(IClusterStoreIteratorCallback callbak) {
		
		for(Iterator<Serializable> it = cache.getKeySet().iterator(); it.hasNext();) {
			
			ICacheElement<Serializable, Serializable> e = cache.get(it.next());
			
			if(e != null) {
				
				if(e.getVal() instanceof Member) {
				
					callbak.next((Member)e.getVal());
				}
			}
		}
	}
}
