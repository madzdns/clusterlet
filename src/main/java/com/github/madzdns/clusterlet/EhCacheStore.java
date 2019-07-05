package com.github.madzdns.clusterlet;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;

import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheManager;

public abstract class EhCacheStore {

	private static final String CACHE_NAME = "CLUSTER";
	
	protected static CacheManager manager;
	protected static Cache cache;
	
	public EhCacheStore(String ehcache_conf_path) throws Exception {
		
		if(manager == null) {
			
			InputStream fis = new FileInputStream(new File(ehcache_conf_path).getAbsolutePath());
			
			try {

				manager = CacheManager.create(fis);
				manager.addCache(CACHE_NAME);
				cache = manager.getCache(CACHE_NAME);
			  
			} finally {
				
			  fis.close();
			}
		}
	}
}
