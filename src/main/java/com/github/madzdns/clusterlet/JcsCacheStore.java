package com.github.madzdns.clusterlet;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

import org.apache.commons.jcs.engine.control.CompositeCacheManager;

public abstract class JcsCacheStore {
	
	protected static CompositeCacheManager ccm = null;
	
	private final static Object mutx = new Object();  
	
	public JcsCacheStore(String jcs_conf_path) throws Exception {
		
		synchronized (mutx) {
		
			if(ccm == null) {
				
				InputStream fis = new FileInputStream(new File(jcs_conf_path).getAbsolutePath());
				
				try {
				
					ccm = CompositeCacheManager.getUnconfiguredInstance();
					Properties props = new Properties();
					props.load(fis);
					
					ccm.configure(props);
				}
				finally {
					
					fis.close();
				}
			}
		}
	}
}
