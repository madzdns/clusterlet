package com.github.madzdns.clusterlet.api.net.ssl.client;

import java.io.IOException;
import java.security.GeneralSecurityException;

import javax.net.ssl.SSLContext;

public class MySslContextFactory {
	
	private static final String PROTOCOL = "TLS";
	
	/**
	 * 
	 * @return
	 * @throws GeneralSecurityException
	 */
	public static SSLContext createBougusClientSslContext()
            throws GeneralSecurityException {
		
        SSLContext context = SSLContext.getInstance(PROTOCOL);
        
        context.init(null, BogusTrustManagerFactory.X509_MANAGERS, null);
        
        return context;
    }
	
	public static SSLContext createSafeClientSslContext(String certFilePath) 
			throws GeneralSecurityException, IOException{
		
		SSLContext context = SSLContext.getInstance(PROTOCOL);
        context.init(null, SafeTrustManagerFactory.getTrustManagers(certFilePath), null);
        return context;
	}

}
