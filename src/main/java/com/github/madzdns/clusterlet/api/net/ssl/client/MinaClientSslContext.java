package com.github.madzdns.clusterlet.api.net.ssl.client;

import java.io.IOException;
import java.security.GeneralSecurityException;

import javax.net.ssl.SSLContext;

public class MinaClientSslContext {

	public SSLContext getSslClientContextFactory() throws GeneralSecurityException {
		
		return MySslContextFactory.createBougusClientSslContext();
	}
	
	public SSLContext getSslClientContextFactory(String certFilePath) 
			throws GeneralSecurityException, IOException {
		
		return MySslContextFactory.createSafeClientSslContext(certFilePath);
	}
	
}
