package com.github.madzdns.clusterlet.api.net.ssl.server;

import java.io.File;
import java.security.KeyStore;

import org.apache.mina.filter.ssl.KeyStoreFactory;
import org.apache.mina.filter.ssl.SslContextFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MinaServerSslContext {
	
	private static String KEYSTORE_PASSWORD = "ManyOf8y567ForKstT#$565";
	private static String TRUSTSTORE_PASSWORD = "Bara%aAgdaz#34poVaaBingaay";
	private static String KEYSTORE_PASSWORD_2ND = "BaWarri!23eQq0_-DELE33";
	
	private Logger log = LoggerFactory.getLogger(getClass());

	public SslContextFactory getSslContextFactory(String keyStorePath, String trustStorePath, 
			String keyStorePassword, String trustStorePassword, 
			String keyStorePassword2nd) {
		
		SslContextFactory sslContextFact = null;
		try {
			
			String keystore = keyStorePath;
			
			String truststore = trustStorePath;
			
			KEYSTORE_PASSWORD = keyStorePassword;
			
			TRUSTSTORE_PASSWORD = trustStorePassword;
			
			KEYSTORE_PASSWORD_2ND = keyStorePassword2nd;
			
			File keyStoreFile = new File(keystore);
			File trustStoreFile = new File(truststore);
 
			if (keyStoreFile.exists() && trustStoreFile.exists()) {
				
				final KeyStoreFactory keyStoreFactory = new KeyStoreFactory();
				keyStoreFactory.setDataFile(keyStoreFile);
				keyStoreFactory.setPassword(KEYSTORE_PASSWORD);

				final KeyStoreFactory trustStoreFactory = new KeyStoreFactory();
				trustStoreFactory.setDataFile(trustStoreFile);
				trustStoreFactory.setPassword(TRUSTSTORE_PASSWORD);
				
				final SslContextFactory sslContextFactory = new SslContextFactory();
				final KeyStore keyStore = keyStoreFactory.newInstance();
				sslContextFactory.setKeyManagerFactoryKeyStore(keyStore);
				
				final KeyStore trustStore = keyStoreFactory.newInstance();
				sslContextFactory.setTrustManagerFactoryKeyStore(trustStore);
				sslContextFactory.setKeyManagerFactoryKeyStorePassword(KEYSTORE_PASSWORD_2ND);
				sslContextFact = sslContextFactory;
			}
			else {
				
				log.error("Keystore or Truststore file does not exist");
			}
		}
		catch (Exception ex) {
			
			log.error("",ex);
		}
		return sslContextFact;
	}
}
