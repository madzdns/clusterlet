package com.github.madzdns.clusterlet.api.net.ssl.server;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.security.*;

public class JavaSSLContext {

	private static String KEYSTORE_PASSWORD = "ManyOf8y567ForKstT#$565";
	private static String TRUSTSTORE_PASSWORD = "Bara%aAgdaz#34poVaaBingaay";
	private static String KEYSTORE_PASSWORD_2ND = "BaWarri!23eQq0_-DELE33";
	
	@SuppressWarnings("unused")
	private static Logger log = LoggerFactory.getLogger(JavaSSLContext.class);
	
	public static SSLContext getSSLcontextFactory(String keyStorePath, String trustStorePath, 
			String keyStorePassword, String trustStorePassword, 
			String keyStorePassword2nd) throws Exception {
		KEYSTORE_PASSWORD = keyStorePassword;
		TRUSTSTORE_PASSWORD = trustStorePassword;
		KEYSTORE_PASSWORD_2ND = keyStorePassword2nd;
		File keyStoreFile = new File(keyStorePath);
		File trustStoreFile = new File(trustStorePath);
		if (keyStoreFile.exists() && trustStoreFile.exists()) {
			// First initialize the key and trust material
			KeyStore ksKeys = KeyStore.getInstance("JKS");
			ksKeys.load(new FileInputStream(keyStoreFile), KEYSTORE_PASSWORD.toCharArray());
			KeyStore ksTrust = KeyStore.getInstance("JKS");
			ksTrust.load(new FileInputStream(trustStoreFile), TRUSTSTORE_PASSWORD.toCharArray());
			// KeyManagers decide which key material to use
			KeyManagerFactory kmf = KeyManagerFactory.getInstance("SunX509");
			kmf.init(ksKeys, KEYSTORE_PASSWORD_2ND.toCharArray());
			// TrustManagers decide whether to allow connections
			TrustManagerFactory tmf = TrustManagerFactory.getInstance("SunX509");
			tmf.init(ksTrust);
			SSLContext sslContext = SSLContext.getInstance("TLS");
			sslContext.init(kmf.getKeyManagers(), tmf.getTrustManagers(), null);
			return sslContext;
		}
		return null;
	}
}
