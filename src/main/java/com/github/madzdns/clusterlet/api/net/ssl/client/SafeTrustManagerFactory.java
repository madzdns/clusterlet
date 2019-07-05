package com.github.madzdns.clusterlet.api.net.ssl.client;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;

import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;

public class SafeTrustManagerFactory {

	public static TrustManager[] getTrustManagers(String certFilePath) 
			throws GeneralSecurityException, IOException {
		
		InputStream is = new FileInputStream(certFilePath);

		CertificateFactory cf = CertificateFactory.getInstance("X.509");
		X509Certificate caCert = (X509Certificate)cf.generateCertificate(is);

		TrustManagerFactory tmf = TrustManagerFactory
		    .getInstance(TrustManagerFactory.getDefaultAlgorithm());
		KeyStore ks = KeyStore.getInstance(KeyStore.getDefaultType());
		ks.load(null);
		ks.setCertificateEntry("caCert", caCert);

		tmf.init(ks);
		
		return tmf.getTrustManagers();
	}
}
