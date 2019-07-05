package com.github.madzdns.clusterlet.api.net;

import java.io.IOException;
import java.security.GeneralSecurityException;

import javax.net.ssl.SSLContext;

import org.apache.mina.core.session.IoSession;
import org.apache.mina.filter.ssl.SslContextFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.frfra.frsynch.api.net.ssl.client.MinaClientSslContext;
import com.frfra.frsynch.api.net.ssl.server.JavaSSLContext;
import com.frfra.frsynch.api.net.ssl.server.MinaServerSslContext;

public class NetProvider {

	private static Logger log = LoggerFactory.getLogger(NetProvider.class);
	
	private static SslContextFactory SSL_CTX = null;
	
	private static SSLContext TLS_CTX = null;
	
	public static SSLContext getServerSSLContext(String keyStorePath, String trustStorePath, 
			String keyStorePassword, String trustStorePassword, 
			String keyStorePassword2nd) {
		
		try {
			
			if(SSL_CTX!=null)
				
				return SSL_CTX.newInstance();
			
			return (SSL_CTX = new MinaServerSslContext().getSslContextFactory(keyStorePath,trustStorePath, 
					keyStorePassword, trustStorePassword, 
					keyStorePassword2nd)).newInstance();
			
		} catch (Exception e) {
			
			log.error("",e);
			
			SSL_CTX = null;
			return null;
		}
	}
	
	public static SSLContext getServerTLSContext(String keyStorePath, String trustStorePath, 
			String keyStorePassword, String trustStorePassword, 
			String keyStorePassword2nd) {
		
		try {
			
			if ( TLS_CTX != null )
				
				return TLS_CTX;
			
			return TLS_CTX = JavaSSLContext.getSSLcontextFactory(keyStorePath,trustStorePath, 
					keyStorePassword, trustStorePassword, 
					keyStorePassword2nd);
			
		} catch (Exception e) {
			
			log.error("",e);
			
			return TLS_CTX = null;
		}
	}
	
	public static SSLContext getClientSslContext() {
		
		try {
			
			return new MinaClientSslContext().getSslClientContextFactory();
			
		} catch (GeneralSecurityException e) {
			
			log.error("",e);
			return null;
		}
	}
	
	public static SSLContext getClientSslContext(String certFilePath) {
		
		try {
			
			return new MinaClientSslContext().getSslClientContextFactory(certFilePath);
			
		} catch (GeneralSecurityException e) {
			
			log.error("",e);
			return null;
			
		}catch (IOException e) {
			
			log.error("",e);
			return null;
		}
	}
	
	public static void closeMinaSession(IoSession session,boolean immidiate) {
		
		if (session != null) {
            /*if (session.isConnected()) {
                // Wait until the chat ends.
                session.getCloseFuture().awaitUninterruptibly();
            }*/
            session.close(immidiate);
        }
	}
}
