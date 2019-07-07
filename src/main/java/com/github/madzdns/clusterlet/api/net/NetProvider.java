package com.github.madzdns.clusterlet.api.net;

import com.github.madzdns.clusterlet.api.net.ssl.client.MinaClientSslContext;
import com.github.madzdns.clusterlet.api.net.ssl.server.JavaSSLContext;
import com.github.madzdns.clusterlet.api.net.ssl.server.MinaServerSslContext;
import lombok.extern.slf4j.Slf4j;
import org.apache.mina.core.session.IoSession;
import org.apache.mina.filter.ssl.SslContextFactory;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.security.GeneralSecurityException;

@Slf4j
public class NetProvider {
    private static SslContextFactory SSL_CTX = null;
    private static SSLContext TLS_CTX = null;
    public static SSLContext getServerSSLContext(String keyStorePath, String trustStorePath,
                                                 String keyStorePassword, String trustStorePassword,
                                                 String keyStorePassword2nd) {
        try {
            if (SSL_CTX != null) {
                return SSL_CTX.newInstance();
            }
            if (keyStorePath != null && trustStorePath != null) {
                return (SSL_CTX = new MinaServerSslContext().getSslContextFactory(keyStorePath, trustStorePath,
                        keyStorePassword, trustStorePassword,
                        keyStorePassword2nd)).newInstance();
            }
        } catch (Exception e) {
            log.error("", e);
        }
        SSL_CTX = null;
        return null;
    }

    public static SSLContext getServerTLSContext(String keyStorePath, String trustStorePath,
                                                 String keyStorePassword, String trustStorePassword,
                                                 String keyStorePassword2nd) {
        try {
            if (TLS_CTX != null) {
                return TLS_CTX;
            }
            if (keyStorePath != null && trustStorePath != null) {
                return TLS_CTX = JavaSSLContext.getSSLcontextFactory(keyStorePath, trustStorePath,
                        keyStorePassword, trustStorePassword,
                        keyStorePassword2nd);
            }
        } catch (Exception e) {
            log.error("", e);
        }
        return TLS_CTX = null;
    }

    public static SSLContext getClientSslContext() {
        try {
            return new MinaClientSslContext().getSslClientContextFactory();
        } catch (GeneralSecurityException e) {
            log.error("", e);
            return null;
        }
    }

    public static SSLContext getClientSslContext(String certFilePath) {
        try {
            return new MinaClientSslContext().getSslClientContextFactory(certFilePath);
        } catch (GeneralSecurityException | IOException e) {
            log.error("for certFilePath={}", certFilePath, e);
            return null;
        }
    }

    public static void closeMinaSession(IoSession session, boolean immidiate) {
        if (session != null) {
            session.close(immidiate);
        }
    }
}
