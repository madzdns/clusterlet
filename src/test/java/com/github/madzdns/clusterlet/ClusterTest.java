package com.github.madzdns.clusterlet;

import com.github.madzdns.clusterlet.config.SynchConfig;
import org.junit.jupiter.api.Test;

import java.io.File;

public class ClusterTest {
    @Test
    public void testClusterStartup() {
        final String sslKeyStorePath = null;
        final String sslTrustStorepath = null;
        final String KEYSTORE_PASSWORD = null;
        final String TRUSTSTORE_PASSWORD = null;
        final String KEYSTORE_PASSWORD_2ND = null;
        final String CERTIFICATE_PATH = null;
        final short NODE_ID = 1;
        SynchConfig config = new SynchConfig("cluster_file",
                sslKeyStorePath, sslTrustStorepath,
                KEYSTORE_PASSWORD, TRUSTSTORE_PASSWORD, KEYSTORE_PASSWORD_2ND,
                CERTIFICATE_PATH);
        SynchContext context = new SynchContext(NODE_ID, config);
    }
}
