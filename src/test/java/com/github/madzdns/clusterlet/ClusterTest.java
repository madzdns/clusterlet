package com.github.madzdns.clusterlet;

import com.github.madzdns.clusterlet.config.SyncConfig;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

import java.io.File;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@Slf4j
public class ClusterTest {

    @SuppressWarnings("ResultOfMethodCallIgnored")
    @Test
    public void testClusterStartup() throws Exception {
        final String sslKeyStorePath = null;
        final String sslTrustStorepath = null;
        final String KEYSTORE_PASSWORD = null;
        final String TRUSTSTORE_PASSWORD = null;
        final String KEYSTORE_PASSWORD_2ND = null;
        final String CERTIFICATE_PATH = null;
        final short NODE_ID = 1;
        final String clusterFile = "cluster_file";
        new File(clusterFile).createNewFile();
        SyncConfig config = new SyncConfig(clusterFile,
                sslKeyStorePath, sslTrustStorepath,
                KEYSTORE_PASSWORD, TRUSTSTORE_PASSWORD, KEYSTORE_PASSWORD_2ND,
                CERTIFICATE_PATH);
        SyncContext context = new SyncContext(NODE_ID, config);
        ClusterSnapshot cs = context.getSnapshot();
        assertNotNull(cs.getCluster());
        assertEquals(1, cs.getCluster().size());
        assertNotNull(cs.getAliveCluster());
        assertEquals(1, cs.getAliveCluster().size());
        new File(clusterFile).delete();
    }
}
