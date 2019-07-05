package com.github.madzdns.clusterlet;

import com.github.madzdns.clusterlet.codec.IMessage;
import com.github.madzdns.clusterlet.config.Bind;
import com.github.madzdns.clusterlet.config.Socket;
import com.github.madzdns.clusterlet.config.SynchConfig;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@Slf4j
public class ClusterTest {
    private static class MyMessage implements IMessage {

        private String key;
        private long version;
        private String msg;

        @Override
        public String getKey() {
            return key;
        }

        @Override
        public long getVersion() {
            return version;
        }

        @Override
        public void close() {

        }

        @Override
        public void configure(Map<String, ?> config) {

        }

        @Override
        public void deserialize(byte[] data) {
            try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(data))) {
                msg = in.readUTF();
            } catch (Exception e) {
                log.error("", e);
            }
        }

        @Override
        public byte[] serialize() {
            try (ByteArrayOutputStream stream = new ByteArrayOutputStream()) {
                DataOutputStream out = new DataOutputStream(stream);
                out.writeUTF(getKey());
                out.writeLong(getVersion());
                out.writeUTF(msg);
                return stream.toByteArray();
            } catch (Exception e) {
                log.error("{}, {}", getKey(), getVersion(), e);
                return null;
            }
        }
    }

    private ISynchCallbak callbak1 = new ISynchCallbak() {
        @Override
        public boolean callBack(ISession session, IMessage message, Set<Short> withNodes, ISynchProtocolOutput out) {
            return false;
        }

        @Override
        public void result(SynchFeature synchFeature) {

        }
    };

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
        SynchConfig config = new SynchConfig(clusterFile,
                sslKeyStorePath, sslTrustStorepath,
                KEYSTORE_PASSWORD, TRUSTSTORE_PASSWORD, KEYSTORE_PASSWORD_2ND,
                CERTIFICATE_PATH);
        SynchContext context = new SynchContext(NODE_ID, config);
        /*
        //We don't want to start server
        SynchHandler handler = context.make()
                .withCallBack(callbak1)
                .withEncoder(MyMessage.class);
        Bind backendBinding = new Bind();
        backendBinding.setSockets(Collections.singletonList(new Socket("localhost:12345")));
        Bind syncBinding = new Bind();
        syncBinding.setSockets(Collections.singletonList(new Socket("localhost:12346")));
        new SynchServer(handler,syncBinding,backendBinding).start();*/

        ClusterSnapshot cs = context.getSnapshot();
        assertNotNull(cs.getCluster());
        assertEquals(1, cs.getCluster().size());
        assertNotNull(cs.getAliveCluster());
        assertEquals(1, cs.getAliveCluster().size());
        new File(clusterFile).delete();
    }
}
