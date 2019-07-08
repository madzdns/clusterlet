package com.github.madzdns.clusterlet;

import com.github.madzdns.clusterlet.codec.IMessage;
import com.github.madzdns.clusterlet.config.Bind;
import com.github.madzdns.clusterlet.config.Socket;
import com.github.madzdns.clusterlet.config.SynchConfig;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.util.Collections;
import java.util.Date;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import static org.junit.jupiter.api.Assertions.*;

@Slf4j
public class ClusterIntegrationTest {
    @Getter
    private static class MyMessage implements IMessage {
        private String key;
        private long version;
        private String msg;

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

    private static class SyncCallback implements ISynchCallbak {

        @Override
        public boolean callBack(ISession session, IMessage message,
                                Set<Short> withNodes, ISynchProtocolOutput out) {
            return false;
        }

        @Override
        public void result(SynchFeature synchFeature) {

        }
    }

    @Test
    public void clusterJoinTest() throws Exception {
        final short memberId1 = 1;
        final String clusterFile1 = "cluster_file";
        assertTrue(new File(clusterFile1).createNewFile());
        SynchConfig config = new SynchConfig(clusterFile1,
                null, null,
                null, null, null,
                null);
        SynchContext context1 = new SynchContext(memberId1, config);
        CountDownLatch startUpLatch = new CountDownLatch(2);
        Thread t1 = new Thread(() -> {
            SynchHandler handler = context1.make()
                    .withCallBack(new SyncCallback())
                    .withEncoder(MyMessage.class);
            Bind syncBinding = new Bind();
            syncBinding.setSockets(Collections.singletonList(new Socket("localhost:12346")));
            try {
                new SynchServer(handler, syncBinding).start();
                startUpLatch.countDown();
            } catch (IOException e) {
                fail();
            }
        });
        t1.setDaemon(true);
        t1.start();

        final short memberId2 = 2;
        final String clusterFile2 = "cluster_file_2";
        assertTrue(new File(clusterFile2).createNewFile());
        SynchConfig config2 = new SynchConfig(clusterFile2,
                null, null,
                null, null, null,
                null);
        SynchContext context2 = new SynchContext(memberId2, config2);
        Thread t2 = new Thread(() -> {
            SynchHandler handler = context2.make()
                    .withCallBack(new SyncCallback())
                    .withEncoder(MyMessage.class);
            Bind syncBinding = new Bind();
            syncBinding.setSockets(Collections.singletonList(new Socket("localhost:12347")));
            try {
                new SynchServer(handler, syncBinding).start();
                startUpLatch.countDown();
            } catch (IOException e) {
                fail();
            }
        });
        t2.setDaemon(true);
        t2.start();
        startUpLatch.await();
        //Now both are listening

        //From member 1 synchronize member 2
        final short memberId = memberId2;
        final Set<Member.ClusterAddress> syncAddresses = Collections.singleton(
                new Member.ClusterAddress("localhost", 12347));
        final boolean useSsl = true;
        final boolean authByKey = true;
        final String key = "member key";
        final long lastModified = new Date().getTime();
        final Set<Short> awareIds = null;//This new member is not aware of other nodes
        final byte state = Member.STATE_VLD;//To delete use Member.STATE_DEL
        Member member = new Member(memberId, syncAddresses, useSsl, authByKey, key, lastModified, awareIds, state);
        assertTrue(context1.synchCluster(member, SynchType.RING));
        ClusterSnapshot cs = context2.getSnapshot();
        assertNotNull(cs.getCluster());
        assertEquals(2, cs.getCluster().size(), "Now member 2 should have 2 alive members in its snapshot");
        assertNotNull(cs.getAliveCluster());
        assertEquals(2, cs.getAliveCluster().size());
        assertTrue(new File(clusterFile1).delete());
        assertTrue(new File(clusterFile2).delete());
    }
}
