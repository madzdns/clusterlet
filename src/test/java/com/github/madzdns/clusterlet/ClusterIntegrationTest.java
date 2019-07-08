package com.github.madzdns.clusterlet;

import com.github.madzdns.clusterlet.codec.IMessage;
import com.github.madzdns.clusterlet.config.SocketBindConfig;
import com.github.madzdns.clusterlet.config.SocketConfig;
import com.github.madzdns.clusterlet.config.SyncConfig;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.util.*;
import java.util.concurrent.CountDownLatch;

import static org.junit.jupiter.api.Assertions.*;

@Slf4j
public class ClusterIntegrationTest {
    final static String clusterFile1 = "cluster_file";
    final static String clusterFile2 = "cluster_file_2";
    final static String messageToSend = "Ohoy";
    final static String messageToSendKey = "OhoyKey";
    final static String messageToSend2 = "Ohoy2";
    final static String messageToSendKey2 = "OhoyKey2";
    SyncServer syncServer1 = null;
    SyncServer syncServer2 = null;

    @Getter
    @Setter
    @NoArgsConstructor
    @AllArgsConstructor
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
                key = in.readUTF();
                version = in.readLong();
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

    public interface SyncCallBackCallable {
        boolean callback(ISession session, IMessage message,
                         Set<Short> withNodes, ISyncProtocolOutput out);
    }

    @AllArgsConstructor
    private static class SyncCallback implements ISyncCallback {
        private SyncCallBackCallable syncCallBackCallable;

        @Override
        public boolean callBack(ISession session, IMessage message,
                                Set<Short> withNodes, ISyncProtocolOutput out) {
            if (syncCallBackCallable != null) {
                return syncCallBackCallable.callback(session, message, withNodes, out);
            } else {
                return false;
            }
        }

        @Override
        public void result(SyncFeature syncFeature) {

        }
    }

    @BeforeEach
    public void beforeEach() throws IOException {
        assertTrue(new File(clusterFile1).createNewFile());
        assertTrue(new File(clusterFile2).createNewFile());
    }

    @AfterEach
    public void afterEach() {
        assertTrue(new File(clusterFile1).delete());
        assertTrue(new File(clusterFile2).delete());
        if (syncServer1 != null) {
            syncServer1.stop();
        }
        if (syncServer2 != null) {
            syncServer2.stop();
        }
    }

    @Test
    public void clusterJoinTest() throws Exception {
        final short memberId1 = 1;
        SyncConfig config = new SyncConfig(clusterFile1,
                null, null,
                null, null,
                null, null);
        SyncContext context1 = new SyncContext(memberId1, config);
        CountDownLatch startUpLatch = new CountDownLatch(2);
        Thread t1 = new Thread(() -> {
            SyncHandler handler = context1.make()
                    .withCallBack(new SyncCallback(null))
                    .withEncoder(MyMessage.class);
            SocketBindConfig syncBinding = new SocketBindConfig();
            syncBinding.setSocketConfigs(Collections.singletonList(new SocketConfig("localhost:12346")));
            try {
                syncServer1 = new SyncServer(handler, syncBinding);
                syncServer1.start();
                startUpLatch.countDown();
            } catch (IOException e) {
                fail();
            }
        });
        t1.setDaemon(true);
        t1.start();

        final short memberId2 = 2;
        SyncConfig config2 = new SyncConfig(clusterFile2,
                null, null,
                null, null, null,
                null);
        SyncContext context2 = new SyncContext(memberId2, config2);
        Thread t2 = new Thread(() -> {
            SyncHandler handler = context2.make()
                    .withCallBack(new SyncCallback(null))
                    .withEncoder(MyMessage.class);
            SocketBindConfig syncBinding = new SocketBindConfig();
            syncBinding.setSocketConfigs(Collections.singletonList(new SocketConfig("localhost:12347")));
            try {
                syncServer2 = new SyncServer(handler, syncBinding);
                syncServer2.start();
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
        final boolean useSsl = false;
        final boolean authByKey = true;
        final String key = "";
        final long lastModified = new Date().getTime();
        final Set<Short> awareIds = null;//This new member is not aware of other nodes
        final byte state = Member.STATE_VLD;//To delete use Member.STATE_DEL
        Member member = new Member(memberId, syncAddresses, useSsl, authByKey, key, lastModified, awareIds, state);
        assertTrue(context1.syncCluster(member, SyncType.RING));
        ClusterSnapshot cs = context2.getSnapshot();
        assertNotNull(cs.getCluster());
        assertEquals(2, cs.getCluster().size(), "Now member 2 should have 2 alive members in its snapshot");
        assertNotNull(cs.getAliveCluster());
        assertEquals(2, cs.getAliveCluster().size());
    }

    @Test
    public void sendingMessage_shouldBeOkTest() throws Exception {
        final short memberId1 = 1;
        SyncConfig config = new SyncConfig(clusterFile1,
                null, null,
                null, null,
                null, null);
        SyncContext context1 = new SyncContext(memberId1, config);
        CountDownLatch startUpLatch = new CountDownLatch(2);
        Thread t1 = new Thread(() -> {
            //Create member 1
            SyncHandler handler = context1.make()
                    .withCallBack(new SyncCallback((session, message, withNodes, out) -> {
                        assertTrue(message instanceof MyMessage);
                        MyMessage myMessage = (MyMessage) message;
                        assertEquals(messageToSend, myMessage.getMsg(), "message Should be received");
                        assertEquals(messageToSendKey, myMessage.getKey(), "key Should be received");
                        assertTrue(myMessage.getVersion() > 0, "version should be > 0");
                        return true;
                    }))
                    .withEncoder(MyMessage.class);
            SocketBindConfig syncBinding = new SocketBindConfig();
            syncBinding.setSocketConfigs(Collections.singletonList(new SocketConfig("localhost:12346")));
            try {
                syncServer1 = new SyncServer(handler, syncBinding);
                syncServer1.start();
                startUpLatch.countDown();
            } catch (IOException e) {
                fail();
            }
        });
        t1.setDaemon(true);
        t1.start();
        t1.setName("member 1 thread");

        final short memberId2 = 2;
        SyncConfig config2 = new SyncConfig(clusterFile2,
                null, null,
                null, null, null,
                null);
        SyncContext context2 = new SyncContext(memberId2, config2);
        Thread t2 = new Thread(() -> {
            SyncHandler handler = context2.make()
                    .withCallBack(new SyncCallback(null))
                    .withEncoder(MyMessage.class);
            SocketBindConfig syncBinding = new SocketBindConfig();
            syncBinding.setSocketConfigs(Collections.singletonList(new SocketConfig("localhost:12347")));
            try {
                syncServer2 = new SyncServer(handler, syncBinding);
                syncServer2.start();
                startUpLatch.countDown();
            } catch (IOException e) {
                fail();
            }
        });
        t2.setDaemon(true);
        t2.start();
        startUpLatch.await();
        t1.setName("member 2 thread");
        //Now both are listening

        //From member 1 synchronize member 2
        final short memberId = memberId2;
        final Set<Member.ClusterAddress> syncAddresses = Collections.singleton(
                new Member.ClusterAddress("localhost", 12347));
        final boolean useSsl = false;
        final boolean authByKey = true;
        final String key = "";
        final long lastModified = new Date().getTime();
        final Set<Short> awareIds = null;//This new member is not aware of other nodes
        final byte state = Member.STATE_VLD;//To delete use Member.STATE_DEL
        Member member = new Member(memberId, syncAddresses, useSsl, authByKey, key, lastModified, awareIds, state);
        assertTrue(context1.syncCluster(member, SyncType.RING));
        ClusterSnapshot cs = context2.getSnapshot();
        assertNotNull(cs.getCluster());
        assertEquals(2, cs.getCluster().size(), "Now member 2 should have 2 alive members in its snapshot");
        assertNotNull(cs.getAliveCluster());
        assertEquals(2, cs.getAliveCluster().size());
        //Now sending message from member 2 to others with ring synchronization type
        MyMessage messageFromMember2 = new MyMessage(messageToSendKey, new Date().getTime(), messageToSend);
        SyncFeature feature = context2.make(SyncType.RING)
                .withoutCluster(memberId2)//Dont send to member 2 again
                .withCallBack(new SyncCallback((session, message, withNodes, out) -> {
                    assertTrue(message instanceof MyMessage);
                    MyMessage myMessage = (MyMessage) message;
                    assertEquals(messageToSend, myMessage.getMsg(), "message Should be received");
                    assertEquals(messageToSendKey, myMessage.getKey(), "key Should be received");
                    assertTrue(myMessage.getVersion() > 0, "version should be > 0");
                    return true;
                }))
                .withEncoder(MyMessage.class)
                .sync(messageFromMember2)
                .get();
        assertTrue(feature.size() > 0);
        assertTrue(feature.containsKey(messageToSendKey));
        assertTrue(feature.get(messageToSendKey).isSuccessful(), "because both callbaks return true the result should be ok");
    }

    @Test
    public void sendingMessage_shouldNotBeOkTest() throws Exception {
        final short memberId1 = 1;
        SyncConfig config = new SyncConfig(clusterFile1,
                null, null,
                null, null,
                null, null);
        SyncContext context1 = new SyncContext(memberId1, config);
        CountDownLatch startUpLatch = new CountDownLatch(2);
        Thread t1 = new Thread(() -> {
            //Create member 1
            SyncHandler handler = context1.make()
                    .withCallBack(new SyncCallback((session, message, withNodes, out) -> {
                        assertTrue(message instanceof MyMessage);
                        MyMessage myMessage = (MyMessage) message;
                        assertEquals(messageToSend, myMessage.getMsg(), "message Should be received");
                        assertEquals(messageToSendKey, myMessage.getKey(), "key Should be received");
                        assertTrue(myMessage.getVersion() > 0, "version should be > 0");
                        return false;
                    }))
                    .withEncoder(MyMessage.class);
            SocketBindConfig syncBinding = new SocketBindConfig();
            syncBinding.setSocketConfigs(Collections.singletonList(new SocketConfig("localhost:12346")));
            try {
                syncServer1 = new SyncServer(handler, syncBinding);
                syncServer1.start();
                startUpLatch.countDown();
            } catch (IOException e) {
                fail();
            }
        });
        t1.setDaemon(true);
        t1.start();
        t1.setName("member 1 thread");

        final short memberId2 = 2;
        SyncConfig config2 = new SyncConfig(clusterFile2,
                null, null,
                null, null, null,
                null);
        SyncContext context2 = new SyncContext(memberId2, config2);
        Thread t2 = new Thread(() -> {
            SyncHandler handler = context2.make()
                    .withCallBack(new SyncCallback(null))
                    .withEncoder(MyMessage.class);
            SocketBindConfig syncBinding = new SocketBindConfig();
            syncBinding.setSocketConfigs(Collections.singletonList(new SocketConfig("localhost:12347")));
            try {
                syncServer2 = new SyncServer(handler, syncBinding);
                syncServer2.start();
                startUpLatch.countDown();
            } catch (IOException e) {
                fail();
            }
        });
        t2.setDaemon(true);
        t2.start();
        startUpLatch.await();
        t1.setName("member 2 thread");
        //Now both are listening

        //From member 1 synchronize member 2
        final short memberId = memberId2;
        final Set<Member.ClusterAddress> syncAddresses = Collections.singleton(
                new Member.ClusterAddress("localhost", 12347));
        final boolean useSsl = false;
        final boolean authByKey = true;
        final String key = "";
        final long lastModified = new Date().getTime();
        final Set<Short> awareIds = null;//This new member is not aware of other nodes
        final byte state = Member.STATE_VLD;//To delete use Member.STATE_DEL
        Member member = new Member(memberId, syncAddresses, useSsl, authByKey, key, lastModified, awareIds, state);
        assertTrue(context1.syncCluster(member, SyncType.RING));
        ClusterSnapshot cs = context2.getSnapshot();
        assertNotNull(cs.getCluster());
        assertEquals(2, cs.getCluster().size(), "Now member 2 should have 2 alive members in its snapshot");
        assertNotNull(cs.getAliveCluster());
        assertEquals(2, cs.getAliveCluster().size());
        //Now sending message from member 2 to others with ring synchronization type
        MyMessage messageFromMember2 = new MyMessage(messageToSendKey, new Date().getTime(), messageToSend);
        SyncFeature feature = context2.make(SyncType.RING)
                .withoutCluster(memberId2)//Dont send to member 2 again
                .withCallBack(new SyncCallback((session, message, withNodes, out) -> {
                    assertTrue(message instanceof MyMessage);
                    MyMessage myMessage = (MyMessage) message;
                    assertEquals(messageToSend, myMessage.getMsg(), "message Should be received");
                    assertEquals(messageToSendKey, myMessage.getKey(), "key Should be received");
                    assertTrue(myMessage.getVersion() > 0, "version should be > 0");
                    return true;
                }))
                .withEncoder(MyMessage.class)
                .sync(messageFromMember2)
                .get();
        assertTrue(feature.size() > 0);
        assertTrue(feature.containsKey(messageToSendKey));
        assertFalse(feature.get(messageToSendKey).isSuccessful(), "Because member 1 calback returns false, message synchronization should not be ok");
    }

    @Test
    public void sendingTwoMessage_oneShouldBeOkOtherNotBeOkTest() throws Exception {
        final short memberId1 = 1;
        SyncConfig config = new SyncConfig(clusterFile1,
                null, null,
                null, null,
                null, null);
        SyncContext context1 = new SyncContext(memberId1, config);
        CountDownLatch startUpLatch = new CountDownLatch(2);
        Thread t1 = new Thread(() -> {
            //Create member 1
            SyncHandler handler = context1.make()
                    .withCallBack(new SyncCallback((session, message, withNodes, out) -> {
                        assertTrue(message instanceof MyMessage);
                        return message.getKey().equals(messageToSendKey2);
                    }))
                    .withEncoder(MyMessage.class);
            SocketBindConfig syncBinding = new SocketBindConfig();
            syncBinding.setSocketConfigs(Collections.singletonList(new SocketConfig("localhost:12346")));
            try {
                syncServer1 = new SyncServer(handler, syncBinding);
                syncServer1.start();
                startUpLatch.countDown();
            } catch (IOException e) {
                fail();
            }
        });
        t1.setDaemon(true);
        t1.start();
        t1.setName("member 1 thread");

        final short memberId2 = 2;
        SyncConfig config2 = new SyncConfig(clusterFile2,
                null, null,
                null, null, null,
                null);
        SyncContext context2 = new SyncContext(memberId2, config2);
        Thread t2 = new Thread(() -> {
            SyncHandler handler = context2.make()
                    .withCallBack(new SyncCallback(null))
                    .withEncoder(MyMessage.class);
            SocketBindConfig syncBinding = new SocketBindConfig();
            syncBinding.setSocketConfigs(Collections.singletonList(new SocketConfig("localhost:12347")));
            try {
                syncServer2 = new SyncServer(handler, syncBinding);
                syncServer2.start();
                startUpLatch.countDown();
            } catch (IOException e) {
                fail();
            }
        });
        t2.setDaemon(true);
        t2.start();
        startUpLatch.await();
        t1.setName("member 2 thread");
        //Now both are listening

        //From member 1 synchronize member 2
        final short memberId = memberId2;
        final Set<Member.ClusterAddress> syncAddresses = Collections.singleton(
                new Member.ClusterAddress("localhost", 12347));
        final boolean useSsl = false;
        final boolean authByKey = true;
        final String key = "";
        final long lastModified = new Date().getTime();
        final Set<Short> awareIds = null;//This new member is not aware of other nodes
        final byte state = Member.STATE_VLD;//To delete use Member.STATE_DEL
        Member member = new Member(memberId, syncAddresses, useSsl, authByKey, key, lastModified, awareIds, state);
        assertTrue(context1.syncCluster(member, SyncType.RING));
        ClusterSnapshot cs = context2.getSnapshot();
        assertNotNull(cs.getCluster());
        assertEquals(2, cs.getCluster().size(), "Now member 2 should have 2 alive members in its snapshot");
        assertNotNull(cs.getAliveCluster());
        assertEquals(2, cs.getAliveCluster().size());
        //Now sending message from member 2 to others with ring synchronization type
        MyMessage messageFromMember2 = new MyMessage(messageToSendKey, new Date().getTime(), messageToSend);
        MyMessage message2FromMember2 = new MyMessage(messageToSendKey2, new Date().getTime(), messageToSend2);
        SyncFeature feature = context2.make(SyncType.RING)
                .withoutCluster(memberId2)//Dont send to member 2 again
                .withCallBack(new SyncCallback((session, message, withNodes, out) -> {
                    assertTrue(message instanceof MyMessage);
                    MyMessage myMessage = (MyMessage) message;
                    assertEquals(messageToSend, myMessage.getMsg(), "message Should be received");
                    assertEquals(messageToSendKey, myMessage.getKey(), "key Should be received");
                    assertTrue(myMessage.getVersion() > 0, "version should be > 0");
                    return true;
                }))
                .withEncoder(MyMessage.class)
                .sync(Arrays.asList(messageFromMember2, message2FromMember2))
                .get();
        assertTrue(feature.size() > 0);
        assertTrue(feature.containsKey(messageToSendKey));
        assertFalse(feature.get(messageToSendKey).isSuccessful(), "Because member 1 calback returns false, message synchronization should not be ok");
        assertTrue(feature.get(messageToSendKey2).isSuccessful(), "Because member 1 calback returns true, message synchronization should be ok");
    }
}
