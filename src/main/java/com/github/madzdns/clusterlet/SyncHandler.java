package com.github.madzdns.clusterlet;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.Map.Entry;
import java.util.function.Consumer;

import javax.net.ssl.SSLContext;

import com.github.madzdns.clusterlet.codec.SyncContent;
import com.github.madzdns.clusterlet.codec.SyncMessage;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.mina.core.service.IoHandlerAdapter;
import org.apache.mina.core.session.IoSession;
import org.apache.mina.filter.codec.ProtocolCodecFactory;
import org.apache.mina.filter.codec.ProtocolCodecFilter;
import org.apache.mina.filter.codec.ProtocolDecoder;
import org.apache.mina.filter.codec.ProtocolEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.madzdns.clusterlet.api.net.NetProvider;
import com.github.madzdns.clusterlet.api.net.compress.filter.MinaCompressionFilter;
import com.github.madzdns.clusterlet.api.net.ssl.server.filter.MinaSslFilter;
import com.github.madzdns.clusterlet.codec.ClusterMessage;
import com.github.madzdns.clusterlet.codec.IMessage;
import com.github.madzdns.clusterlet.codec.SyncMessage.SyncMode;
import com.github.madzdns.clusterlet.codec.mina.SyncMinaDecoder;
import com.github.madzdns.clusterlet.codec.mina.SyncMinaEncoder;
import com.github.madzdns.clusterlet.helper.CollectionHelper;

public class SyncHandler extends IoHandlerAdapter {

    private final static String HIM_KEY = "_HimM_ckKey." + SyncHandler.class;
    private final static String SESSION_INITIATED_KEY = "_initkey." + SyncHandler.class;

    private Logger log = LoggerFactory.getLogger(SyncHandler.class);

    public final static byte STATE_WORK_DONE = 0;
    public final static byte STATE_WORK_FAILED = 1;
    public final static byte STATE_UNPROPER = 2;

    boolean isSender;
    private SyncType sync;
    private ISyncCallback callbak;
    private short[] ids = null;
    SyncMode mode = SyncMode.SYNC_MESSAGE;
    private List<SyncSession> sessions;
    private String link;
    private Set<Integer> unProperSockets = null;
    private int currentSocket = 0;
    private boolean nonAsync = false;
    private Object nonasyncLock = new Object();
    private short numberOfTrieds = 0;
    private boolean startupState;
    private SyncFeature syncFeature = null;
    private boolean isInclusive = true;
    private Map<String, SyncContent> syncContents = null;
    private boolean withBalance = false;
    private Set<Short> expectedNodes;
    SyncContext syncContext;
    Member me;
    private Class<? extends IMessage> encoder;

    private SyncHandler withModeCluster() {
        this.mode = SyncMode.SYNC_CLUSTER;
        return this;
    }

    private SyncHandler withBalance() {
        this.withBalance = true;
        return this;
    }

    SyncHandler(SyncContext ctx) {
        this.isSender = false;
        this.syncContext = ctx;
        this.me = ctx.getMyInfo();
    }

    SyncHandler(SyncContext ctx, SyncType sync) {
        this.isSender = true;
        this.sync = sync;
        this.syncContext = ctx;
        this.me = ctx.getMyInfo();
        unProperSockets = new HashSet<>();
    }

    /**
     * Sepcifies callback handler of type @see ISyncCallback.
     * In order to sync messages with cluster, you must implement
     *
     * @param callbak
     * @return this SyncHandler
     * @see ISyncCallback so using that, Clusterlet knows how to deal
     * with the received message
     */
    public SyncHandler withCallBack(ISyncCallback callbak) {
        this.callbak = callbak;
        return this;
    }

    /**
     * Specifies witch nodes you want to sync your messages. Also see @see SyncHandler#withoutCluster
     * If you leave this empty (or simply dont use it) mesage will be tried to sync with all nodes
     *
     * @param ids of nodes
     * @return this SyncHandler
     */
    public SyncHandler withCluster(short... ids) {
        this.isInclusive = true;
        this.ids = ids;
        return this;
    }

    /**
     * Specifies message encoder class
     *
     * @param encoder
     * @return this SyncHandler
     */
    public SyncHandler withEncoder(Class<? extends IMessage> encoder) {
        this.encoder = encoder;
        return this;
    }

    /**
     * Specifies witch nodes you don't want to sync your messages. Also see @seeSynchHandler#withCluster
     *
     * @param ids
     * @return this SyncHandler
     */
    public SyncHandler withoutCluster(short... ids) {
        isInclusive = false;
        this.ids = ids;
        return this;
    }

    /**
     * Starts to sync a single message with the cluster. This method should be called
     * after you specified callback and encoder and destinations. Also @see SyncHandler#sync(List<IMessage> msg)
     *
     * @param msg
     * @return
     */
    public SyncHandler sync(IMessage msg) {
        return sync(Collections.singletonList(msg));
    }

    private boolean checkWithBalanceAndSetCallback() {
        if (this.withBalance) {
            this.callbak.result(null);
            return true;
        }
        return false;
    }

    private void doInverseIds(List<Member> cluster, Set<Short> idss) {
        Set<Short> idSet = new HashSet<>();
        for (short id : ids) {
            idSet.add(id);
        }
        cluster.stream().filter(member -> !idSet.contains(member.getId())).forEach(
                member -> {
                    idss.add(member.getId());
                    sessions.add(member.createSynchSession(SyncHandler.this));
                }
        );
    }

    private void doInverseIds(List<Member> cluster, Set<Short> idss, Consumer<Member> consumer) {
        Set<Short> idSet = new HashSet<>();
        for (short id : ids) {
            idSet.add(id);
        }
        cluster.stream().filter(member -> !idSet.contains(member.getId())).forEach(
                member -> {
                    idss.add(member.getId());
                    consumer.accept(member);
                }
        );
    }

    /**
     * Starts to sync batch of messages with cluster. This method should be called
     * after you specified callback and encoder and destinations.
     *
     * @param msg
     * @return
     */
    public SyncHandler sync(List<IMessage> msg) {
        this.isSender = true;
        if (msg == null || msg.size() == 0 ||
                this.callbak == null || (mode == SyncMode.SYNC_MESSAGE && this.encoder == null)) {
            log.warn("messages {}, callback {} encoder {}", msg, callbak, encoder);
            nonasyncLock = null;
            checkWithBalanceAndSetCallback();
            return this;
        }

        ClusterSnapshot snapshot = syncContext.getSnapshot();
        if (snapshot == null) {
            nonasyncLock = null;
            checkWithBalanceAndSetCallback();
            return this;
        }

        if (SyncType.checkIfBalanceType(this.sync)) {
            if (!this.withBalance) {
                syncWithBalance(msg);
                return this;
            }
        }
        List<Member> cluster = snapshot.getAliveCluster();
        sessions = new ArrayList<>();
        if (SyncType.checkIfUnicastType(this.sync)) {
            if (ids == null || ids.length == 0) {
                if (checkWithBalanceAndSetCallback()) {
                    return this;
                }
                if (cluster.size() > 0) {
                    ids = new short[cluster.size()];
                    int i = 0;
                    for (Member e : cluster) {
                        ids[i++] = e.getId();
                        sessions.add(e.createSynchSession(this));
                    }
                }
            } else {
                if (isInclusive) {
                    for (short id : ids) {
                        Member e = snapshot.getById(id, ClusterSnapshot.MEMBER_CHECK_VALID);
                        if (e != null) {
                            sessions.add(e.createSynchSession(this));
                        }
                    }
                } else {
                    Set<Short> idss = new HashSet<>();
                    doInverseIds(cluster, idss);
                    this.ids = ArrayUtils.toPrimitive(idss.toArray(new Short[0]));
                }
            }
        } else if (SyncType.checkIfRingType(this.sync)) {
            if (ids == null || ids.length == 0) {
                if (checkWithBalanceAndSetCallback()) {
                    return this;
                }

                if (cluster.size() > 0) {
                    ids = new short[cluster.size()];
                    this.expectedNodes = new HashSet<>();
                    int i = 0;
                    for (Member e : cluster) {
                        ids[i++] = e.getId();
                        sessions.add(e.createSynchSession(this));
                        this.expectedNodes.add(e.getId());
                    }
                }
            } else {
                if (isInclusive) {
                    this.expectedNodes = new HashSet<>();
                    for (short id : ids) {
                        Member member = snapshot.getById(id, ClusterSnapshot.MEMBER_CHECK_VALID);
                        if (member != null) {
                            sessions.add(member.createSynchSession(this));
                            this.expectedNodes.add(id);
                        }
                    }
                } else {
                    expectedNodes = new HashSet<>();
                    doInverseIds(cluster, expectedNodes);
                    this.ids = ArrayUtils.toPrimitive(expectedNodes.toArray(new Short[0]));
                }
            }
        }

        startupState = syncContext.isInStartup();
        SyncMessage message = new SyncMessage();
        message.setSyncType(sync);
        message.setId(me.getId());
        message.setInStartup(startupState);
        message.setSyncMode(mode);
        message.setType(SyncMessage.TYPE_CHECK);

        syncContents = new HashMap<>();
        this.syncFeature = new SyncFeature();
        for (IMessage m : msg) {
            Set<Short> awareIds = null;
            if (this.mode == SyncMode.SYNC_MESSAGE) {
                if (m.getKey() != null && !m.getKey().equals("") && m.getVersion() > 0) {
                    awareIds = addAndGetAwareNodesOfMessage(m);
                }
            } else if (m.getKey() != null && !m.getKey().equals("") && m.getVersion() > 0) {
                ClusterMessage cm = (ClusterMessage) m;
                Member node = syncContext.getMemberById(cm.getId());
                awareIds = node == null ? null : node.getAwareIds();
            }
            log.debug("aware members for {} = {}", m.getKey(), awareIds);
            syncContents.put(m.getKey(), new SyncContent(m.getKey(), m.getVersion(), awareIds, m.serialize()));
            SyncResult s = new SyncResult();
            s.addSyncedMember(awareIds);
            this.syncFeature.put(m.getKey(), s);
        }
        message.setContents(syncContents.values());
        if (log.isDebugEnabled()) {
            log.debug("created syncContents ==> {} for sync type {}", syncContents.values(), sync);
            log.debug("created sessions ==> {}", sessions);
        }

        if (sessions != null && sessions.size() > 0) {
            if (SyncType.checkIfRingType(sync) ||
                    sync == SyncType.UNICAST_ONE_OF) {
                sessions.get(0).sendMsg(message);
            } else if (SyncType.checkIfUnicastType(sync)) {
                for (SyncSession s : sessions) {
                    s.sendMsg(message);
                }
            }
        } else {
            nonasyncLock = null;
            createResult();
        }
        return this;
    }

    private Set<Short> addAndGetAwareNodesOfMessage(IMessage m) {
        Set<Short> awareIds;
        awareIds = syncContext.getAwareNodes(m.getKey(), m.getVersion());
        if (awareIds == null || awareIds.size() == 0) {
            awareIds = new HashSet<>();
            awareIds.add(syncContext.myId);
            syncContext.addAwareNodes(m.getKey(), m.getVersion(), awareIds);
        }
        return awareIds;
    }

    /**
     * This method blocks execuion util the result is generated
     *
     * @return SyncFeature result
     */
    public SyncFeature get() {
        this.nonAsync = true;
        if (nonasyncLock == null) {
            return this.syncFeature;
        }
        synchronized (nonasyncLock) {
            while (nonAsync)
                try {
                    nonasyncLock.wait();
                } catch (InterruptedException e) {
                    log.error("", e);
                }
        }
        return this.syncFeature;
    }

    @Override
    public void sessionCreated(IoSession session) {
        if (!isSender) {
            SSLContext ssl = NetProvider.getServerTLSContext(syncContext.getConfig().getKeyStorePath(),
                    syncContext.getConfig().getTrustStorePath(), syncContext.getConfig().getKeyStorePassword(),
                    syncContext.getConfig().getTrustStorePassword(), syncContext.getConfig().getKeyStorePassword2nd());
            if (ssl != null) {
                MinaSslFilter sslFilter = new MinaSslFilter(ssl, false);
                session.getFilterChain().addLast(MinaSslFilter.NAME, sslFilter);
            }
            session.getFilterChain().addLast("compress_filter",
                    new MinaCompressionFilter());
            session.getFilterChain().addLast("peer_coder",
                    new ProtocolCodecFilter(new ProtocolCodecFactory() {
                        private ProtocolDecoder decoder = new SyncMinaDecoder();
                        private ProtocolEncoder encoder = new SyncMinaEncoder();

                        @Override
                        public ProtocolEncoder getEncoder(IoSession arg0) throws Exception {
                            return this.encoder;
                        }

                        @Override
                        public ProtocolDecoder getDecoder(IoSession arg0) throws Exception {
                            return this.decoder;
                        }
                    }));
        }

        InetSocketAddress peer1 = ((InetSocketAddress) session.getRemoteAddress());
        link = peer1.getAddress().getHostAddress() + ":" + peer1.getPort();
        session.setAttribute("startupstate", syncContext.isInStartup());
    }

    private SyncMessage createSimpleResponse(byte messageType,
                                             Boolean startupStateFromSession,
                                             SyncMode syncMode) {
        SyncMessage response = new SyncMessage();
        response.setId(me.getId());
        if (startupStateFromSession != null) {
            response.setInStartup(startupStateFromSession);
        }
        response.setSyncMode(syncMode);
        response.setType(messageType);
        return response;
    }

    private SyncMessage createCompleteResponse(byte messageType,
                                               Boolean startupStateFromSession,
                                               SyncMode syncMode,
                                               SyncType syncType,
                                               Byte sequence) {
        SyncMessage response = createSimpleResponse(messageType, startupStateFromSession, syncMode);
        response.setSyncType(syncType);
        if (sequence > -1) {
            response.setSequence(sequence);
        }
        return response;
    }

    private void fillSyncContents(ClusterMessage response, SyncContent sc, Collection<SyncContent> responseContents) {
        if (response != null) {
            Member node = syncContext.getMemberById(response.getId());
            Set<Short> awareIds = null;
            if (node != null) {
                awareIds = node.getAwareIds();
            }
            responseContents.add(new SyncContent(sc.getKey(), response.getVersion(), awareIds,
                    response.serialize()));
        }
    }

    private boolean decodeGetCallbackResult(ISyncCallback callbak, IoSession session, IMessage decoded, byte[] data, Set<Short> awareIds, SyncProtocolOutput out) {
        if (data != null) {
            decoded.deserialize(data);
        }
        return callbak.callBack(new MinaToISession(session), decoded, awareIds, out);
    }

    private Set<Short> getAliveMemberIds() {
        ClusterSnapshot snapshot = syncContext.getSnapshot();
        if (snapshot != null) {
            List<Member> cluster = snapshot.getAliveCluster();
            if (cluster.size() > 0) {
                Set<Short> nodesForRingUpdate = new HashSet<>();
                for (Member member : cluster) {
                    nodesForRingUpdate.add(member.getId());
                }
                return nodesForRingUpdate;
            }
        }
        return null;
    }

    private SyncType getProperRingType(SyncMessage msg) {
        SyncType type = SyncType.RING_BALANCE;
        if (msg.getSyncType() == SyncType.RING_QUORUM ||
                msg.getSyncType() == SyncType.RING_BALANCE_QUORUM) {
            type = SyncType.RING_BALANCE_QUORUM;
        }
        return type;
    }

    private boolean fillCallbackResult(boolean result, IMessage message, Collection<SyncContent> responseContents,
                                       List<IMessage> responses, Map<String, String> ringMsgToScMap,
                                       Set<Short> awareIds, SyncResult syncResult) {
        if (result) {
            if (responses == null) {
                //means it synced successfully
                responseContents.add(new SyncContent(ringMsgToScMap.get(message.getKey()),
                        message.getVersion(),
                        awareIds, null));
                return false;
            }
        } else {
            if (responses == null) {
                Set<Short> failedMembers = syncResult.getFailedMembers();
                failedMembers.add(me.getId());
                responseContents.add(new SyncContent(ringMsgToScMap.get(message.getKey()),
                        0,
                        failedMembers, null));
                return false;
            }
        }
        return true;
    }

    private void handleMessageSyncListener(IoSession session, SyncMessage msg, Member him,
                                           Boolean startupStateFromSession,
                                           Boolean isFirstMessage) throws IllegalAccessException, InstantiationException {
        /*
         * Here, we check if a communicating edge is valid in our
         * database. If not, don't sync our ZONEs with him.
         * TODO why did not I check if him is null here? I think
         * I should not keep going if could not find him in zone
         * synchronizing. So I'm adding that part below. Feel free
         * to remove this in future if its not necessary
         */
        /*
         * TODO
         * If anybody send me a zone sync while I was not
         * valid anymore, should I communicate with him?
         */
        boolean notValidMember = false;
        short id = 0;
        if (him == null || !him.isValid()) {
            notValidMember = true;
            if (him != null) {
                id = him.getId();
            }
        }

        if (notValidMember) {
            log.error("Communicating edge {} is not valid in my database", id);
            SyncMessage response = createSimpleResponse(SyncMessage.TYPE_NOT_VALID_EDGE, startupStateFromSession, mode);
            session.write(response);
            session.close(false);
            return;
        }
        //TODO If it is necessary to check cluster here
        Collection<SyncContent> contents = msg.getContents();
        if (log.isDebugEnabled()) {
            log.debug("Received contents {}", contents);
        }
        if (contents == null) {
            log.warn("contents was null fro sync messages from {}", msg.getId());
            session.close(false);
            return;
        }
        Set<SyncContent> responseContents = new HashSet<>();
        List<IMessage> messagesForRing = new ArrayList<>();
        boolean isRing = SyncType.checkIfRingType(msg.getSyncType());

        Map<String, String> ringMsgToScMap = null;
        if (isRing) {
            ringMsgToScMap = new HashMap<>();
        }

        for (SyncContent sc : contents) {
            byte[] m = sc.getContent();
            if (m == null) {
                //TODO should not check version and see if its OK?
                //It sounds no because I don't send null
                log.warn("message was null for syncing messages from {}", msg.getId());
                continue;
            }

            IMessage decoded = this.encoder.newInstance();
            SyncProtocolOutput out = new SyncProtocolOutput();
            boolean result = decodeGetCallbackResult(callbak, session, decoded, m, sc.getAwareIds(), out);
            List<IMessage> responses = out.getMessages();
            if (result) {
                Set<Short> awareNodes = sc.getAwareIds();
                log.debug("Check if it is ok to store awareIds for message version {}, {}, {}", decoded.getVersion() > -1,
                        decoded.getKey() != null,
                        !"".equals(decoded.getKey()));
                if (decoded.getVersion() > -1 && decoded.getKey() != null &&
                        !"".equals(decoded.getKey())) {
                    if (awareNodes == null) {
                        awareNodes = new HashSet<>();
                    }
                    awareNodes.add(me.getId());
                    syncContext.addAwareNodes(decoded.getKey(), decoded.getVersion(), awareNodes);
                }
                if (isRing) {
                    messagesForRing.add(decoded);
                    ringMsgToScMap.put(decoded.getKey(), sc.getKey());
                    log.debug("message for ring {} added", decoded.getKey());
                } else {
                    if (responses == null) {
                        /* I decided to not send this sort of situation
                         * responseContents.add(new SyncContent(sc.getKey(),
                                sc.getVersion(),
                                awareNodes, null));*/
                        continue;
                    }
                    for (IMessage response : responses) {
                        if (response != null) {
                            responseContents.add(new SyncContent(sc.getKey(), response.getVersion(), awareNodes, response.serialize()));
                        }
                    }
                }
            } else {
                if (responses == null) {
                    HashSet<Short> iFailed = new HashSet<>();
                    iFailed.add(me.getId());
                    log.debug("it was not synced successfully due to null response and false result from callback");
                    //means it was not synced successfully
                    responseContents.add(new SyncContent(sc.getKey(), 0,
                            iFailed, null));
                    continue;
                }
                for (IMessage response : responses) {
                    if (response != null) {
                        responseContents.add(new SyncContent(sc.getKey(), response.getVersion(),
                                syncContext.getAwareNodes(response.getKey(), response.getVersion()), response.serialize()));
                    }
                }
            }
        }

        Set<Short> membersForRingUpdate = null;
        if (messagesForRing.size() > 0) {
            ISyncCallback callBack = this.callbak;
            SyncType type = getProperRingType(msg);
            membersForRingUpdate = getAliveMemberIds();

            SyncFeature sf = new SyncHandler(syncContext, type)
                    .withCallBack(callBack)
                    .withEncoder(encoder)
                    .withoutCluster(msg.getId(), syncContext.myId)
                    .sync(messagesForRing)
                    .get();

            if (sf == null) {
                //TODO is this right?
                SyncMessage m = createSimpleResponse(SyncMessage.TYPE_FAILD_RING,
                        null, SyncMode.SYNC_MESSAGE);
                log.warn("Synch failed due to null SF");
                session.write(m);
                session.close(false);
                return;
            }
            for (IMessage m : messagesForRing) {
                SyncResult s = sf.get(m.getKey());
                if (s.isSuccessful()) {
                    SyncProtocolOutput out = new SyncProtocolOutput();
                    Set<Short> awareIds = syncContext.getAwareNodes(m.getKey(), m.getVersion());
                    boolean result = decodeGetCallbackResult(callbak, session, m, null, awareIds, out);
                    List<IMessage> responses = out.getMessages();
                    if (!fillCallbackResult(result, m, responseContents, responses, ringMsgToScMap, awareIds, s)) {
                        continue;
                    }

                    for (IMessage response : responses) {
                        if (response != null) {
                            responseContents.add(new SyncContent(ringMsgToScMap.get(m.getKey()), response.getVersion(), awareIds,
                                    response.serialize()));
                        }
                    }
                } else {
                    log.debug("{} was not successfully syched with ring with others. Responcing with faild", m.getKey());
                    //means it was not synced successfully
                    responseContents.add(new SyncContent(ringMsgToScMap.get(m.getKey()), 0,
                            s.getFailedMembers(), null));
                }
            }

        }

        /* Here in message sync, we don't send originators key chain back either.
         * This way, an edge will accept zone synchronizations form an unknown Edge.
         * But because originator brought a valid key, I think this is OK
         * response.setKeyChain(him.getKeyChain());*/

        if (log.isDebugEnabled()) {
            log.debug("Contents {} is being replied to {}", responseContents, msg.getId());
        }

        if (responseContents.size() == 0) {
            SyncMessage m = createCompleteResponse(SyncMessage.TYPE_OK, null, SyncMode.SYNC_MESSAGE, msg.getSyncType(), (byte) 0);
            session.write(m);
            return;
        }

        SyncMessage m = createCompleteResponse(SyncMessage.TYPE_CHECK, null, SyncMode.SYNC_MESSAGE, msg.getSyncType(), (byte) (msg.getSequence() + 1));
        m.setContents(responseContents);
        if (isRing && isFirstMessage) {
            m.setExpectedIds(membersForRingUpdate);
        }
        session.write(m);
    }

    private void handleClusterSyncListener(IoSession session, SyncMessage msg, Boolean isFirstMessage) {
        /*
         * First of all we remove startup flag
         */
        //TODO I commented this in 14 APR 16
        //syncContext.inStartup = false;
        Collection<SyncContent> contents = msg.getContents();
        if (contents == null /*|| contents.size == 0*/) {
            log.warn("Received contents was null. closing session");
            session.close(false);
            return;
        }
        boolean isRing = SyncType.checkIfRingType(msg.getSyncType());
        Map<String, String> ringMsgToScMap = null;
        if (isRing) {
            ringMsgToScMap = new HashMap<>();
        }

        ClusterSyncCallback clusterCallback = new ClusterSyncCallback(syncContext);
        Collection<SyncContent> responseContents = new ArrayList<>();
        List<IMessage> messagesForRing = new ArrayList<>();
        for (SyncContent sc : contents) {
            byte[] m = sc.getContent();
            if (m == null) {
                if (sc.getVersion() > 0) {
                    try {
                        Member member = syncContext.getMemberById(Short.parseShort(sc.getKey()));
                        if (member != null) {
                            member.addAwareId(me.getId());
                            member.addAwareId(msg.getId());
                        } else {
                            log.error("Wired state!!!!");
                        }
                    } catch (Exception e) {
                        log.error("Error in parsing {}", sc.getKey(), e);
                    }
                    continue;
                }
                log.error("FAILD state!!!!");
                continue;
            }

            IMessage decoded = new ClusterMessage();
            SyncProtocolOutput out = new SyncProtocolOutput();
            boolean result = decodeGetCallbackResult(clusterCallback, session, decoded, m, sc.getAwareIds(), out);
            List<IMessage> responses = out.getMessages();
            if (result) {
                if (isRing) {
                    messagesForRing.add(decoded);
                    ringMsgToScMap.put(decoded.getKey(), sc.getKey());
                }
            }

            if (responses != null) {
                for (IMessage iMessage : responses) {
                    ClusterMessage response = (ClusterMessage) iMessage;
                    fillSyncContents(response, sc, responseContents);
                }
            }
        }

        Set<Short> nodesForRingUpdate = null;
        if (messagesForRing.size() > 0) {
            if (log.isDebugEnabled()) {
                log.debug("Starting to update messages {} for ring", messagesForRing);
            }
            ISyncCallback callBack = this.callbak;
            SyncType type = getProperRingType(msg);
            nodesForRingUpdate = getAliveMemberIds();
            SyncFeature sf = new SyncHandler(syncContext, type)
                    .withCallBack(callBack)
                    .withEncoder(encoder)
                    .withoutCluster(msg.getId(), syncContext.myId)
                    .withModeCluster()
                    .sync(messagesForRing)
                    .get();
            if (sf == null) {
                //TODO is this right?
                SyncMessage m = createCompleteResponse(SyncMessage.TYPE_FAILD_RING, null, SyncMode.SYNC_CLUSTER, msg.getSyncType(), (byte) -1);
                log.warn("sync failed due to null SF");
                session.write(m);
                session.close(false);
                return;
            }

            for (IMessage m : messagesForRing) {
                SyncResult s = sf.get(m.getKey());
						/*if(s == null) {
							//In the case of balance, there might be some messages gets omitted
							continue;
						}*/
                if (s.isSuccessful()) {
                    SyncProtocolOutput out = new SyncProtocolOutput();
                    Member member = syncContext.getMemberById(((ClusterMessage) m).getId());
                    Set<Short> awareIds = null;
                    if (member != null) {
                        awareIds = member.getAwareIds();
                    }
                    boolean result = decodeGetCallbackResult(clusterCallback, session, m, null, awareIds, out);
                    List<IMessage> responses = out.getMessages();
                    fillCallbackResult(result, m, responseContents, responses, ringMsgToScMap, awareIds, s);
                    for (IMessage response : responses) {
                        if (response != null) {
                            member = syncContext.getMemberById(((ClusterMessage) response).getId());
                            awareIds = null;
                            if (member != null) {
                                awareIds = member.getAwareIds();
                            }
                            responseContents.add(new SyncContent(ringMsgToScMap.get(m.getKey()), response.getVersion(), awareIds,
                                    response.serialize()));
                        }
                    }
                } else {
                    //means it was not synced successfully
                    responseContents.add(new SyncContent(ringMsgToScMap.get(m.getKey()), 0,
                            s.getFailedMembers(), null));
                }
            }

        }

        if (log.isDebugEnabled()) {
            log.debug("Response with contents {} to {}", responseContents, msg.getId());
        }

        if (responseContents.size() == 0) {
            SyncMessage m = createCompleteResponse(SyncMessage.TYPE_OK, null, SyncMode.SYNC_CLUSTER, msg.getSyncType(), (byte) 0);
            session.write(m);
            return;
        }

        SyncMessage m = createCompleteResponse(SyncMessage.TYPE_CHECK, null, SyncMode.SYNC_CLUSTER, msg.getSyncType(), (byte) (msg.getSequence() + 1));
        m.setContents(responseContents);
        if (isRing && isFirstMessage) {
            m.setExpectedIds(nodesForRingUpdate);
        }
        session.write(m);
        if (log.isDebugEnabled()) {
            log.debug("Message responded with contents {}, type {}, sequence {}", responseContents, m.getType(), m.getSequence());
        }
    }

    private void handleListener(IoSession session, SyncMessage msg, String peer) throws IllegalAccessException, InstantiationException {
        Boolean startupStateFromSession = (Boolean) session.getAttribute("startupstate");
        if (msg.getType() == SyncMessage.TYPE_OK) {
            log.debug("Returned type OK");
            session.close(false);
            return;
        } else if (msg.isInStartup() && startupStateFromSession) {
            log.error("Peer {} and I both were in statup", peer);
            session.write(createSimpleResponse(SyncMessage.TYPE_BOTH_STARTUP, startupStateFromSession, mode));
            session.close(false);
            return;
        } else if (msg.getType() == SyncMessage.TYPE_BAD_ID) {
            log.error("Got TYPE_BAD_ID sync response from {}. My Id is {}", peer, me.getId());
            session.close(true);
            return;
        } else if (msg.getType() == SyncMessage.TYPE_BAD_SEQ) {
            log.error("{} and I could not get aggried on syncing out zones", peer);
            session.close(true);
            return;
        } else if (msg.getSequence() > SyncMessage.SEQ_MAX) {
            log.error("Too many conversion between peer {}", peer);
            session.write(createSimpleResponse(SyncMessage.TYPE_BAD_SEQ, startupStateFromSession, mode));
            session.close(true);
            return;
        } else if (msg.getType() == SyncMessage.TYPE_FAILD_RING) {
            log.error("Got TYPE_FAILD_RING sync response from {} ", peer);
            session.close(true);
            return;
        }

        boolean isFirstMessage = false;
        if (!session.containsAttribute(SESSION_INITIATED_KEY)) {
            session.setAttribute(SESSION_INITIATED_KEY, true);
            isFirstMessage = true;
            if (me.getKey() != null) {
                if (!CollectionHelper
                        .containsAny(me.getKeyChain()
                                , msg.getKeyChain())) {
                    log.error("Got wrong sync message from {} with wrong keychain:{}", peer, msg.getKeyChain());
                    log.error("my keychain:{}", me.getKeyChain());
                    SyncMessage response = createSimpleResponse(SyncMessage.TYPE_BAD_KEY, startupStateFromSession, mode);
                    session.write(response);
                    session.close(true);
                    return;
                }
            }
        }

        Member him = (Member) session.getAttribute(HIM_KEY);
        if (him == null) {
            him = syncContext.getMemberById(msg.getId());
            log.debug("Received node was {} for id {} in my database", him, msg.getId());
            session.setAttribute(HIM_KEY, him);
        }

        if (him != null && him.isDown()) {
            /*
             * Since this could send us a message, so its no longer down.
             * So we change its state to DEL
             * TODO why DEL????????
             */
            syncContext.synchronizedStateChange(him, Member.STATE_DEL);
        }

        if (msg.getSyncMode() == SyncMode.SYNC_MESSAGE) {
            handleMessageSyncListener(session, msg, him, startupStateFromSession, isFirstMessage);
        } else if (msg.getSyncMode() == SyncMode.SYNC_CLUSTER) {
            handleClusterSyncListener(session, msg, isFirstMessage);
        }
    }

    private void handleSender(IoSession session, SyncMessage msg, String peer) throws IllegalAccessException, InstantiationException {
        SyncSession sync = (SyncSession) session.getAttribute("SyncSession");
        boolean isRing = SyncType.checkIfRingType(this.sync);
        if (isRing) {
            if (log.isDebugEnabled()) {
                log.debug("Remote node knew nodes {}", msg.getExpectedIds());
            }
            if (msg.getExpectedIds() != null) {
                this.expectedNodes.removeAll(msg.getExpectedIds());
            } else {
                this.expectedNodes.remove(msg.getId());
            }
        }

        if (msg.getType() == SyncMessage.TYPE_OK) {
            numberOfTrieds++;
            createResult();
            session.setAttribute("planned_close");
            session.close(false);
            return;
        } else if (msg.getType() == SyncMessage.TYPE_BOTH_STARTUP) {
            log.warn("Got TYPE_BOTH_STARTUP sync responce from {} ", peer);
            workCallback(sync, STATE_UNPROPER, link);
            return;
        } else if (msg.getType() == SyncMessage.TYPE_NOT_VALID_EDGE) {
            log.warn("Got TYPE_NOT_VALID_EDGE sync responce from {} ", peer);
            workCallback(sync, STATE_UNPROPER, link);
            return;
        } else if (msg.getType() == SyncMessage.TYPE_BAD_ID) {
            log.warn("Got TYPE_BAD_ID sync responce from {} ", peer);
            workCallback(sync, STATE_UNPROPER, link);
            return;
        } else if (msg.getType() == SyncMessage.TYPE_BAD_SEQ) {
            log.warn("Got TYPE_BAD_SEQ sync responce from {}", peer);
            workCallback(sync, STATE_UNPROPER, link);
            return;
        } else if (msg.getType() == SyncMessage.TYPE_BAD_KEY) {
            log.warn("Got BAD_KEY sync responce from {} my key chain of him was:{}", peer, sync.getMember().getKeyChain());
            workCallback(sync, STATE_UNPROPER, link);
            session.setAttribute("planned_close");
            session.close(false);
            return;
        }
        if (msg.getSequence() > SyncMessage.SEQ_MAX) {
            log.warn("Too many conversion between peer {}", peer);
            SyncMessage response = createSimpleResponse(SyncMessage.TYPE_BAD_SEQ, startupState, mode);
            session.write(response);
            session.setAttribute("planned_close");
            session.close(false);
            workCallback(sync, STATE_UNPROPER, link);
            return;
        }

        if (msg.getType() == SyncMessage.TYPE_FAILD_RING) {
            log.warn("Got TYPE_FAILD_RING sync responce from {} ", peer);
            workCallback(sync, STATE_UNPROPER, link);
            session.setAttribute("planned_close");
            session.close(false);
            return;
        }

        if (msg.getSyncMode() == SyncMode.SYNC_MESSAGE) {
            Collection<SyncContent> contents = msg.getContents();
            if (contents == null) {
                log.warn("Wrong state - RCVD contents should not be null");
					/*
					 * I think it is wrong to put these to line in here
					 * so I replaced them with workCallback()
					 * numberOfTrieds ++;
					createResult();*/
                session.setAttribute("planned_close");
                session.close(false);
                workCallback(sync, STATE_UNPROPER, link);
                return;
            }

            Set<SyncContent> responseContents = new HashSet<>();
            Map<String, SyncContent> failedContents = new HashMap<>();
            int numberOfNull = 0;

            for (SyncContent sc : contents) {
                byte[] m = sc.getContent();
                if (m == null) {
                    Set<Short> awareMembers = sc.getAwareIds();
                    if (awareMembers == null) {
                        awareMembers = new HashSet<>();
                    }
                    awareMembers.add(msg.getId());
                    SyncResult sr = syncFeature.get(sc.getKey());
                    if (sc.getVersion() > 0) {
                        fillSyncResultForVersionBiggerThan0(msg, isRing, awareMembers, sr);
                        awareMembers.add(me.getId());
                        if (sc.getKey() != null && !sc.getKey().equals("")) {
                            syncContext.addAwareNodes(sc.getKey(), sc.getVersion(), awareMembers);
                            log.debug("Content {} synced up perfectly with version {}", sc.getKey(), sc.getVersion());
                        }
                        continue;
                    }

                    handleNullDataOfStarterHandler(msg, isRing, failedContents, sc, awareMembers, sr);
                    continue;
                }

                IMessage decoder = this.encoder.newInstance();
                SyncProtocolOutput out = new SyncProtocolOutput();
                boolean result = decodeGetCallbackResult(callbak, session, decoder, m, sc.getAwareIds(), out);
                List<IMessage> responses = out.getMessages();

                if (!result) {
                    if (isRing) {
                        failedContents.put(sc.getKey(), this.syncContents.get(sc.getKey()));
                    }
                    if (responses != null) {
                        for (IMessage response : responses) {
                            if (response != null) {
                                Set<Short> awareNodes = syncContext.getAwareNodes(response.getKey(), response.getVersion());
                                responseContents.add(new SyncContent(response.getKey(), response.getVersion(),
                                        awareNodes, response.serialize()));
                            }
                        }
                    } else {
                        numberOfNull = handleResponseContentsForNullResponses(msg, isRing, responseContents, numberOfNull, sc);
                    }
						/*
						 * I think it is not right to do this here
						 * if(response == null) {

							syncFeature.get(sc.getKey()).addFailedMember(msg.getId());
						}*/
                } else {
                    Set<Short> awareNodes = sc.getAwareIds();
                    if (awareNodes == null) {
                        awareNodes = new HashSet<>();
                    }
                    awareNodes.add(me.getId());
                    awareNodes.add(msg.getId());
                    if (sc.getVersion() > 0 && sc.getKey() != null && !sc.getKey().equals("")) {
                        syncContext.addAwareNodes(sc.getKey(), sc.getVersion(), awareNodes);
                    }
                    syncResultHandleStarter2(msg, isRing, sc, awareNodes);
                    if (responses != null) {
                        for (IMessage response : responses) {
                            if (response != null) {
                                awareNodes = syncContext.getAwareNodes(response.getKey(), response.getVersion());
                                responseContents.add(new SyncContent(sc.getKey(), response.getVersion(),
                                        awareNodes, response.serialize()));
                            }
                        }
                    }
                    /*else {

                        responseContents.add(new SyncContent(sc.getKey(),
                                sc.getVersion(),
                                awareNodes, null));
                    }*/
                }
            }

            if (checkAndPrepareFailedResult(session, isRing, responseContents, failedContents, numberOfNull)) {
                return;
            }

            /* Here in message sync, we don't send originators key chain back either.
             * This way, an edge will accept zone synchronizations form an unknown Edge.
             * But because originator brought a valid key, I think this is OK
             * response.setKeyChain(him.getKeyChain());*/

            SyncMessage m = createCompleteResponse(SyncMessage.TYPE_CHECK, null, SyncMode.SYNC_MESSAGE, this.sync, (byte) (msg.getSequence() + 1));
            m.setContents(responseContents);
            session.write(m);
        } else {
            Collection<SyncContent> contents = msg.getContents();
            if (contents == null) {
                log.warn("Wrong state - RCVD contents should not be null");
					/*
					 * I think it is wrong to put these to line in here
					 * so I replaced them with workCallback()
					 * numberOfTrieds ++;
					createResult();*/
                session.setAttribute("planned_close");
                session.close(true);
                workCallback(sync, STATE_UNPROPER, link);
                return;
            }

            if (log.isDebugEnabled()) {
                log.debug("Received contents {} from {}", contents, msg.getId());
            }

            Set<SyncContent> responseContents = new HashSet<>();
            Map<String, SyncContent> faildContents = new HashMap<>();
            ClusterSyncCallback clusterCallback = new ClusterSyncCallback(syncContext);
            int numberOfNull = 0;
            for (SyncContent sc : contents) {
                byte[] m = sc.getContent();
                if (m == null) {
                    Set<Short> awareNodes = sc.getAwareIds();
                    if (awareNodes == null) {
                        awareNodes = new HashSet<>();
                    }
                    awareNodes.add(msg.getId());

                    if (sc.getVersion() > 0) {
                        SyncResult sr = syncFeature.get(sc.getKey());
                        fillSyncResultForVersionBiggerThan0(msg, isRing, awareNodes, sr);
                        try {
                            Member member = syncContext.getMemberById(Short.parseShort(sc.getKey()));
                            if (member != null) {
                                awareNodes.add(me.getId());
                                member.addAwareId(awareNodes);
                            } else {
                                log.warn("how come member {} is null!!!", sc.getKey());
                            }
                        } catch (Exception e) {
                            log.error("Error in parsing {}", sc.getKey(), e);
                        }
                        continue;
                    }
                    SyncResult sr = syncFeature.get(sc.getKey());
                    handleNullDataOfStarterHandler(msg, isRing, faildContents, sc, awareNodes, sr);
                }

                ClusterMessage decoded = new ClusterMessage();
                SyncProtocolOutput out = new SyncProtocolOutput();
                boolean result = decodeGetCallbackResult(clusterCallback, session, decoded, m, sc.getAwareIds(), out);
                List<IMessage> responses = out.getMessages();

                if (!result) {
                    if (isRing) {
                        faildContents.put(sc.getKey(), this.syncContents.get(sc.getKey()));
                    }
                    if (responses != null) {
                        for (IMessage respons : responses) {
                            ClusterMessage response = (ClusterMessage) respons;
                            fillSyncContents(response, sc, responseContents);
                        }
                    } else {
                        numberOfNull = handleResponseContentsForNullResponses(msg, isRing, responseContents, numberOfNull, sc);
                    }
                    /*
                     * I think it is not right to do this here
                     * if(response == null) {

                        syncFeature.get(sc.getKey()).addFailedMember(msg.getId());
                    }*/
                } else {
                    Member node = syncContext.getMemberById(decoded.getId());
                    Set<Short> awareIds = sc.getAwareIds();
                    if (node != null) {
                        awareIds = node.getAwareIds();
                    } else {
                        log.warn("how come node is null!!");
                    }
                    Set<Short> awareNodes = awareIds;
                    syncResultHandleStarter2(msg, isRing, sc, awareNodes);
                    if (responses != null) {
                        for (IMessage respons : responses) {
                            ClusterMessage response = (ClusterMessage) respons;
                            node = syncContext.getMemberById(response.getId());
                            awareIds = null;
                            if (node != null) {
                                awareIds = node.getAwareIds();
                            }
                            responseContents.add(new SyncContent(sc.getKey(), response.getVersion(),
                                    awareIds, response.serialize()));
                        }
                    }
                    /*else {

                        responseContents.add(new SyncContent(sc.getKey(),
                                sc.getVersion(),
                                awareIds, null));

                        numberOfTrueResultsWithoutContents ++;
                    }*/
                }
            }

            if (checkAndPrepareFailedResult(session, isRing, responseContents, faildContents, numberOfNull)) return;

            /* Here in message sync, we don't send originators key chain back either.
             * This way, an edge will accept zone synchronizations from an unknown Edge.
             * But because originator brought a valid key, I think this is OK
             * response.setKeyChain(him.getKeyChain());*/
            SyncMessage m = createCompleteResponse(SyncMessage.TYPE_CHECK, null, SyncMode.SYNC_CLUSTER, this.sync, (byte) (msg.getSequence() + 1));
            m.setContents(responseContents);
            session.write(m);
            if (log.isDebugEnabled()) {
                log.debug("message responded {}, type {}, sequence {}", responseContents, m.getType(), m.getSequence());
            }
        }
    }

    private boolean checkAndPrepareFailedResult(IoSession session, boolean isRing, Set<SyncContent> responseContents, Map<String, SyncContent> faildContents, int numberOfNull) {
        if (isRing) {
            if (this.expectedNodes.size() == 0) {
                this.syncContents = faildContents;
            }
        }

        int responsesSize = responseContents.size();
        log.debug("responsesSize = {}, numberOfNull = {}", responsesSize, numberOfNull);
        if (responsesSize == 0 || numberOfNull == responsesSize) {
            numberOfTrieds++;
            createResult();
            session.setAttribute("planned_close");
            session.close(false);
            return true;
        }
        return false;
    }

    private void syncResultHandleStarter2(SyncMessage msg, boolean isRing, SyncContent sc, Set<Short> awareNodes) {
        SyncResult r = syncFeature.get(sc.getKey());
        if (r != null) {
            Set<Short> expectedIds = msg.getExpectedIds();
            short memberId = 0;
            if (isRing && expectedIds != null) {
                expectedIds.addAll(awareNodes);
                for (short id : ids) {
                    memberId = id;
                    if (expectedIds.contains(memberId)) {
                        r.removeFailedMember(memberId);
                        r.addSyncedMember(memberId);
                    }
                }
            } else {
                for (short id : ids) {
                    memberId = id;
                    if (awareNodes.contains(memberId)) {
                        r.removeFailedMember(memberId);
                        r.addSyncedMember(memberId);
                    }
                }
            }
        }

        if (this.expectedNodes != null &&
                this.expectedNodes.size() > 0) {
            SyncContent scc = this.syncContents.get(sc.getKey());
            if (scc != null) {
                scc.addAwareId(sc.getAwareIds());
            }
        }
    }

    private int handleResponseContentsForNullResponses(SyncMessage msg, boolean isRing, Set<SyncContent> responseContents, int numberOfNull, SyncContent sc) {
        HashSet<Short> iFaild = new HashSet<>();
        iFaild.add(me.getId());
        Set<Short> awareNodes = msg.getExpectedIds();
        SyncResult sr = syncFeature.get(sc.getKey());
        if (isRing && awareNodes != null) {
            for (short id : ids) {
                if (awareNodes.contains(id)) {
                    sr.addFailedMember(id);
                    sr.removeSyncedMember(id);
                }
            }
        } else {
            sr.addFailedMember(msg.getId());
            sr.removeSyncedMember(msg.getId());
        }
        numberOfNull++;
        responseContents.add(new SyncContent(sc.getKey(),
                0/*sc.getVersion()*/,
                iFaild, null));
        return numberOfNull;
    }

    private void handleNullDataOfStarterHandler(SyncMessage msg, boolean isRing, Map<String, SyncContent> faildContents, SyncContent sc, Set<Short> awareMembers, SyncResult sr) {
        if (isRing) {
            for (short id : ids) {
                if (awareMembers.contains(id)) {
                    sr.addFailedMember(id);
                    sr.removeSyncedMember(id);
                }
            }
        } else {
            sr.addFailedMember(msg.getId());
            sr.removeSyncedMember(msg.getId());
        }

        log.warn("Content {} failed", sc.getKey());
        faildContents.put(sc.getKey(), this.syncContents.get(sc.getKey()));
    }

    private void fillSyncResultForVersionBiggerThan0(SyncMessage msg, boolean isRing, Set<Short> awareNodes, SyncResult sr) {
        if (isRing) {
            for (short id : ids) {
                if (awareNodes.contains(id)) {
                    sr.addSyncedMember(id);
                    sr.removeFailedMember(id);
                }
            }
        } else {
            sr.addSyncedMember(msg.getId());
            sr.removeFailedMember(msg.getId());
        }
    }

    @Override
    public void messageReceived(IoSession session, Object message)
            throws Exception {
        if (!(message instanceof SyncMessage)) {
            log.error("Maliformed decoded message!");
            return;
        }
        String peer = ((InetSocketAddress) session.getRemoteAddress()).getAddress().getHostAddress();
        if (me == null) {
            log.error("Could not find myself!");
            return;
        }
        //XXX here I discarded any possibility of TCP sequence number hacking
        SyncMessage msg = (SyncMessage) message;
        log.debug("new message received from {} with sync mode {} and Sequence {}",
                msg.getId(), msg.getSyncMode(), msg.getSequence());
		/*
		 * If this member does not know the member sent this
		 * message, with this code, it discards the communication.
		 * But this is wrong. Because this way there is no possibility
		 * of adding new edges to cluster.
		 * So I decided to assign this rule:
		 * If an edge starts communication via EdgeSocket, it must bring
		 * destination edge's key chain. But when an edge received a message,
		 * it does not need to put senders key chain in answer.
		 * The security is on TCP's shoulder.
		 *
		if(him == null) {
			him = ClusterSynch.getEdgeInfoSynch(msg.getId());
			if(him==null) {
				log.error("Could not find Edge {} info",msg.getId());
				SynchResponce r = new SynchResponce();
				r.setId(me.getId());
				r.setType(SyncMessage.TYPE_BAD_ID);
				session.write(r);
				r = null;
				session.close(false);
				return;
			}
		}*/
        if (!isSender) {
            handleListener(session, msg, peer);
        } else {
            handleSender(session, msg, peer);
        }
    }

    @Override
    public void exceptionCaught(IoSession session, Throwable cause) {

        if (cause instanceof IOException) {
            log.error("{} by {}", cause.getMessage(), ((InetSocketAddress) session.getRemoteAddress())
                    .getAddress().getHostAddress());
            return;
        } else {
            log.error("", cause);
        }

        if (isSender) {
            InetSocketAddress peer = ((InetSocketAddress) session.getRemoteAddress());
            String link = peer.getAddress().getHostAddress() + ":" + peer.getPort();
            SyncSession sync = (SyncSession) session.getAttribute("SyncSession");
            workCallback(sync, STATE_WORK_FAILED, link);
        }
    }

    @Override
    public void sessionClosed(IoSession session) {
        if (isSender && !session.containsAttribute("planned_close")) {
            SyncSession sync = (SyncSession) session.getAttribute("SyncSession");
            log.debug("Session unexpectly closed. Starting close worker");
            workCallback(sync, STATE_UNPROPER, link);
        }
    }

    public void workCallback(SyncSession session, byte state, String link_ip) {
        switch (sync) {
            case RING:
            case RING_QUORUM:
            case RING_BALANCE:
            case RING_BALANCE_QUORUM:
            case UNICAST_ONE_OF:
                if (sessions == null) {
                    log.warn("Sessions had already been made null");
                    return;
                }

                if (state == STATE_WORK_FAILED ||
                        state == STATE_UNPROPER) {
                    Member e = session.getMember();
                    if (e != null && e.getState() == Member.STATE_DEL) {
                        log.debug("Setting edge {} as DOWN", e.getId());
                        syncContext.synchronizedStateChange(e, Member.STATE_DWN);
                        unProperSockets.add(currentSocket);
                        if (sync != SyncType.UNICAST_ONE_OF) {
                            this.expectedNodes.remove(session.getMemberId());
                        }
                    }
                }

                if (state == STATE_WORK_FAILED) {
                    log.error("Last sync failed with edge {} due to failure of link {}. Trying other links"
                            , session.getMemberId(), link_ip);
                    try {
                        session.setupNextSocket();
                    } catch (Exception e1) {
                        log.error("", e1);
                    }

                    if (session.isAllTried()) {
                        if (sync != SyncType.UNICAST_ONE_OF) {
                            this.expectedNodes.remove(session.getMemberId());
                        }
                        numberOfTrieds++;
                        addFaildNodeToSynchFeatures(session.getMember().getId());
                    }
                    failureOrUnproperSocketWorkAround();
                    return;
                } else if (state == STATE_UNPROPER) {
                    synchronized (session.improperMutex) {
                        if (session.isImproper()) {
                            return;
                        }
                        session.setImproper(true);
                        log.error("Last sync failed, due to struggle (or other hard errors) with edge {}. Trying other links", session.getMemberId());
                        unProperSockets.add(currentSocket);
                        if (sync != SyncType.UNICAST_ONE_OF) {
                            this.expectedNodes.remove(session.getMemberId());
                        }
                        numberOfTrieds++;
                        addFaildNodeToSynchFeatures(session.getMember().getId());
                        failureOrUnproperSocketWorkAround();
                        return;
                    }
                }

                break;

            case UNICAST:
            case UNICAST_QUORUM:
            case UNICAST_BALANCE:
            case UNICAST_BALANCE_QUORUM:
                if (state == STATE_WORK_FAILED ||
                        state == STATE_UNPROPER) {
                    Member member = session.getMember();
                    if (member != null && member.getState() == Member.STATE_DEL) {
                        log.warn("Setting edge {} as DOWN", member.getId());
                        syncContext.synchronizedStateChange(member, Member.STATE_DWN);
                        numberOfTrieds++;
                        addFaildNodeToSynchFeatures(member.getId());
                        break;
                    }
                }

                if (state == STATE_WORK_FAILED) {
                    log.error("Last sync failed with edge {} due to failure of link {}. Trying other links"
                            , session.getMemberId(), link_ip);
                    try {
                        session.setupNextSocket();
                        if (!session.isAllTried()) {
                            SyncMessage message = createCompleteResponse(
                                    SyncMessage.TYPE_CHECK,
                                    startupState, mode, sync, (byte) -1);
                            message.setContents(this.syncContents.values());
                            session.sendMsg(message);
                            return;
                        } else {
                            numberOfTrieds++;
                            addFaildNodeToSynchFeatures(session.getMember().getId());
                        }
                    } catch (Exception e1) {
                        log.error("", e1);
                    }
                } else if (state == STATE_UNPROPER) {
                    log.error("Last sync failed, due to struggle (or othere other hard errors) with edge {}. Trying other links", session.getMemberId());
                    try {
                        synchronized (session.improperMutex) {
                            if (session.isImproper()) {
                                return;
                            }
                            session.setImproper(true);
                            numberOfTrieds++;
                            addFaildNodeToSynchFeatures(session.getMember().getId());
                        }
                    } catch (Exception e1) {
                        log.error("", e1);
                    }
                }
                break;
            default:
                break;
        }
        createResult();
    }

    private void failureOrUnproperSocketWorkAround() {
        int last_socket = currentSocket;
        currentSocket = (++currentSocket) % sessions.size();
        while (sessions
                .get(currentSocket)
                .isAllTried()
                || unProperSockets
                .contains(currentSocket)
                || (sync != SyncType.UNICAST_ONE_OF &&
                !this.expectedNodes.contains(sessions
                        .get(currentSocket).getMemberId()))) {
            if (currentSocket == last_socket) {
                log.error("All linkes tried with no success. Synchnig failed");
                //TODO I commented this in 14 APR 16
                //syncContext.inStartup = false;
                /*callbak.result(ids, false);*/
                sessions = null;
                fixMonitorLastModified();
                createResult();
                return;
            }
            currentSocket = (++currentSocket) % sessions.size();
        }
        SyncMessage message = createCompleteResponse(
                SyncMessage.TYPE_CHECK,
                startupState, mode, sync, (byte) -1);
        message.setContents(this.syncContents.values());
        sessions.get(currentSocket)
                .sendMsg(message);
    }

    private void createResult() {
        //TODO seems wrong for ring in balance mode
        if (sessions == null || (this.expectedNodes != null &&
                this.expectedNodes.size() == 0) ||
                ids == null || numberOfTrieds == ids.length ||
                (numberOfTrieds > 0 && this.sync == SyncType.UNICAST_ONE_OF)) {
            int syncingNodesLength = ids == null ? 0 : ids.length;
            boolean isQuorum = sync == SyncType.UNICAST_QUORUM ||
                    sync == SyncType.RING_QUORUM ||
                    sync == SyncType.RING_BALANCE_QUORUM ||
                    sync == SyncType.UNICAST_BALANCE_QUORUM;
            for (Entry<String, SyncResult> eit : syncFeature.entrySet()) {
                SyncResult s = eit.getValue();
                if (isQuorum) {
                    if (syncingNodesLength == 0) {
                        s.setSuccessful(false);
                    } else if (s.getFailedMembers().size() * 2 < syncingNodesLength) {
                        s.setSuccessful(true);
                    } else if (s.getSyncedMembers().size() * 2 > syncingNodesLength) {
                        s.setSuccessful(true);
                    } else {
                        s.setSuccessful(false);
                    }
                } else {
                    if (syncingNodesLength == 0) {
                        s.setSuccessful(false);
                    } else if (s.getSyncedMembers().size() > s.getFailedMembers().size()) {
                        s.setSuccessful(true);
                    } else if (sync == SyncType.UNICAST_ONE_OF
                            && s.getFailedMembers().size() > 0) {
                        s.setSuccessful(false);
                    } else if (s.getFailedMembers().size() < syncingNodesLength) {
                        s.setSuccessful(true);
                    } else {
                        s.setSuccessful(false);
                    }
                }
                log.debug("Synch result for {} = {}", eit.getKey(), s.isSuccessful());
            }

            if (nonAsync) {
                synchronized (nonasyncLock) {
                    nonAsync = false;
                    nonasyncLock.notify();
                    return;
                }
            }

            if (callbak != null) {
                callbak.result(syncFeature);
            }
        } else if (this.expectedNodes != null && this.syncContents.size() > 0) {
            //means it is ring but something has not done completely
            currentSocket = (++currentSocket) % sessions.size();
            log.warn("Using {}nd link of {} links", currentSocket + 1, sessions.size());
            int last_socket = currentSocket;
            while (sessions
                    .get(currentSocket)
                    .isAllTried()
                    || unProperSockets
                    .contains(currentSocket)
                    || !this.expectedNodes.contains(sessions
                    .get(currentSocket).getMemberId())) {

                if (currentSocket == last_socket) {
                    log.error("All linkes tried with no success. Synchnig failed");
                    //TODO I commented this in 14 APR 16
                    //syncContext.inStartup = false;
                    /*callbak.result(ids, false);*/
                    sessions = null;
                    /*
                     * In zone synchronizing, lastModified is always 0
                     * so this is safe to call
                     */
                    fixMonitorLastModified();
                    createResult();
                    return;
                }
                currentSocket = (++currentSocket) % sessions.size();
            }

            SyncMessage message = new SyncMessage();
            message.setId(me.getId());
            message.setInStartup(startupState);
            message.setSyncMode(mode);
            message.setSyncType(this.sync);
            message.setType(SyncMessage.TYPE_CHECK);
            message.setContents(this.syncContents.values());
            sessions.get(currentSocket)
                    .sendMsg(message);
        }
    }

    private void addFaildNodeToSynchFeatures(Short id) {
        for (Entry<String, SyncResult> stringSynchResultEntry : syncFeature.entrySet()) {
            SyncResult s = stringSynchResultEntry.getValue();
            s.addFailedMember(id);
        }
    }

    private void syncWithBalance(List<IMessage> msg) {
        this.isSender = true;
        ClusterSnapshot snapshot = syncContext.getSnapshot();
        if (snapshot == null) {
            nonasyncLock = null;
            return;
        }
        List<Member> cluster = null;
        ISyncCallback callBackHandler = new ISyncCallback() {
            private short count = 0;

            @Override
            public synchronized void result(SyncFeature syncFeature) {
                count++;
                if (syncFeature != null) {
                    SyncHandler.this.syncFeature.putAll(syncFeature);
                }

                if (count == SyncHandler.this.ids.length) {
                    SyncHandler.this.numberOfTrieds = count;
                    SyncHandler.this.createResult();
                    //inside createResult we call callback
                    //SyncHandler.this.callbak.result(SyncHandler.this.syncFeature);
                }
            }

            @Override
            public boolean callBack(ISession session, IMessage message,
                                    Set<Short> withNodes, ISyncProtocolOutput out) {
                if (SyncHandler.this.callbak == null) {
                    return false;
                }
                return SyncHandler.this.callbak.callBack(session, message, withNodes, out);
            }
        };

        this.syncFeature = new SyncFeature();
        if (this.sync == SyncType.UNICAST_BALANCE ||
                this.sync == SyncType.UNICAST_BALANCE_QUORUM) {
            Map<Short, List<IMessage>> e2Mag = new HashMap<>();
            if (ids == null || ids.length == 0) {
                cluster = snapshot.getAliveCluster();
                if (cluster.size() > 0) {
                    ids = new short[cluster.size()];
                }
                int i = 0;
                for (Member e : cluster) {
                    ids[i++] = e.getId();
                    fillMessagesPerNode(msg, e2Mag, e);
                }
            } else {
                if (isInclusive) {
                    for (short id : ids) {
                        Member e = snapshot.getById(id, 2);
                        if (e != null) {
                            fillMessagesPerNode(msg, e2Mag, e);
                        }
                    }
                } else {
                    cluster = snapshot.getAliveCluster();
                    Set<Short> idss = new HashSet<>();
                    doInverseIds(cluster, idss, member -> {
                    });
                    this.ids = ArrayUtils.toPrimitive(idss.toArray(new Short[0]));
                }
            }

            if (e2Mag.size() == 0) {
                nonasyncLock = null;
                createSynchResultWhenDidtSend(msg);
                return;
            }

            for (Entry<Short, List<IMessage>> ee : e2Mag.entrySet()) {
                SyncHandler h = new SyncHandler(syncContext, this.sync)
                        .withCluster(ee.getKey())
                        .withCallBack(callBackHandler)
                        .withEncoder(this.encoder)
                        .withBalance();
                h.mode = this.mode;
                h.sync(ee.getValue());
            }

            return;
        } else if (this.sync == SyncType.RING_BALANCE ||
                this.sync == SyncType.RING_BALANCE_QUORUM) {
            Set<Member> members = new HashSet<>();
            if (ids == null || ids.length == 0) {
                cluster = snapshot.getAliveCluster();
                if (cluster.size() > 0) {
                    ids = new short[cluster.size()];
                }
                int i = 0;
                for (Member e : cluster) {
                    ids[i++] = e.getId();
                    members.add(e);
                }
            } else {
                if (isInclusive) {
                    for (short id : ids) {
                        Member e = snapshot.getById(id, ClusterSnapshot.MEMBER_CHECK_VALID);
                        if (e != null) {
                            members.add(e);
                        }
                    }
                } else {
                    cluster = snapshot.getAliveCluster();
                    Set<Short> idss = new HashSet<>();
                    doInverseIds(cluster, idss, members::add);
                    this.ids = ArrayUtils.toPrimitive(idss.toArray(new Short[0]));
                }
            }

            if (members.size() > 0) {
                Map<IMessage, Set<Short>> message2NotAwareMembers = new HashMap<>();
                for (IMessage m : msg) {
                    Set<Short> membersForMessage = new HashSet<>();
                    for (Member member : members) {
                        if (getIfIsAware(m, member)) {
                            continue;
                        }
                        membersForMessage.add(member.getId());
                    }

                    if (log.isDebugEnabled()) {
                        log.debug("Created edge groups {} for message {}", membersForMessage, m.getKey());
                    }

                    if (membersForMessage.size() > 0) {
                        message2NotAwareMembers.put(m, membersForMessage);
                    } else {
                        SyncResult sr = new SyncResult();
                        sr.setSuccessful(true);
                        this.syncFeature.put(m.getKey(), sr);
                    }
                }

                if (message2NotAwareMembers.size() > 0) {
                    Set<String> proccessedMessages = new HashSet<>();
                    Map<List<IMessage>, Set<Short>> finalMessages2Members = new HashMap<>();
                    for (Entry<IMessage, Set<Short>> ee : message2NotAwareMembers.entrySet()) {
                        if (ee.getValue().size() == 0 ||
                                proccessedMessages.contains(ee.getKey().getKey())) {
                            continue;
                        }

                        List<IMessage> mList = new ArrayList<>();
                        proccessedMessages.add(ee.getKey().getKey());
                        mList.add(ee.getKey());

                        for (Entry<IMessage, Set<Short>> eeInner : message2NotAwareMembers.entrySet()) {
                            if (eeInner.getValue().size() == 0 ||
                                    proccessedMessages.contains(eeInner.getKey().getKey())) {
                                continue;
                            }
                            if (ee.getValue().containsAll(eeInner.getValue()) &&
                                    eeInner.getValue().containsAll(ee.getValue())) {
                                mList.add(eeInner.getKey());
                                proccessedMessages.add(eeInner.getKey().getKey());
                            }
                        }
                        finalMessages2Members.put(mList, ee.getValue());
                    }

                    if (finalMessages2Members.size() > 0) {
                        for (Entry<List<IMessage>, Set<Short>> ee : finalMessages2Members
                                .entrySet()) {
                            SyncHandler h = new SyncHandler(syncContext, this.sync)
                                    .withCluster(ArrayUtils.toPrimitive(ee.getValue()
                                            .toArray(new Short[0])))
                                    .withCallBack(callBackHandler)
                                    .withEncoder(this.encoder)
                                    .withBalance();
                            h.mode = this.mode;
                            h.sync(ee.getKey());
                        }

                        return;
                    }
                }
            }

            createSynchResultWhenDidtSend(msg);
            nonasyncLock = null;
        }
        return;
    }

    private boolean getIfIsAware(IMessage m, Member e) {
        Set<Short> memberIds = null;
        if (this.mode == SyncMode.SYNC_MESSAGE) {
            memberIds = addAndGetAwareNodesOfMessage(m);
        } else {
            ClusterMessage cm = (ClusterMessage) m;
            Member node = syncContext.getMemberById(cm.getId());
            memberIds = node == null ? null : node.getAwareIds();
        }

        if (memberIds != null && memberIds.contains(e.getId())) {
            return true;
        }
        return false;
    }

    private void createSynchResultWhenDidtSend(List<IMessage> msg) {
        for (Iterator<IMessage> xIt = msg.iterator(); xIt.hasNext(); ) {
            IMessage m = xIt.next();
            SyncResult sr = new SyncResult();
            sr.setSuccessful(true);
            this.syncFeature.put(m.getKey(), sr);
        }
    }

    private void fillMessagesPerNode(List<IMessage> msg, Map<Short, List<IMessage>> e2Mag, Member member1) {
        List<IMessage> memberMsgz = new ArrayList<>();
        for (IMessage m : msg) {
            if (getIfIsAware(m, member1)) continue;
            memberMsgz.add(m);
        }

        if (memberMsgz.size() > 0) {
            e2Mag.put(member1.getId(), memberMsgz);
        }
    }

    void fixMonitorLastModified() {
    }
}
