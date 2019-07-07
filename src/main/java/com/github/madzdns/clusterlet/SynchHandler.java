package com.github.madzdns.clusterlet;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.Map.Entry;
import java.util.function.Consumer;

import javax.net.ssl.SSLContext;

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
import com.github.madzdns.clusterlet.codec.SynchContent;
import com.github.madzdns.clusterlet.codec.SynchMessage;
import com.github.madzdns.clusterlet.codec.SynchMessage.SynchMode;
import com.github.madzdns.clusterlet.codec.mina.SynchMinaDecoder;
import com.github.madzdns.clusterlet.codec.mina.SynchMinaEncoder;
import com.github.madzdns.clusterlet.helper.CollectionHelper;

public class SynchHandler extends IoHandlerAdapter {

    private final static String HIM_KEY = "_HimM_ckKey." + SynchHandler.class;
    private final static String SESSION_INITIATED_KEY = "_initkey." + SynchHandler.class;

    private Logger log = LoggerFactory.getLogger(SynchHandler.class);

    public final static byte STATE_WORK_DONE = 0;
    public final static byte STATE_WORK_FAILED = 1;
    public final static byte STATE_UNPROPER = 2;

    //private List<IMessage> msg ;
    boolean starter = false;
    private SynchType synch;
    private ISynchCallbak callbak;
    private short[] ids = null;
    SynchMode mode = SynchMode.SYNCH_MESSAGE;
    private List<SynchSession> sessions;
    private String link;
    private Set<Integer> unProperSockets = null;
    private int currentSocket = 0;
    private boolean nonasynch = false;
    private Object nonasyncLock = new Object();
    private short numberOfTrieds = 0;
    private boolean startupState;
    private SynchFeature synchFeature = null;
    private boolean isInclusive = true;
    private Map<String, SynchContent> synchContents = null;
    private boolean withBalance = false;
    private Set<Short> expectedNodes;
    SynchContext synchContext;
    Member me;
    private Class<? extends IMessage> encoder;

    private SynchHandler withModeCluster() {
        this.mode = SynchMode.SYNCH_CLUSTER;
        return this;
    }

    private SynchHandler withBalance() {
        this.withBalance = true;
        return this;
    }

    SynchHandler(SynchContext ctx) {
        this.starter = false;
        this.synchContext = ctx;
        this.me = ctx.getMyInfo();
    }

    SynchHandler(SynchContext ctx, SynchType synch) {
        this.starter = true;
        this.synch = synch;
        this.synchContext = ctx;
        this.me = ctx.getMyInfo();
        unProperSockets = new HashSet<Integer>();
    }

    /**
     * Sepcifies callback handler of type @see ISynchCallbak.
     * In order to synch messages with cluster, you must implement
     *
     * @param callbak
     * @return this SynchHandler
     * @see ISynchCallbak so using that, Frsynch knows how to deal
     * with the received message
     */
    public SynchHandler withCallBack(ISynchCallbak callbak) {
        this.callbak = callbak;
        return this;
    }

    /**
     * Specifies witch nodes you want to synch your messages. Also see @see SynchHandler#withoutCluster
     * If you leave this empty (or simply dont use it) mesage will be tried to synch with all nodes
     *
     * @param ids of nodes
     * @return this SynchHandler
     */
    public SynchHandler withCluster(short... ids) {
        this.isInclusive = true;
        this.ids = ids;
        return this;
    }

    /**
     * Specifies message encoder class
     *
     * @param encoder
     * @return this SynchHandler
     */
    public SynchHandler withEncoder(Class<? extends IMessage> encoder) {
        this.encoder = encoder;
        return this;
    }

    /**
     * Specifies witch nodes you don't want to synch your messages. Also see @seeSynchHandler#withCluster
     *
     * @param ids
     * @return this SynchHandler
     */
    public SynchHandler withoutCluster(short... ids) {
        isInclusive = false;
        this.ids = ids;
        return this;
    }

    /**
     * Starts to synch a single message with the cluster. This method should be called
     * after you specified callback and encoder and destinations. Also @see SynchHandler#synch(List<IMessage> msg)
     *
     * @param msg
     * @return
     */
    public SynchHandler synch(IMessage msg) {
        return synch(Collections.singletonList(msg));
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
                    sessions.add(member.createSynchSession(SynchHandler.this));
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
     * Starts to synch batch of messages with cluster. This method should be called
     * after you specified callback and encoder and destinations.
     *
     * @param msg
     * @return
     */
    public SynchHandler synch(List<IMessage> msg) {
        this.starter = true;
        if (msg == null || msg.size() == 0 ||
                this.callbak == null || (mode == SynchMode.SYNCH_MESSAGE && this.encoder == null)) {
            log.warn("messages {}, callback {} encoder {}", msg, callbak, encoder);
            nonasyncLock = null;
            checkWithBalanceAndSetCallback();
            return this;
        }

        ClusterSnapshot snapshot = synchContext.getSnapshot();
        if (snapshot == null) {
            nonasyncLock = null;
            checkWithBalanceAndSetCallback();
            return this;
        }

        if (SynchType.checkIfBalanceType(this.synch)) {
            if (!this.withBalance) {
                synchWithBalance(msg);
                return this;
            }
        }
        List<Member> cluster = snapshot.getAliveCluster();
        sessions = new ArrayList<>();
        if (SynchType.checkIfUnicastType(this.synch)) {
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
        } else if (SynchType.checkIfRingType(this.synch)) {
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

        startupState = synchContext.isInStartup();
        SynchMessage message = new SynchMessage();
        message.setSynchType(synch);
        message.setId(me.getId());
        message.setInStartup(startupState);
        message.setSynchMode(mode);
        message.setType(SynchMessage.TYPE_CHECK);

        synchContents = new HashMap<>();
        this.synchFeature = new SynchFeature();
        for (IMessage m : msg) {
            Set<Short> awareIds = null;
            if (this.mode == SynchMode.SYNCH_MESSAGE) {
                if (m.getKey() != null && !m.getKey().equals("") && m.getVersion() > 0) {
                    awareIds = addAndGetAwareNodesOfMessage(m);
                }
            } else if (m.getKey() != null && !m.getKey().equals("") && m.getVersion() > 0) {
                ClusterMessage cm = (ClusterMessage) m;
                Member node = synchContext.getMemberById(cm.getId());
                awareIds = node == null ? null : node.getAwareIds();
            }
            log.debug("aware members for {} = {}", m.getKey(), awareIds);
            synchContents.put(m.getKey(), new SynchContent(m.getKey(), m.getVersion(), awareIds, m.serialize()));
            SynchResult s = new SynchResult();
            s.addSynchedMember(awareIds);
            this.synchFeature.put(m.getKey(), s);
        }
        message.setContents(synchContents.values());
        if (log.isDebugEnabled()) {
            log.debug("created synchContents ==> {} for synch type {}", synchContents.values(), synch);
            log.debug("created sessions ==> {}", sessions);
        }

        if (sessions != null && sessions.size() > 0) {
            if (SynchType.checkIfRingType(synch) ||
                    synch == SynchType.UNICAST_ONE_OF) {
                sessions.get(0).sendMsg(message);
            } else if (SynchType.checkIfUnicastType(synch)) {
                for (SynchSession s : sessions) {
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
        awareIds = synchContext.getAwareNodes(m.getKey(), m.getVersion());
        if (awareIds == null || awareIds.size() == 0) {
            awareIds = new HashSet<>();
            awareIds.add(synchContext.myId);
            synchContext.addAwareNodes(m.getKey(), m.getVersion(), awareIds);
        }
        return awareIds;
    }

    /**
     * This method blocks execuion util the result is generated
     *
     * @return SynchFeature result
     */
    public SynchFeature get() {
        this.nonasynch = true;
        if (nonasyncLock == null) {
            return this.synchFeature;
        }
        synchronized (nonasyncLock) {
            while (nonasynch)
                try {
                    nonasyncLock.wait();
                } catch (InterruptedException e) {
                    log.error("", e);
                }
        }
        return this.synchFeature;
    }

    @Override
    public void sessionCreated(IoSession session) {
        if (!starter) {
            SSLContext ssl = NetProvider.getServerTLSContext(synchContext.getConfig().getKeyStorePath(),
                    synchContext.getConfig().getTrustStorePath(), synchContext.getConfig().getKeyStorePassword(),
                    synchContext.getConfig().getTrustStorePassword(), synchContext.getConfig().getKeyStorePassword2nd());
            if (ssl != null) {
                MinaSslFilter sslFilter = new MinaSslFilter(ssl, false);
                session.getFilterChain().addLast(MinaSslFilter.NAME, sslFilter);
            }
            session.getFilterChain().addLast("compress_filter",
                    new MinaCompressionFilter());
            session.getFilterChain().addLast("peer_coder",
                    new ProtocolCodecFilter(new ProtocolCodecFactory() {
                        private ProtocolDecoder decoder = new SynchMinaDecoder();
                        private ProtocolEncoder encoder = new SynchMinaEncoder();

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
        session.setAttribute("startupstate", synchContext.isInStartup());
    }

    private SynchMessage createSimpleResponse(byte messageType,
                                              Boolean startupStateFromSession,
                                              SynchMode synchMode) {
        SynchMessage response = new SynchMessage();
        response.setId(me.getId());
        if (startupStateFromSession != null) {
            response.setInStartup(startupStateFromSession);
        }
        response.setSynchMode(synchMode);
        response.setType(messageType);
        return response;
    }

    private SynchMessage createCompleteResponse(byte messageType,
                                                Boolean startupStateFromSession,
                                                SynchMode synchMode,
                                                SynchType synchType,
                                                Byte sequence) {
        SynchMessage response = createSimpleResponse(messageType, startupStateFromSession, synchMode);
        response.setSynchType(synchType);
        if (sequence > -1) {
            response.setSequence(sequence);
        }
        return response;
    }

    private void fillSyncContents(ClusterMessage response, SynchContent sc, Collection<SynchContent> responseContents) {
        if (response != null) {
            Member node = synchContext.getMemberById(response.getId());
            Set<Short> awareIds = null;
            if (node != null) {
                awareIds = node.getAwareIds();
            }
            responseContents.add(new SynchContent(sc.getKey(), response.getVersion(), awareIds,
                    response.serialize()));
        }
    }

    private boolean decodeGetCallbackResult(IoSession session, IMessage decoded, byte[] data, Set<Short> awareIds, SynchProtocolOutput out) {
        if (data != null) {
            decoded.deserialize(data);
        }
        return callbak.callBack(new MinaToISession(session), decoded, awareIds, out);
    }

    private Set<Short> getAliveMemberIds() {
        ClusterSnapshot snapshot = synchContext.getSnapshot();
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

    private SynchType getProperRingType(SynchMessage msg) {
        SynchType type = SynchType.RING_BALANCE;
        if (msg.getSynchType() == SynchType.RING_QUERIOM ||
                msg.getSynchType() == SynchType.RING_BALANCE_QUERIOM) {
            type = SynchType.RING_BALANCE_QUERIOM;
        }
        return type;
    }

    private boolean fillCallbackResult(boolean result, IMessage message, Collection<SynchContent> responseContents,
                                       List<IMessage> responses, Map<String, String> ringMsgToScMap,
                                       Set<Short> awareIds, SynchResult synchResult) {
        if (result) {
            if (responses == null) {
                //means it synched successfully
                responseContents.add(new SynchContent(ringMsgToScMap.get(message.getKey()),
                        message.getVersion(),
                        awareIds, null));
                return false;
            }
        } else {
            if (responses == null) {
                Set<Short> failedMembers = synchResult.getFailedMembers();
                failedMembers.add(me.getId());
                responseContents.add(new SynchContent(ringMsgToScMap.get(message.getKey()),
                        0,
                        failedMembers, null));
                return false;
            }
        }
        return true;
    }

    private void handleMessageSync(IoSession session, SynchMessage msg, Member him,
                                   Boolean startupStateFromSession,
                                   Boolean isFirstMessge) throws IllegalAccessException, InstantiationException {
        /*
         * Here, we check if a communicating edge is valid in our
         * database. If not, don't synch our ZONEs with him.
         * TODO why did not I check if him is null here? I think
         * I should not keep going if could not find him in zone
         * synchronizing. So I'm adding that part below. Feel free
         * to remove this in future if its not necessary
         */
        /*
         * TODO
         * If anybody send me a zone synch while I was not
         * valid anymore, should I communicate with him?
         */
        boolean not_valid_node = false;
        short id = 0;
        if (him == null || !him.isValid()) {
            not_valid_node = true;
            if (him != null) {
                id = him.getId();
            }
        }

        if (not_valid_node) {
            log.error("Communicating edge {} is not valid in my database", id);
            SynchMessage responce = createSimpleResponse(SynchMessage.TYPE_NOT_VALID_EDGE, startupStateFromSession, mode);
            session.write(responce);
            session.close(false);
            return;
        }
        //TODO If it is necessary to check cluster here
        Collection<SynchContent> contents = msg.getContents();
        if (log.isDebugEnabled()) {
            log.debug("Received contents {}", contents);
        }
        if (contents == null) {
            log.warn("contents was null fro synch messages from {}", msg.getId());
            session.close(false);
            return;
        }
        Set<SynchContent> responseContents = new HashSet<>();
        List<IMessage> messagesForRing = new ArrayList<>();
        boolean isRing = SynchType.checkIfRingType(msg.getSynchType());

        Map<String, String> ringMsgToScMap = null;
        if (isRing) {
            ringMsgToScMap = new HashMap<>();
        }

        for (SynchContent sc : contents) {
            byte[] m = sc.getContent();
            if (m == null) {
                //TODO should not check version and see if its is OK?
                //It sounds no because I don't send null
                log.warn("message was null for synching messages from {}", msg.getId());
                continue;
            }

            IMessage decoded = this.encoder.newInstance();
            SynchProtocolOutput out = new SynchProtocolOutput();
            boolean result = decodeGetCallbackResult(session, decoded, m, sc.getAwareIds(), out);
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
                    synchContext.addAwareNodes(decoded.getKey(), decoded.getVersion(), awareNodes);
                }
                if (isRing) {
                    messagesForRing.add(decoded);
                    ringMsgToScMap.put(decoded.getKey(), sc.getKey());
                    log.debug("message for ring {} added", decoded.getKey());
                } else {
                    if (responses == null) {
								/* I decided to not send this sort of situation
								 * responseContents.add(new SynchContent(sc.getKey(),
										sc.getVersion(),
										awareNodes, null));*/
                        continue;
                    }
                    for (IMessage response : responses) {
                        if (response != null) {
                            responseContents.add(new SynchContent(sc.getKey(), response.getVersion(), awareNodes, response.serialize()));
                        }
                    }
                }
            } else {
                if (responses == null) {
                    HashSet<Short> iFaild = new HashSet<Short>();
                    iFaild.add(me.getId());
                    log.debug("it was not synched successfully due to null responce and false result from callback");
                    //means it was not synched successfully
                    responseContents.add(new SynchContent(sc.getKey(), 0,
                            iFaild, null));
                    continue;
                }
                for (IMessage response : responses) {
                    if (response != null) {
                        responseContents.add(new SynchContent(sc.getKey(), response.getVersion(),
                                synchContext.getAwareNodes(response.getKey(), response.getVersion()), response.serialize()));
                    }
                }
            }
        }

        Set<Short> membersForRingUpdate = null;
        if (messagesForRing.size() > 0) {
            ISynchCallbak callBack = this.callbak;
            SynchType type = getProperRingType(msg);
            membersForRingUpdate = getAliveMemberIds();

            SynchFeature sf = new SynchHandler(synchContext, type)
                    .withCallBack(callBack)
                    .withEncoder(encoder)
                    .withoutCluster(msg.getId(), synchContext.myId)
                    .synch(messagesForRing)
                    .get();

            if (sf == null) {
                //TODO is this right?
                SynchMessage m = createSimpleResponse(SynchMessage.TYPE_FAILD_RING,
                        null, SynchMode.SYNCH_MESSAGE);
                log.warn("Synch failed due to null SF");
                session.write(m);
                session.close(false);
                return;
            }
            for (IMessage m : messagesForRing) {
                SynchResult s = sf.get(m.getKey());
                if (s.isSuccessful()) {
                    SynchProtocolOutput out = new SynchProtocolOutput();
                    Set<Short> awareIds = synchContext.getAwareNodes(m.getKey(), m.getVersion());
                    boolean result = decodeGetCallbackResult(session, m, null, awareIds, out);
                    List<IMessage> responses = out.getMessages();
                    if (!fillCallbackResult(result, m, responseContents, responses, ringMsgToScMap, awareIds, s)) {
                        continue;
                    }

                    for (IMessage response : responses) {
                        if (response != null) {
                            responseContents.add(new SynchContent(ringMsgToScMap.get(m.getKey()), response.getVersion(), awareIds,
                                    response.serialize()));
                        }
                    }
                } else {
                    log.debug("{} was not successfully syched with ring with others. Responcing with faild", m.getKey());
                    //means it was not synched successfully
                    responseContents.add(new SynchContent(ringMsgToScMap.get(m.getKey()), 0,
                            s.getFailedMembers(), null));
                }
            }

        }

        /* Here in message synch, we don't send originators key chain back either.
         * This way, an edge will accept zone synchronizations form an unknown Edge.
         * But because originator brought a valid key, I think this is OK
         * response.setKeyChain(him.getKeyChain());*/

        if (log.isDebugEnabled()) {
            log.debug("Contents {} is being replied to {}", responseContents, msg.getId());
        }

        if (responseContents.size() == 0) {
            SynchMessage m = createCompleteResponse(SynchMessage.TYPE_OK, null, SynchMode.SYNCH_MESSAGE, msg.getSynchType(), (byte) 0);
            session.write(m);
            return;
        }

        SynchMessage m = createCompleteResponse(SynchMessage.TYPE_CHECK, null, SynchMode.SYNCH_MESSAGE, msg.getSynchType(), (byte) (msg.getSequence() + 1));
        m.setContents(responseContents);
        if (isRing && isFirstMessge) {
            m.setExpectedIds(membersForRingUpdate);
        }
        session.write(m);
    }

    private void handleClusterSync(IoSession session, SynchMessage msg, Boolean isFirstMessage) {
        /*
         * First of all we remove startup flag
         */
        //TODO I commented this in 14 APR 16
        //synchContext.inStartup = false;
        Collection<SynchContent> contents = msg.getContents();
        if (contents == null /*|| contents.size == 0*/) {
            log.warn("Received contents was null. closing session");
            session.close(false);
            return;
        }
        boolean isRing = SynchType.checkIfRingType(msg.getSynchType());
        Map<String, String> ringMsgToScMap = null;
        if (isRing) {
            ringMsgToScMap = new HashMap<>();
        }

        ClusterSynchCallback clusterCallback = new ClusterSynchCallback(synchContext);
        Collection<SynchContent> responseContents = new ArrayList<>();
        List<IMessage> messagesForRing = new ArrayList<>();
        for (SynchContent sc : contents) {
            byte[] m = sc.getContent();
            if (m == null) {
                if (sc.getVersion() > 0) {
                    Member node = null;
                    try {
                        node = synchContext.getMemberById(Short.parseShort(sc.getKey()));
                        if (node != null) {
                            node.addAwareId(me.getId());
                            node.addAwareId(msg.getId());
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
            SynchProtocolOutput out = new SynchProtocolOutput();
            boolean result = decodeGetCallbackResult(session, decoded, m, sc.getAwareIds(), out);
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
            ISynchCallbak callBack = this.callbak;
            SynchType type = getProperRingType(msg);
            nodesForRingUpdate = getAliveMemberIds();
            SynchFeature sf = new SynchHandler(synchContext, type)
                    .withCallBack(callBack)
                    .withEncoder(encoder)
                    .withoutCluster(msg.getId(), synchContext.myId)
                    .withModeCluster()
                    .synch(messagesForRing)
                    .get();
            if (sf == null) {
                //TODO is this right?
                SynchMessage m = createCompleteResponse(SynchMessage.TYPE_FAILD_RING, null, SynchMode.SYNCH_CLUSTER, msg.getSynchType(), (byte) -1);
                log.warn("synch failed due to null SF");
                session.write(m);
                session.close(false);
                return;
            }

            for (IMessage m : messagesForRing) {
                SynchResult s = sf.get(m.getKey());
						/*if(s == null) {
							//In the case of balance, there might be some messages gets omitted
							continue;
						}*/
                if (s.isSuccessful()) {
                    SynchProtocolOutput out = new SynchProtocolOutput();
                    Member member = synchContext.getMemberById(((ClusterMessage) m).getId());
                    Set<Short> awareIds = null;
                    if (member != null) {
                        awareIds = member.getAwareIds();
                    }
                    boolean result = decodeGetCallbackResult(session, m, null, awareIds, out);
                    List<IMessage> responses = out.getMessages();
                    fillCallbackResult(result, m, responseContents, responses, ringMsgToScMap, awareIds, s);
                    for (IMessage response : responses) {
                        if (response != null) {
                            member = synchContext.getMemberById(((ClusterMessage) response).getId());
                            awareIds = null;
                            if (member != null) {
                                awareIds = member.getAwareIds();
                            }
                            responseContents.add(new SynchContent(ringMsgToScMap.get(m.getKey()), response.getVersion(), awareIds,
                                    response.serialize()));
                        }
                    }
                } else {
                    //means it was not synched successfully
                    responseContents.add(new SynchContent(ringMsgToScMap.get(m.getKey()), 0,
                            s.getFailedMembers(), null));
                }
            }

        }

        if (log.isDebugEnabled()) {
            log.debug("Response with contents {} to {}", responseContents, msg.getId());
        }

        if (responseContents.size() == 0) {
            SynchMessage m = createCompleteResponse(SynchMessage.TYPE_OK, null, SynchMode.SYNCH_CLUSTER, msg.getSynchType(), (byte) 0);
            session.write(m);
            return;
        }

        SynchMessage m = createCompleteResponse(SynchMessage.TYPE_CHECK, null, SynchMode.SYNCH_CLUSTER, msg.getSynchType(), (byte) (msg.getSequence() + 1));
        m.setContents(responseContents);
        if (isRing && isFirstMessage) {
            m.setExpectedIds(nodesForRingUpdate);
        }
        session.write(m);
        if (log.isDebugEnabled()) {
            log.debug("Message responded with contents {}, type {}, sequence {}", responseContents, m.getType(), m.getSequence());
        }
    }

    private void handleNoneStarter(IoSession session, SynchMessage msg, String peer) throws IllegalAccessException, InstantiationException {
        Boolean startupStateFromSession = (Boolean) session.getAttribute("startupstate");
        if (msg.getType() == SynchMessage.TYPE_OK) {
            log.debug("Returned type OK");
            session.close(false);
            return;
        } else if (msg.isInStartup() && startupStateFromSession) {
            log.error("Peer {} and I both were in statup", peer);
            session.write(createSimpleResponse(SynchMessage.TYPE_BOTH_STARTUP, startupStateFromSession, mode));
            session.close(false);
            return;
        } else if (msg.getType() == SynchMessage.TYPE_BAD_ID) {
            log.error("Got TYPE_BAD_ID synch responce from {}. My Id is {}", peer, me.getId());
            session.close(true);
            return;
        } else if (msg.getType() == SynchMessage.TYPE_BAD_SEQ) {
            log.error("{} and I could not get aggried on synching out zones", peer);
            session.close(true);
            return;
        } else if (msg.getSequence() > SynchMessage.SEQ_MAX) {
            log.error("Too many conversion between peer {}", peer);
            session.write(createSimpleResponse(SynchMessage.TYPE_BAD_SEQ, startupStateFromSession, mode));
            session.close(true);
            return;
        } else if (msg.getType() == SynchMessage.TYPE_FAILD_RING) {
            log.error("Got TYPE_FAILD_RING synch responce from {} ", peer);
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
                    log.error("Got wrong synch message from {} with wrong keychain:{}", peer, msg.getKeyChain());
                    log.error("my keychain:{}", me.getKeyChain());
                    SynchMessage response = createSimpleResponse(SynchMessage.TYPE_BAD_KEY, startupStateFromSession, mode);
                    session.write(response);
                    session.close(true);
                    return;
                }
            }
        }

        Member him = (Member) session.getAttribute(HIM_KEY);
        if (him == null) {
            him = synchContext.getMemberById(msg.getId());
            log.debug("Received node was {} for id {} in my database", him, msg.getId());
            session.setAttribute(HIM_KEY, him);
        }

        if (him != null && him.isDown()) {
            /*
             * Since this could send us a message, so its no longer down.
             * So we change its state to DEL
             * TODO why DEL????????
             */
            synchContext.synchronizedStateChange(him, Member.STATE_DEL);
        }

        if (msg.getSynchMode() == SynchMode.SYNCH_MESSAGE) {
            handleMessageSync(session, msg, him, startupStateFromSession, isFirstMessage);
        } else if (msg.getSynchMode() == SynchMode.SYNCH_CLUSTER) {
            handleClusterSync(session, msg, isFirstMessage);
        }
    }

    private void handleStarter(IoSession session, SynchMessage msg, String peer) throws IllegalAccessException, InstantiationException {
        SynchSession synch = (SynchSession) session.getAttribute("SynchSession");
        boolean isRing = SynchType.checkIfRingType(this.synch);
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

        if (msg.getType() == SynchMessage.TYPE_OK) {
            numberOfTrieds++;
            createResult();
            session.setAttribute("planned_close");
            session.close(false);
            return;
        } else if (msg.getType() == SynchMessage.TYPE_BOTH_STARTUP) {
            log.warn("Got TYPE_BOTH_STARTUP synch responce from {} ", peer);
            workCallback(synch, STATE_UNPROPER, link);
            return;
        } else if (msg.getType() == SynchMessage.TYPE_NOT_VALID_EDGE) {
            log.warn("Got TYPE_NOT_VALID_EDGE synch responce from {} ", peer);
            workCallback(synch, STATE_UNPROPER, link);
            return;
        } else if (msg.getType() == SynchMessage.TYPE_BAD_ID) {
            log.warn("Got TYPE_BAD_ID synch responce from {} ", peer);
            workCallback(synch, STATE_UNPROPER, link);
            return;
        } else if (msg.getType() == SynchMessage.TYPE_BAD_SEQ) {
            log.warn("Got TYPE_BAD_SEQ synch responce from {}", peer);
            workCallback(synch, STATE_UNPROPER, link);
            return;
        } else if (msg.getType() == SynchMessage.TYPE_BAD_KEY) {
            log.warn("Got BAD_KEY synch responce from {} my key chain of him was:{}", peer, synch.getMember().getKeyChain());
            workCallback(synch, STATE_UNPROPER, link);
            session.setAttribute("planned_close");
            session.close(false);
            return;
        }
        if (msg.getSequence() > SynchMessage.SEQ_MAX) {
            log.warn("Too many conversion between peer {}", peer);
            SynchMessage responce = createSimpleResponse(SynchMessage.TYPE_BAD_SEQ, startupState, mode);
            session.write(responce);
            session.setAttribute("planned_close");
            session.close(false);
            workCallback(synch, STATE_UNPROPER, link);
            return;
        }

        if (msg.getType() == SynchMessage.TYPE_FAILD_RING) {
            log.warn("Got TYPE_FAILD_RING synch responce from {} ", peer);
            workCallback(synch, STATE_UNPROPER, link);
            session.setAttribute("planned_close");
            session.close(false);
            return;
        }

        if (msg.getSynchMode() == SynchMode.SYNCH_MESSAGE) {
            Collection<SynchContent> contents = msg.getContents();
            if (contents == null) {
                log.warn("Wrong state - RCVD contents should not be null");
					/*
					 * I think it is wrong to put these to line in here
					 * so I replaced them with workCallback()
					 * numberOfTrieds ++;
					createResult();*/
                session.setAttribute("planned_close");
                session.close(false);
                workCallback(synch, STATE_UNPROPER, link);
                return;
            }

            Set<SynchContent> responseContents = new HashSet<>();
            Map<String, SynchContent> faildContents = new HashMap<>();
            int numberOfNull = 0;

            for (SynchContent sc : contents) {
                byte[] m = sc.getContent();
                if (m == null) {
                    Set<Short> awareMembers = sc.getAwareIds();
                    if (awareMembers == null) {
                        awareMembers = new HashSet<>();
                    }
                    awareMembers.add(msg.getId());
                    SynchResult sr = synchFeature.get(sc.getKey());
                    if (sc.getVersion() > 0) {
                        fillSyncResultForVersionBiggerThan0(msg, isRing, awareMembers, sr);
                        awareMembers.add(me.getId());
                        if (sc.getKey() != null && !sc.getKey().equals("")) {
                            synchContext.addAwareNodes(sc.getKey(), sc.getVersion(), awareMembers);
                            log.debug("Content {} synched up perfectly with version {}", sc.getKey(), sc.getVersion());
                        }
                        continue;
                    }

                    handleNullDataOfStarterHandler(msg, isRing, faildContents, sc, awareMembers, sr);
                    continue;
                }

                IMessage decoder = this.encoder.newInstance();
                SynchProtocolOutput out = new SynchProtocolOutput();
                boolean result = decodeGetCallbackResult(session, decoder, m, sc.getAwareIds(), out);
                List<IMessage> responses = out.getMessages();

                if (!result) {
                    if (isRing) {
                        faildContents.put(sc.getKey(), this.synchContents.get(sc.getKey()));
                    }
                    if (responses != null) {
                        for (IMessage response : responses) {
                            if (response != null) {
                                Set<Short> awareNodes = synchContext.getAwareNodes(response.getKey(), response.getVersion());
                                responseContents.add(new SynchContent(response.getKey(), response.getVersion(),
                                        awareNodes, response.serialize()));
                            }
                        }
                    } else {
                        numberOfNull = handleResponseContentsForNullResponses(msg, isRing, responseContents, numberOfNull, sc);
                    }
						/*
						 * I think it is not right to do this here
						 * if(response == null) {

							synchFeature.get(sc.getKey()).addFailedMember(msg.getId());
						}*/
                } else {
                    Set<Short> awareNodes = sc.getAwareIds();
                    if (awareNodes == null) {
                        awareNodes = new HashSet<>();
                    }
                    awareNodes.add(me.getId());
                    awareNodes.add(msg.getId());
                    if (sc.getVersion() > 0 && sc.getKey() != null && !sc.getKey().equals("")) {
                        synchContext.addAwareNodes(sc.getKey(), sc.getVersion(), awareNodes);
                    }
                    syncResultHandleStarter2(msg, isRing, sc, awareNodes);

                    if (responses != null) {

                        for (Iterator<IMessage> respIt = responses.iterator(); respIt.hasNext(); ) {

                            IMessage response = respIt.next();

                            if (response != null) {

                                awareNodes = synchContext.getAwareNodes(response.getKey(), response.getVersion());

                                responseContents.add(new SynchContent(sc.getKey(), response.getVersion(),
                                        awareNodes, response.serialize()));
                            }
                        }
                    }
						/*else {

							responseContents.add(new SynchContent(sc.getKey(),
									sc.getVersion(),
									awareNodes, null));
						}*/
                }
            }

            if (isRing) {

                if (this.expectedNodes.size() == 0) {

                    this.synchContents = faildContents;
                }
            }

            int responcesSize = responseContents.size();

            log.debug("responcesSize = {}, numberOfNull = {}", responcesSize, numberOfNull);

            if (responcesSize == 0 || numberOfNull == responcesSize) {

                numberOfTrieds++;
                createResult();

                session.setAttribute("planned_close");
                session.close(false);
                return;
            }

            /* Here in message synch, we don't send originators key chain back either.
             * This way, an edge will accept zone synchronizations form an unknown Edge.
             * But because originator brought a valid key, I think this is OK
             * response.setKeyChain(him.getKeyChain());*/

            SynchMessage m = new SynchMessage();
            m.setId(me.getId());
            m.setSynchMode(SynchMode.SYNCH_MESSAGE);
            m.setSynchType(this.synch);
            m.setType(SynchMessage.TYPE_CHECK);
            m.setContents(responseContents);
            m.setSequence((byte) (msg.getSequence() + 1));

            session.write(m);
        } else {

            Collection<SynchContent> contents = msg.getContents();

            if (contents == null) {

                log.warn("Wrong state - RCVD contents should not be null");

					/*
					 * I think it is wrong to put these to line in here
					 * so I replaced them with workCallback()
					 * numberOfTrieds ++;
					createResult();*/

                session.setAttribute("planned_close");
                session.close(true);

                workCallback(synch, STATE_UNPROPER, link);
                return;
            }

            if (log.isDebugEnabled()) {

                log.debug("Received contents {} from {}", contents, msg.getId());
            }

            Set<SynchContent> responseContents = new HashSet<SynchContent>();

            Map<String, SynchContent> faildContents = new HashMap<String, SynchContent>();

            ClusterSynchCallback clusterCallback = new ClusterSynchCallback(synchContext);

            int numberOfNull = 0;

            for (Iterator<SynchContent> it = contents.iterator();
                 it.hasNext(); ) {

                SynchContent sc = it.next();

                byte[] m = sc.getContent();

                if (m == null) {

                    Set<Short> awareNodes = sc.getAwareIds();

                    if (awareNodes == null) {

                        awareNodes = new HashSet<Short>();
                    }

                    awareNodes.add(msg.getId());

                    if (sc.getVersion() > 0) {

                        SynchResult sr = synchFeature.get(sc.getKey());

                        fillSyncResultForVersionBiggerThan0(msg, isRing, awareNodes, sr);

                        Member node = null;

                        try {

                            node = synchContext.getMemberById(Short.parseShort(sc.getKey()));

                            if (node != null) {

                                awareNodes.add(me.getId());

                                node.addAwareId(awareNodes);
                            } else {

                                log.warn("how come node is null!!!");
                            }

                        } catch (Exception e) {

                            log.error("Error in parsing {}", sc.getKey(), e);
                        }

                        continue;
                    }

                    SynchResult sr = synchFeature.get(sc.getKey());

                    handleNullDataOfStarterHandler(msg, isRing, faildContents, sc, awareNodes, sr);
                }

                ClusterMessage decoded = new ClusterMessage();
                SynchProtocolOutput out = new SynchProtocolOutput();
                boolean result = decodeGetCallbackResult(session, decoded, m, sc.getAwareIds(), out);
                List<IMessage> responses = out.getMessages();

                if (!result) {
                    if (isRing) {
                        faildContents.put(sc.getKey(), this.synchContents.get(sc.getKey()));
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

                        synchFeature.get(sc.getKey()).addFailedMember(msg.getId());
                    }*/
                } else {
                    Member node = synchContext.getMemberById(decoded.getId());
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
                            node = synchContext.getMemberById(response.getId());
                            awareIds = null;
                            if (node != null) {
                                awareIds = node.getAwareIds();
                            }
                            responseContents.add(new SynchContent(sc.getKey(), response.getVersion(),
                                    awareIds, response.serialize()));
                        }
                    }
                    /*else {

                        responseContents.add(new SynchContent(sc.getKey(),
                                sc.getVersion(),
                                awareIds, null));

                        numberOfTrueResultsWithoutContents ++;
                    }*/
                }
            }

            if (isRing) {
                if (this.expectedNodes.size() == 0) {
                    this.synchContents = faildContents;
                }
            }

            int responcesSize = responseContents.size();
            log.debug("responcesSize = {}, numberOfNull = {}", responcesSize, numberOfNull);
            if (responcesSize == 0 || numberOfNull == responcesSize) {
                numberOfTrieds++;
                createResult();
                session.setAttribute("planned_close");
                session.close(false);
                return;
            }

            /* Here in message synch, we don't send originators key chain back either.
             * This way, an edge will accept zone synchronizations from an unknown Edge.
             * But because originator brought a valid key, I think this is OK
             * response.setKeyChain(him.getKeyChain());*/
            SynchMessage m = createCompleteResponse(SynchMessage.TYPE_CHECK, null, SynchMode.SYNCH_CLUSTER, this.synch, (byte) (msg.getSequence() + 1));
            m.setContents(responseContents);
            session.write(m);
            if (log.isDebugEnabled()) {
                log.debug("message responded {}, type {}, sequence {}", responseContents, m.getType(), m.getSequence());
            }
        }
    }

    private void syncResultHandleStarter2(SynchMessage msg, boolean isRing, SynchContent sc, Set<Short> awareNodes) {
        SynchResult r = synchFeature.get(sc.getKey());
        if (r != null) {
            Set<Short> expectedIds = msg.getExpectedIds();
            short memberId = 0;
            if (isRing && expectedIds != null) {
                expectedIds.addAll(awareNodes);
                for (int i = 0; i < ids.length; i++) {
                    memberId = ids[i];
                    if (expectedIds.contains(memberId)) {
                        r.removeFailedMember(memberId);
                        r.addSynchedMember(memberId);
                    }
                }
            } else {
                for (int i = 0; i < ids.length; i++) {
                    memberId = ids[i];
                    if (awareNodes.contains(memberId)) {
                        r.removeFailedMember(memberId);
                        r.addSynchedMember(memberId);
                    }
                }
            }
        }

        if (this.expectedNodes != null &&
                this.expectedNodes.size() > 0) {
            SynchContent scc = this.synchContents.get(sc.getKey());
            if (scc != null) {
                scc.addAwareId(sc.getAwareIds());
            }
        }
    }

    private int handleResponseContentsForNullResponses(SynchMessage msg, boolean isRing, Set<SynchContent> responseContents, int numberOfNull, SynchContent sc) {
        HashSet<Short> iFaild = new HashSet<Short>();
        iFaild.add(me.getId());
        Set<Short> awareNodes = msg.getExpectedIds();
        SynchResult sr = synchFeature.get(sc.getKey());
        if (isRing && awareNodes != null) {
            short nodeId = 0;
            for (int i = 0; i < ids.length; i++) {
                nodeId = ids[i];
                if (awareNodes.contains(nodeId)) {
                    sr.addFailedMember(nodeId);
                    sr.removeSynchedMember(nodeId);
                }
            }
        } else {
            sr.addFailedMember(msg.getId());
            sr.removeSynchedMember(msg.getId());
        }
        numberOfNull++;
        responseContents.add(new SynchContent(sc.getKey(),
                0/*sc.getVersion()*/,
                iFaild, null));
        return numberOfNull;
    }

    private void handleNullDataOfStarterHandler(SynchMessage msg, boolean isRing, Map<String, SynchContent> faildContents, SynchContent sc, Set<Short> awareMembers, SynchResult sr) {
        if (isRing) {
            short nodeId = 0;
            for (int i = 0; i < ids.length; i++) {
                nodeId = ids[i];
                if (awareMembers.contains(nodeId)) {
                    sr.addFailedMember(nodeId);
                    sr.removeSynchedMember(nodeId);
                }
            }
        } else {

            sr.addFailedMember(msg.getId());
            sr.removeSynchedMember(msg.getId());
        }

        log.warn("Content {} faild", sc.getKey());
        faildContents.put(sc.getKey(), this.synchContents.get(sc.getKey()));
        return;
    }

    private void fillSyncResultForVersionBiggerThan0(SynchMessage msg, boolean isRing, Set<Short> awareNodes, SynchResult sr) {
        if (isRing) {
            short nodeId = 0;
            for (int i = 0; i < ids.length; i++) {
                nodeId = ids[i];
                if (awareNodes.contains(nodeId)) {
                    sr.addSynchedMember(nodeId);
                    sr.removeFailedMember(nodeId);
                }
            }
        } else {
            sr.addSynchedMember(msg.getId());
            sr.removeFailedMember(msg.getId());
        }
    }

    @Override
    public void messageReceived(IoSession session, Object message)
            throws Exception {
        if (!(message instanceof SynchMessage)) {
            log.error("Maliformed decoded message!");
            return;
        }
        String peer = ((InetSocketAddress) session.getRemoteAddress()).getAddress().getHostAddress();
        if (me == null) {
            log.error("Could not find myself!");
            return;
        }
        //XXX here I discarded any possibility of TCP sequence number hacking
        SynchMessage msg = (SynchMessage) message;
        log.debug("new message received from {} with synch mode {} and Sequence {}",
                msg.getId(), msg.getSynchMode(), msg.getSequence());
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
				r.setType(SynchMessage.TYPE_BAD_ID);
				session.write(r);
				r = null;
				session.close(false);
				return;
			}
		}*/
        if (!starter) {
            handleNoneStarter(session, msg, peer);
        } else {
            handleStarter(session, msg, peer);
        }
    }

    @Override
    public void exceptionCaught(IoSession session, Throwable cause)
            throws Exception {

        if (cause instanceof IOException) {
            log.error("{} by {}", cause.getMessage(), ((InetSocketAddress) session.getRemoteAddress())
                    .getAddress().getHostAddress());
            return;
        } else {
            log.error("", cause);
        }

        if (starter) {
            InetSocketAddress peer = ((InetSocketAddress) session.getRemoteAddress());
            String link =
                    new StringBuilder(peer.getAddress()
                            .getHostAddress())
                            .append(":")
                            .append(peer.getPort()).toString();
            SynchSession sync = (SynchSession) session.getAttribute("SynchSession");
            workCallback(sync, STATE_WORK_FAILED, link);
        }
    }

    @Override
    public void sessionClosed(IoSession session) throws Exception {
        if (starter && !session.containsAttribute("planned_close")) {
            SynchSession synch = (SynchSession) session.getAttribute("SynchSession");
            log.debug("Session unexpectly closed. Starting close worker");
            workCallback(synch, STATE_UNPROPER, link);
        }
    }

    public void workCallback(SynchSession session, byte state, String link_ip) {
        switch (synch) {
            case RING:
            case RING_QUERIOM:
            case RING_BALANCE:
            case RING_BALANCE_QUERIOM:
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
                        synchContext.synchronizedStateChange(e, Member.STATE_DWN);
                        unProperSockets.add(currentSocket);
                        if (synch != SynchType.UNICAST_ONE_OF) {
                            this.expectedNodes.remove(session.getFrNodeId());
                        }
                    }
                }

                if (state == STATE_WORK_FAILED) {
                    log.error("Last synch failed with edge {} due to failure of link {}. Trying other links"
                            , session.getFrNodeId(), link_ip);
                    try {
                        session.setupNextSocket();
                    } catch (Exception e1) {
                        log.error("", e1);
                    }

                    if (session.isAllTried()) {
                        if (synch != SynchType.UNICAST_ONE_OF) {
                            this.expectedNodes.remove(session.getFrNodeId());
                        }
                        numberOfTrieds++;
                        addFaildNodeToSynchFeatures(session.getMember().getId());
                    }

                    int last_socket = currentSocket;
                    currentSocket = (++currentSocket) % sessions.size();
                    log.warn("Using {}nd link of {} links", currentSocket, sessions.size());
                    while (sessions
                            .get(currentSocket)
                            .isAllTried()
                            || unProperSockets
                            .contains(currentSocket)
                            || (synch != SynchType.UNICAST_ONE_OF &&
                            !this.expectedNodes.contains(sessions
                                    .get(currentSocket).getFrNodeId()))) {

                        if (currentSocket == last_socket) {
                            log.error("All linkes tried with no success. Synchnig failed");
                            //TODO I commented this in 14 APR 16
                            //synchContext.inStartup = false;

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

                    SynchMessage message = new SynchMessage();
                    message.setId(me.getId());
                    message.setInStartup(startupState);
                    message.setSynchMode(mode);
                    message.setSynchType(this.synch);
                    message.setType(SynchMessage.TYPE_CHECK);
                    message.setContents(this.synchContents.values());

                    sessions.get(currentSocket)
                            .sendMsg(message);

                    return;
                } else if (state == STATE_UNPROPER) {
                    synchronized (session.unproperMutx) {
                        if (session.isUnproper()) {
                            return;
                        }
                        session.setUnproper(true);
                        log.error("Last synch failed, due to struggle (or other hard errors) with edge {}. Trying other links", session.getFrNodeId());
                        unProperSockets.add(currentSocket);
                        if (synch != SynchType.UNICAST_ONE_OF) {
                            this.expectedNodes.remove(session.getFrNodeId());
                        }
                        numberOfTrieds++;
                        addFaildNodeToSynchFeatures(session.getMember().getId());
                        int last_socket = currentSocket;
                        currentSocket = (++currentSocket) % sessions.size();
                        while (sessions
                                .get(currentSocket)
                                .isAllTried()
                                || unProperSockets
                                .contains(currentSocket)
                                || (synch != SynchType.UNICAST_ONE_OF &&
                                !this.expectedNodes.contains(sessions
                                        .get(currentSocket).getFrNodeId()))) {
                            if (currentSocket == last_socket) {
                                log.error("All linkes tried with no success. Synchnig failed");
                                //TODO I commented this in 14 APR 16
                                //synchContext.inStartup = false;

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

                        SynchMessage message = new SynchMessage();
                        message.setId(me.getId());
                        message.setInStartup(startupState);
                        message.setSynchMode(mode);
                        message.setSynchType(this.synch);
                        message.setType(SynchMessage.TYPE_CHECK);
                        message.setContents(this.synchContents.values());

                        sessions.get(currentSocket)
                                .sendMsg(message);
                        return;
                    }
                }

                break;

            case UNICAST:
            case UNICAST_QUERIOM:
            case UNICAST_BALANCE:
            case UNICAST_BALANCE_QUERIOM:
                if (state == STATE_WORK_FAILED ||
                        state == STATE_UNPROPER) {
                    Member member = session.getMember();
                    if (member != null && member.getState() == Member.STATE_DEL) {
                        log.warn("Setting edge {} as DOWN", member.getId());
                        synchContext.synchronizedStateChange(member, Member.STATE_DWN);
                        numberOfTrieds++;
                        addFaildNodeToSynchFeatures(member.getId());
                        break;
                    }
                }

                if (state == STATE_WORK_FAILED) {
                    log.error("Last synch failed with edge {} due to failure of link {}. Trying other links"
                            , session.getFrNodeId(), link_ip);
                    try {
                        session.setupNextSocket();
                        if (!session.isAllTried()) {
                            SynchMessage message = new SynchMessage();
                            message.setId(me.getId());
                            message.setInStartup(startupState);
                            message.setSynchMode(mode);
                            message.setSynchType(this.synch);
                            message.setType(SynchMessage.TYPE_CHECK);
                            message.setContents(this.synchContents.values());
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
                    log.error("Last synch failed, due to struggle (or othere other hard errors) with edge {}. Trying other links", session.getFrNodeId());
                    try {
                        synchronized (session.unproperMutx) {
                            if (session.isUnproper()) {
                                return;
                            }
                            session.setUnproper(true);
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

    private void createResult() {
        //TODO seems wrong for ring in balance mode
        if (sessions == null || (this.expectedNodes != null &&
                this.expectedNodes.size() == 0) ||
                ids == null || numberOfTrieds == ids.length ||
                (numberOfTrieds > 0 && this.synch == SynchType.UNICAST_ONE_OF)) {
            int synchingNodesLength = ids == null ? 0 : ids.length;
            boolean isQueriom = synch == SynchType.UNICAST_QUERIOM ||
                    synch == SynchType.RING_QUERIOM ||
                    synch == SynchType.RING_BALANCE_QUERIOM ||
                    synch == SynchType.UNICAST_BALANCE_QUERIOM;
            for (Entry<String, SynchResult> eit : synchFeature.entrySet()) {
                SynchResult s = eit.getValue();
                if (isQueriom) {
                    if (synchingNodesLength == 0) {
                        s.setSuccessful(false);
                    } else if (s.getFailedMembers().size() * 2 < synchingNodesLength) {
                        s.setSuccessful(true);
                    } else if (s.getSynchedMembers().size() * 2 > synchingNodesLength) {
                        s.setSuccessful(true);
                    } else {
                        s.setSuccessful(false);
                    }
                } else {
                    if (synchingNodesLength == 0) {
                        s.setSuccessful(false);
                    } else if (s.getSynchedMembers().size() > 0) {
                        s.setSuccessful(true);
                    } else if (synch == SynchType.UNICAST_ONE_OF
                            && s.getFailedMembers().size() > 0) {
                        s.setSuccessful(false);
                    } else if (s.getFailedMembers().size() < synchingNodesLength) {
                        s.setSuccessful(true);
                    } else {
                        s.setSuccessful(false);
                    }
                }
                log.debug("Synch result for {} = {}", eit.getKey(), s.isSuccessful());
            }

            if (nonasynch) {
                synchronized (nonasyncLock) {
                    nonasynch = false;
                    nonasyncLock.notify();
                    return;
                }
            }

            if (callbak != null) {
                callbak.result(synchFeature);
            }
        } else if (this.expectedNodes != null && this.synchContents.size() > 0) {
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
                    .get(currentSocket).getFrNodeId())) {

                if (currentSocket == last_socket) {
                    log.error("All linkes tried with no success. Synchnig failed");
                    //TODO I commented this in 14 APR 16
                    //synchContext.inStartup = false;
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

            SynchMessage message = new SynchMessage();
            message.setId(me.getId());
            message.setInStartup(startupState);
            message.setSynchMode(mode);
            message.setSynchType(this.synch);
            message.setType(SynchMessage.TYPE_CHECK);
            message.setContents(this.synchContents.values());
            sessions.get(currentSocket)
                    .sendMsg(message);
        }
    }

    private void addFaildNodeToSynchFeatures(Short id) {
        for (Entry<String, SynchResult> stringSynchResultEntry : synchFeature.entrySet()) {
            SynchResult s = stringSynchResultEntry.getValue();
            s.addFailedMember(id);
        }
    }

    private void synchWithBalance(List<IMessage> msg) {
        this.starter = true;
        ClusterSnapshot snapshot = synchContext.getSnapshot();
        if (snapshot == null) {
            nonasyncLock = null;
            return;
        }
        List<Member> cluster = null;
        ISynchCallbak callBackHandler = new ISynchCallbak() {
            private short count = 0;

            @Override
            public synchronized void result(SynchFeature synchFeature) {
                count++;
                if (synchFeature != null) {
                    SynchHandler.this.synchFeature.putAll(synchFeature);
                }

                if (count == SynchHandler.this.ids.length) {
                    SynchHandler.this.numberOfTrieds = count;
                    SynchHandler.this.createResult();
                    //inside createResult we call callback
                    //SynchHandler.this.callbak.result(SynchHandler.this.synchFeature);
                }
            }

            @Override
            public boolean callBack(ISession session, IMessage message,
                                    Set<Short> withNodes, ISynchProtocolOutput out) {
                if (SynchHandler.this.callbak == null) {
                    return false;
                }
                return SynchHandler.this.callbak.callBack(session, message, withNodes, out);
            }
        };

        this.synchFeature = new SynchFeature();
        if (this.synch == SynchType.UNICAST_BALANCE ||
                this.synch == SynchType.UNICAST_BALANCE_QUERIOM) {
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
                SynchHandler h = new SynchHandler(synchContext, this.synch)
                        .withCluster(ee.getKey())
                        .withCallBack(callBackHandler)
                        .withEncoder(this.encoder)
                        .withBalance();
                h.mode = this.mode;
                h.synch(ee.getValue());
            }

            return;
        } else if (this.synch == SynchType.RING_BALANCE ||
                this.synch == SynchType.RING_BALANCE_QUERIOM) {
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
                        SynchResult sr = new SynchResult();
                        sr.setSuccessful(true);
                        this.synchFeature.put(m.getKey(), sr);
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
                            SynchHandler h = new SynchHandler(synchContext, this.synch)
                                    .withCluster(ArrayUtils.toPrimitive(ee.getValue()
                                            .toArray(new Short[0])))
                                    .withCallBack(callBackHandler)
                                    .withEncoder(this.encoder)
                                    .withBalance();
                            h.mode = this.mode;
                            h.synch(ee.getKey());
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
        if (this.mode == SynchMode.SYNCH_MESSAGE) {
            memberIds = addAndGetAwareNodesOfMessage(m);
        } else {
            ClusterMessage cm = (ClusterMessage) m;
            Member node = synchContext.getMemberById(cm.getId());
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
            SynchResult sr = new SynchResult();
            sr.setSuccessful(true);
            this.synchFeature.put(m.getKey(), sr);
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
