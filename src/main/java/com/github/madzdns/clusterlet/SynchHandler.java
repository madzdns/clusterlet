package com.github.madzdns.clusterlet;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

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
    private boolean synchModel = true;
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

        this.synchModel = true;
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

        synchModel = false;
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

        return synch(Arrays.asList(msg));
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
            log.error("messages {}, callback {} encoder {}", msg, callbak, encoder);
            nonasyncLock = null;
            if (this.withBalance) {
                this.callbak.result(null);
            }
            return this;
        }

        ClusterSnapshot snapshot = synchContext.getSnapshot();
        if (snapshot == null) {
            nonasyncLock = null;
            if (this.withBalance) {
                this.callbak.result(null);
            }
            return this;
        }

        if (this.synch == SynchType.RING_BALANCE ||
                this.synch == SynchType.RING_BALANCE_QUERIOM ||
                this.synch == SynchType.UNICAST_BALANCE ||
                this.synch == SynchType.UNICAST_BALANCE_QUERIOM) {
            if (!this.withBalance) {
                synchWithBalance(msg);
                return this;
            }
        }
        List<Member> cluster = null;
        if (this.synch == SynchType.UNICAST ||
                this.synch == SynchType.UNICAST_QUERIOM ||
                this.synch == SynchType.UNICAST_BALANCE ||
                this.synch == SynchType.UNICAST_BALANCE_QUERIOM ||
                this.synch == SynchType.UNICAST_ONE_OF) {
            if (ids == null || ids.length == 0) {
                if (this.withBalance) {
                    this.callbak.result(null);
                    return this;
                }

                cluster = snapshot.getAliveCluster();
                sessions = new ArrayList<SynchSession>();
                if (cluster.size() > 0) {
                    ids = new short[cluster.size()];
                }

                int i = 0;
                for (Member e : cluster) {
                    ids[i++] = e.getId();
                    sessions.add(e.createSynchSession(this));
                }
            } else {
                sessions = new ArrayList<SynchSession>();
                if (synchModel) {
                    for (short id : ids) {
                        Member e = snapshot.getById(id, 2);
                        if (e != null) {
                            sessions.add(e.createSynchSession(this));
                        }
                    }
                } else {
                    cluster = snapshot.getAliveCluster();
                    ArrayList<Short> idss = new ArrayList<Short>();
                    boolean found = false;
                    for (Iterator<Member> it = cluster.iterator();
                         it.hasNext(); ) {
                        Member e = it.next();
                        found = false;
                        for (int i = 0; i < ids.length; i++) {
                            if (ids[i] == e.getId()) {
                                found = true;
                                break;
                            }
                        }
                        if (found) {
                            continue;
                        }
                        idss.add(e.getId());
                        sessions.add(e.createSynchSession(this));
                    }
                    this.ids = ArrayUtils.toPrimitive(idss.toArray(new Short[0]));
                }
            }
        } else if (this.synch == SynchType.RING ||
                this.synch == SynchType.RING_QUERIOM ||
                this.synch == SynchType.RING_BALANCE ||
                this.synch == SynchType.RING_BALANCE_QUERIOM) {
            if (ids == null || ids.length == 0) {
                if (this.withBalance) {
                    this.callbak.result(null);
                    return this;
                }

                cluster = snapshot.getAliveCluster();
                if (cluster.size() > 0) {
                    ids = new short[cluster.size()];
                    this.expectedNodes = new HashSet<Short>();
                }

                int i = 0;
                sessions = new ArrayList<SynchSession>();
                for (Member e : cluster) {
                    ids[i++] = e.getId();
                    sessions.add(e.createSynchSession(this));
                    this.expectedNodes.add(e.getId());
                }
            } else {
                sessions = new ArrayList<SynchSession>();
                if (synchModel) {
                    this.expectedNodes = new HashSet<Short>();
                    for (short id : ids) {
                        Member e = snapshot.getById(id, 2);
                        if (e != null) {
                            sessions.add(e.createSynchSession(this));
                            this.expectedNodes.add(id);
                        }
                    }
                } else {
                    cluster = snapshot.getAliveCluster();

                    ArrayList<Short> idss = new ArrayList<Short>();
                    this.expectedNodes = new HashSet<Short>();
                    boolean found = false;
                    for (Iterator<Member> it = cluster.iterator();
                         it.hasNext(); ) {
                        Member e = it.next();
                        found = false;
                        for (int i = 0; i < ids.length; i++) {
                            if (ids[i] == e.getId()) {
                                found = true;
                                break;
                            }
                        }

                        if (found) {
                            continue;
                        }
                        idss.add(e.getId());
                        sessions.add(e.createSynchSession(this));
                    }
                    this.expectedNodes.addAll(idss);
                    this.ids = ArrayUtils.toPrimitive(idss.toArray(new Short[0]));
                }
            }
        }

        this.startupState = synchContext.isInStartup();

        SynchMessage message = new SynchMessage();
        message.setSynchType(synch);
        message.setId(me.getId());
        message.setInStartup(startupState);
        message.setSynchMode(mode);
        message.setType(SynchMessage.TYPE_CHECK);

        synchContents = new HashMap<String, SynchContent>();

        this.synchFeature = new SynchFeature();

        for (Iterator<IMessage> it = msg.iterator();
             it.hasNext(); ) {

            IMessage m = it.next();

            Set<Short> awareIds = null;

            if (this.mode == SynchMode.SYNCH_MESSAGE) {

                if (m.getKey() != null && !m.getKey().equals("") && m.getVersion() > 0) {

                    awareIds = synchContext.getAwareNodes(m.getKey(), m.getVersion());

                    if (awareIds == null || awareIds.size() == 0) {

                        awareIds = new HashSet<Short>();
                        awareIds.add(synchContext.myId);
                        synchContext.addAwareNodes(m.getKey(), m.getVersion(), awareIds);
                    }
                }
            } else if (m.getKey() != null && !m.getKey().equals("") && m.getVersion() > 0) {

                ClusterMessage cm = (ClusterMessage) m;
                Member node = synchContext.getFrNodeById(cm.getId());

                awareIds = node == null ? null : node.getAwareIds();
            }

            log.debug("aware Nodes for {} = {}", m.getKey(), awareIds);

            synchContents.put(m.getKey(), new SynchContent(m.getKey(), m.getVersion(), awareIds, m.serialize()));

            SynchResult s = new SynchResult();
            s.addSynchedNode(awareIds);
            this.synchFeature.put(m.getKey(), s);
        }

        message.setContents(synchContents.values());

        if (log.isDebugEnabled()) {

            log.debug("created synchContents ==> {} for synch type {}", synchContents.values(), synch);
            log.debug("created sessions ==> {}", sessions);
        }

        if (sessions != null && sessions.size() > 0) {

            if (this.synch == SynchType.RING ||
                    this.synch == SynchType.RING_QUERIOM ||
                    this.synch == SynchType.RING_BALANCE ||
                    this.synch == SynchType.RING_BALANCE_QUERIOM ||
                    this.synch == SynchType.UNICAST_ONE_OF) {

                sessions.get(0).sendMsg(message);
            } else if (this.synch == SynchType.UNICAST ||
                    this.synch == SynchType.UNICAST_QUERIOM ||
                    this.synch == SynchType.UNICAST_BALANCE ||
                    this.synch == SynchType.UNICAST_BALANCE_QUERIOM) {

                for (Iterator<SynchSession> it = sessions.iterator();
                     it.hasNext(); ) {

                    SynchSession s = it.next();
                    s.sendMsg(message);
                }
            }
        } else {

            nonasyncLock = null;
            createResult();
        }

        return this;
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
        link = new StringBuilder(peer1.getAddress().getHostAddress())
                .append(":")
                .append(peer1.getPort()).toString();

        session.setAttribute("startupstate", synchContext.isInStartup());
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
            Boolean startupState = (Boolean) session.getAttribute("startupstate");
            if (msg.getType() == SynchMessage.TYPE_OK) {
                log.debug("Returned type OK");
                session.close(false);
                return;
            } else if (msg.isInStartup() && startupState) {
                log.error("Peer {} and I both were in statup", peer);
                SynchMessage responce = new SynchMessage();
                responce.setId(me.getId());
                responce.setInStartup(startupState);
                responce.setSynchMode(mode);
                responce.setType(SynchMessage.TYPE_BOTH_STARTUP);
                session.write(responce);
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
                SynchMessage responce = new SynchMessage();
                responce.setId(me.getId());
                responce.setInStartup(startupState);
                responce.setSynchMode(mode);
                responce.setType(SynchMessage.TYPE_BAD_SEQ);
                session.write(responce);
                session.close(true);
                return;
            } else if (msg.getType() == SynchMessage.TYPE_FAILD_RING) {
                log.error("Got TYPE_FAILD_RING synch responce from {} ", peer);
                session.close(true);
                return;
            }

            boolean isFirstMessge = false;
            if (!session.containsAttribute(SESSION_INITIATED_KEY)) {
                session.setAttribute(SESSION_INITIATED_KEY, true);
                isFirstMessge = true;
                if (me.getKey() != null) {
                    if (!CollectionHelper
                            .containsAny(me.getKeyChain()
                                    , msg.getKeyChain())) {
                        log.error("Got wrong synch message from {} with wrong keychain:{}", peer, msg.getKeyChain());
                        log.error("my keychain:{}", me.getKeyChain());

                        SynchMessage responce = new SynchMessage();
                        responce.setId(me.getId());
                        responce.setInStartup(startupState);
                        responce.setSynchMode(mode);
                        responce.setType(SynchMessage.TYPE_BAD_KEY);

                        session.write(responce);
                        session.close(true);

                        return;
                    }
                }
            }

            Member him = (Member) session.getAttribute(HIM_KEY);
            if (him == null) {
                him = synchContext.getFrNodeById(msg.getId());
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

                    SynchMessage responce = new SynchMessage();
                    responce.setId(me.getId());
                    responce.setInStartup(startupState);
                    responce.setSynchMode(mode);
                    responce.setType(SynchMessage.TYPE_NOT_VALID_EDGE);

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

                boolean isRing = msg.getSynchType() == SynchType.RING ||
                        msg.getSynchType() == SynchType.RING_QUERIOM ||
                        msg.getSynchType() == SynchType.RING_BALANCE ||
                        msg.getSynchType() == SynchType.RING_BALANCE_QUERIOM;

                Map<String, String> ringMsgToScMap = null;
                if (isRing) {
                    ringMsgToScMap = new HashMap<String, String>();
                }

                for (Iterator<SynchContent> it = contents.iterator();
                     it.hasNext(); ) {
                    SynchContent sc = it.next();
                    byte[] m = sc.getContent();
                    if (m == null) {
                        //TODO should not check version and see if its is OK?
                        //It sounds no because I don't send null
                        log.warn("message was null for synching messages from {}", msg.getId());
                        continue;
                    }

                    IMessage decoded = this.encoder.newInstance();
                    decoded.deserialize(m);
                    SynchProtocolOutput out = new SynchProtocolOutput();
                    boolean result = callbak.callBack(new MinaToISession(session), decoded, sc.getAwareIds(), out);
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

                        for (Iterator<IMessage> respIt = responses.iterator(); respIt.hasNext(); ) {

                            IMessage response = respIt.next();

                            if (response != null) {

                                responseContents.add(new SynchContent(sc.getKey(), response.getVersion(),
                                        synchContext.getAwareNodes(response.getKey(), response.getVersion()), response.serialize()));
                            }
                        }
                    }
                }

                Set<Short> nodesForRingUpdate = null;

                if (messagesForRing.size() > 0) {

                    ISynchCallbak callBack = this.callbak;
					
					/*try {
						
						callBack = this.callbak.getClass().newInstance();
						
					} catch (Exception e) {
						
						log.error("", e);
						SynchMessage m = new SynchMessage();
						m.setId(me.getId());
						m.setSynchMode(SynchMode.SYNCH_MESSAGE);
						m.setType(SynchMessage.TYPE_FAILD_RING);
						
						session.write(m);
						session.close(true);
						return;
					}*/

                    SynchType type = SynchType.RING_BALANCE;

                    if (msg.getSynchType() == SynchType.RING_QUERIOM ||
                            msg.getSynchType() == SynchType.RING_BALANCE_QUERIOM) {

                        type = SynchType.RING_BALANCE_QUERIOM;
                    }

                    ClusterSnapshot snapshot = synchContext.getSnapshot();

                    if (snapshot != null) {

                        List<Member> cluster = snapshot.getAliveCluster();

                        if (cluster.size() > 0) {

                            nodesForRingUpdate = new HashSet<Short>();

                            for (Iterator<Member> it = cluster.iterator(); it.hasNext(); ) {

                                nodesForRingUpdate.add(it.next().getId());
                            }
                        }
                    }

                    SynchFeature sf = new SynchHandler(synchContext, type)
                            .withCallBack(callBack)
                            .withEncoder(encoder)
                            .withoutCluster(msg.getId(), synchContext.myId)
                            .synch(messagesForRing)
                            .get();

                    if (sf == null) {
                        //TODO is this right?
                        SynchMessage m = new SynchMessage();
                        m.setId(me.getId());
                        m.setSynchMode(SynchMode.SYNCH_MESSAGE);
                        m.setType(SynchMessage.TYPE_FAILD_RING);
                        log.warn("Synch failed due to null SF");
                        session.write(m);
                        session.close(false);
                        return;
                    }

                    Iterator<IMessage> mIt = messagesForRing.iterator();

                    while (mIt.hasNext()) {

                        IMessage m = mIt.next();

                        SynchResult s = sf.get(m.getKey());

                        if (s.isSuccessful()) {

                            SynchProtocolOutput out = new SynchProtocolOutput();

                            Set<Short> awareIds = synchContext.getAwareNodes(m.getKey(), m.getVersion());

                            boolean result = callbak.callBack(new MinaToISession(session), m, awareIds, out);

                            List<IMessage> responses = out.getMessages();

                            if (result) {

                                if (responses == null) {

                                    //means it synched successfully
                                    responseContents.add(new SynchContent(ringMsgToScMap.get(m.getKey()),
                                            m.getVersion(),
                                            awareIds, null));
                                    continue;
                                }
                            } else {

                                if (responses == null) {

                                    Set<Short> n = s.getFaildNodes();
                                    n.add(me.getId());
                                    responseContents.add(new SynchContent(ringMsgToScMap.get(m.getKey()),
                                            0,
                                            n, null));
                                    continue;
                                }
                            }

                            for (Iterator<IMessage> respIt = responses.iterator(); respIt.hasNext(); ) {

                                IMessage response = respIt.next();

                                if (response != null) {

                                    responseContents.add(new SynchContent(ringMsgToScMap.get(m.getKey()), response.getVersion(), awareIds,
                                            response.serialize()));
                                }
                            }
                        } else {

                            log.debug("{} was not successfully syched with ring with others. Responcing with faild", m.getKey());
                            //means it was not synched successfully
                            responseContents.add(new SynchContent(ringMsgToScMap.get(m.getKey()), 0,
                                    s.getFaildNodes(), null));
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

                    SynchMessage m = new SynchMessage();
                    m.setId(me.getId());
                    m.setSynchMode(SynchMode.SYNCH_MESSAGE);
                    m.setType(SynchMessage.TYPE_OK);
                    m.setSequence((byte) 0);
                    m.setSynchType(msg.getSynchType());
                    session.write(m);
                    return;
                }

                SynchMessage m = new SynchMessage();
                m.setId(me.getId());
                m.setSynchMode(SynchMode.SYNCH_MESSAGE);
                m.setType(SynchMessage.TYPE_CHECK);
                m.setContents(responseContents);
                m.setSequence((byte) (msg.getSequence() + 1));
                m.setSynchType(msg.getSynchType());

                if (isRing && isFirstMessge) {

                    m.setExpectedIds(nodesForRingUpdate);
                }

                session.write(m);
            } else if (msg.getSynchMode() == SynchMode.SYNCH_CLUSTER) {

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

                boolean isRing = msg.getSynchType() == SynchType.RING ||
                        msg.getSynchType() == SynchType.RING_QUERIOM ||
                        msg.getSynchType() == SynchType.RING_BALANCE ||
                        msg.getSynchType() == SynchType.RING_BALANCE_QUERIOM;

                Map<String, String> ringMsgToScMap = null;

                if (isRing) {

                    ringMsgToScMap = new HashMap<String, String>();
                }

                ClusterSynchCallback clusterCallback = new ClusterSynchCallback(synchContext);

                Collection<SynchContent> responseContents = new ArrayList<SynchContent>();

                List<IMessage> messagesForRing = new ArrayList<IMessage>();

                for (Iterator<SynchContent> it = contents.iterator();
                     it.hasNext(); ) {

                    SynchContent sc = it.next();

                    byte[] m = sc.getContent();

                    if (m == null) {

                        if (sc.getVersion() > 0) {

                            Member node = null;

                            try {

                                node = synchContext.getFrNodeById(Short.parseShort(sc.getKey()));

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

                    decoded.deserialize(m);

                    SynchProtocolOutput out = new SynchProtocolOutput();

                    boolean result = clusterCallback.callBack(new MinaToISession(session), decoded, sc.getAwareIds(), out);

                    List<IMessage> responses = out.getMessages();

                    if (result) {

                        if (isRing) {

                            messagesForRing.add(decoded);
                            ringMsgToScMap.put(decoded.getKey(), sc.getKey());
                        }
                    }

                    if (responses != null) {

                        for (Iterator<IMessage> respIt = responses.iterator(); respIt.hasNext(); ) {

                            ClusterMessage response = (ClusterMessage) respIt.next();

                            if (response != null) {

                                Member node = synchContext.getFrNodeById(response.getId());

                                Set<Short> awareIds = null;

                                if (node != null) {

                                    awareIds = node.getAwareIds();
                                }

                                responseContents.add(new SynchContent(sc.getKey(), response.getVersion(), awareIds,
                                        response.serialize()));
                            }
                        }
                    }
                }

                Set<Short> nodesForRingUpdate = null;

                if (messagesForRing.size() > 0) {

                    if (log.isDebugEnabled()) {

                        log.debug("Starting to update messages {} for ring", messagesForRing);
                    }

                    ISynchCallbak callBack = this.callbak;

                    SynchType type = SynchType.RING_BALANCE;

                    if (msg.getSynchType() == SynchType.RING_QUERIOM ||
                            msg.getSynchType() == SynchType.RING_BALANCE_QUERIOM) {

                        type = SynchType.RING_BALANCE_QUERIOM;
                    }

                    ClusterSnapshot snapshot = synchContext.getSnapshot();

                    if (snapshot != null) {

                        List<Member> cluster = snapshot.getAliveCluster();

                        if (cluster.size() > 0) {

                            nodesForRingUpdate = new HashSet<Short>();

                            for (Iterator<Member> it = cluster.iterator(); it.hasNext(); ) {

                                nodesForRingUpdate.add(it.next().getId());
                            }
                        }
                    }

                    SynchFeature sf = new SynchHandler(synchContext, type)
                            .withCallBack(callBack)
                            .withEncoder(encoder)
                            .withoutCluster(msg.getId(), synchContext.myId)
                            .withModeCluster()
                            .synch(messagesForRing)
                            .get();

                    if (sf == null) {
                        //TODO is this right?
                        SynchMessage m = new SynchMessage();
                        m.setId(me.getId());
                        m.setSynchMode(SynchMode.SYNCH_CLUSTER);
                        m.setType(SynchMessage.TYPE_FAILD_RING);
                        m.setSynchType(msg.getSynchType());
                        log.warn("synch failed due to null SF");
                        session.write(m);
                        session.close(false);
                        return;
                    }

                    Iterator<IMessage> mIt = messagesForRing.iterator();

                    while (mIt.hasNext()) {

                        IMessage m = mIt.next();

                        SynchResult s = sf.get(m.getKey());
						
						/*if(s == null) {
							
							//In the case of balance, there might be some messages gets omitted
							continue;
						}*/

                        if (s.isSuccessful()) {

                            SynchProtocolOutput out = new SynchProtocolOutput();

                            Member node = synchContext.getFrNodeById(((ClusterMessage) m).getId());

                            Set<Short> awareIds = null;

                            if (node != null) {

                                awareIds = node.getAwareIds();
                            }

                            boolean result = clusterCallback.callBack(new MinaToISession(session), m, awareIds, out);

                            List<IMessage> responses = out.getMessages();

                            if (result) {

                                if (responses == null) {

                                    //means it synched successfully
                                    responseContents.add(new SynchContent(ringMsgToScMap.get(m.getKey()),
                                            m.getVersion(),
                                            awareIds, null));
                                    continue;
                                }
                            } else {

                                if (responses == null) {

                                    Set<Short> n = s.getFaildNodes();
                                    n.add(me.getId());
                                    responseContents.add(new SynchContent(ringMsgToScMap.get(m.getKey()),
                                            0,
                                            n, null));
                                    continue;
                                }
                            }

                            for (Iterator<IMessage> respIt = responses.iterator(); respIt.hasNext(); ) {

                                IMessage response = respIt.next();

                                if (response != null) {

                                    node = synchContext.getFrNodeById(((ClusterMessage) response).getId());

                                    awareIds = null;

                                    if (node != null) {

                                        awareIds = node.getAwareIds();
                                    }

                                    responseContents.add(new SynchContent(ringMsgToScMap.get(m.getKey()), response.getVersion(), awareIds,
                                            response.serialize()));
                                }
                            }
                        } else {

                            //means it was not synched successfully
                            responseContents.add(new SynchContent(ringMsgToScMap.get(m.getKey()), 0,
                                    s.getFaildNodes(), null));
                        }
                    }

                }

                if (log.isDebugEnabled()) {

                    log.debug("Response with contents {} to {}", responseContents, msg.getId());
                }

                if (responseContents.size() == 0) {

                    SynchMessage m = new SynchMessage();
                    m.setId(me.getId());
                    m.setSynchMode(SynchMode.SYNCH_CLUSTER);
                    m.setType(SynchMessage.TYPE_OK);
                    m.setSequence((byte) 0);
                    m.setSynchType(msg.getSynchType());
                    session.write(m);

                    return;
                }

                SynchMessage m = new SynchMessage();
                m.setId(me.getId());
                m.setSynchMode(SynchMode.SYNCH_CLUSTER);
                m.setType(SynchMessage.TYPE_CHECK);
                m.setContents(responseContents);
                m.setSequence((byte) (msg.getSequence() + 1));
                m.setSynchType(msg.getSynchType());

                if (isRing && isFirstMessge) {

                    m.setExpectedIds(nodesForRingUpdate);
                }

                session.write(m);

                if (log.isDebugEnabled()) {

                    log.debug("Message responded with contents {}, type {}, sequence {}", responseContents, m.getType(), m.getSequence());
                }
            }
        } else {

            SynchSession synch = (SynchSession) session.getAttribute("SynchSession");

            boolean isRing = this.synch == SynchType.RING ||
                    this.synch == SynchType.RING_QUERIOM ||
                    this.synch == SynchType.RING_BALANCE ||
                    this.synch == SynchType.RING_BALANCE_QUERIOM;

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

                log.warn("Got BAD_KEY synch responce from {} my key chain of him was:{}", peer, synch.getFrNode().getKeyChain());

                workCallback(synch, STATE_UNPROPER, link);

                session.setAttribute("planned_close");
                session.close(false);

                return;
            }

            if (msg.getSequence() > SynchMessage.SEQ_MAX) {

                log.warn("Too many conversion between peer {}", peer);

                SynchMessage responce = new SynchMessage();
                responce.setId(me.getId());
                responce.setInStartup(startupState);
                responce.setSynchMode(mode);
                responce.setType(SynchMessage.TYPE_BAD_SEQ);

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

                Set<SynchContent> responseContents = new HashSet<SynchContent>();

                Map<String, SynchContent> faildContents = new HashMap<String, SynchContent>();

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

                            if (isRing) {

                                short nodeId = 0;

                                for (int i = 0; i < ids.length; i++) {

                                    nodeId = ids[i];

                                    if (awareNodes.contains(nodeId)) {

                                        sr.addSynchedNode(nodeId);
                                        sr.removeFaildNode(nodeId);
                                    }
                                }
                            } else {

                                sr.addSynchedNode(msg.getId());
                                sr.removeFaildNode(msg.getId());
                            }

                            awareNodes.add(me.getId());

                            if (sc.getKey() != null && !sc.getKey().equals("")) {

                                synchContext.addAwareNodes(sc.getKey(), sc.getVersion(), awareNodes);
                                log.debug("Content {} synched up perfectly with version {}", sc.getKey(), sc.getVersion());
                            }

                            continue;
                        }

                        SynchResult sr = synchFeature.get(sc.getKey());

                        if (isRing) {

                            short nodeId = 0;

                            for (int i = 0; i < ids.length; i++) {

                                nodeId = ids[i];

                                if (awareNodes.contains(nodeId)) {

                                    sr.addFaildNode(nodeId);
                                    sr.removeSynchedNode(nodeId);
                                }
                            }
                        } else {

                            sr.addFaildNode(msg.getId());
                            sr.removeSynchedNode(msg.getId());
                        }

                        log.warn("Content {} faild", sc.getKey());
                        faildContents.put(sc.getKey(), this.synchContents.get(sc.getKey()));
                        continue;
                    }

                    IMessage decoder = (IMessage) this.encoder.newInstance();

                    decoder.deserialize(m);

                    SynchProtocolOutput out = new SynchProtocolOutput();

                    boolean result = callbak.callBack(new MinaToISession(session), decoder, sc.getAwareIds(), out);

                    List<IMessage> responses = out.getMessages();

                    if (!result) {

                        if (isRing) {

                            faildContents.put(sc.getKey(), this.synchContents.get(sc.getKey()));
                        }

                        if (responses != null) {

                            for (Iterator<IMessage> respIt = responses.iterator(); respIt.hasNext(); ) {

                                IMessage response = respIt.next();

                                if (response != null) {

                                    Set<Short> awareNodes = synchContext.getAwareNodes(response.getKey(), response.getVersion());

                                    responseContents.add(new SynchContent(response.getKey(), response.getVersion(),
                                            awareNodes, response.serialize()));
                                }
                            }
                        } else {

                            HashSet<Short> iFaild = new HashSet<Short>();
                            iFaild.add(me.getId());

                            Set<Short> awareNodes = msg.getExpectedIds();
                            SynchResult sr = synchFeature.get(sc.getKey());
                            if (isRing && awareNodes != null) {

                                short nodeId = 0;

                                for (int i = 0; i < ids.length; i++) {

                                    nodeId = ids[i];

                                    if (awareNodes.contains(nodeId)) {

                                        sr.addFaildNode(nodeId);
                                        sr.removeSynchedNode(nodeId);
                                    }
                                }
                            } else {

                                sr.addFaildNode(msg.getId());
                                sr.removeSynchedNode(msg.getId());
                            }

                            numberOfNull++;

                            responseContents.add(new SynchContent(sc.getKey(),
                                    0/*sc.getVersion()*/,
                                    iFaild, null));
                        }
						
						/*
						 * I think it is not right to do this here
						 * if(response == null) {
							
							synchFeature.get(sc.getKey()).addFaildNode(msg.getId());
						}*/
                    } else {

                        Set<Short> awareNodes = sc.getAwareIds();

                        if (awareNodes == null) {

                            awareNodes = new HashSet<Short>();
                        }

                        awareNodes.add(me.getId());
                        awareNodes.add(msg.getId());

                        if (sc.getVersion() > 0 && sc.getKey() != null && !sc.getKey().equals("")) {

                            synchContext.addAwareNodes(sc.getKey(), sc.getVersion(), awareNodes);
                        }

                        SynchResult r = synchFeature.get(sc.getKey());

                        if (r != null) {

                            Set<Short> expectedIds = msg.getExpectedIds();

                            if (isRing && expectedIds != null) {

                                expectedIds.addAll(awareNodes);

                                short nodeId = 0;

                                for (int i = 0; i < ids.length; i++) {

                                    nodeId = ids[i];

                                    if (expectedIds.contains(nodeId)) {

                                        r.removeFaildNode(nodeId);
                                        r.addSynchedNode(nodeId);
                                    }
                                }
                            } else {

                                short nodeId = 0;

                                for (int i = 0; i < ids.length; i++) {

                                    nodeId = ids[i];

                                    if (awareNodes.contains(nodeId)) {

                                        r.removeFaildNode(nodeId);
                                        r.addSynchedNode(nodeId);
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

                            if (isRing) {

                                short nodeId = 0;

                                for (int i = 0; i < ids.length; i++) {

                                    nodeId = ids[i];

                                    if (awareNodes.contains(nodeId)) {

                                        sr.addSynchedNode(nodeId);
                                        sr.removeFaildNode(nodeId);
                                    }
                                }
                            } else {

                                sr.addSynchedNode(msg.getId());
                                sr.removeFaildNode(msg.getId());
                            }

                            Member node = null;

                            try {

                                node = synchContext.getFrNodeById(Short.parseShort(sc.getKey()));

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

                        if (isRing) {

                            short nodeId = 0;

                            for (int i = 0; i < ids.length; i++) {

                                nodeId = ids[i];

                                if (awareNodes.contains(nodeId)) {

                                    sr.addFaildNode(nodeId);
                                    sr.removeSynchedNode(nodeId);
                                }
                            }
                        } else {

                            sr.addFaildNode(msg.getId());
                            sr.removeSynchedNode(msg.getId());
                        }

                        log.warn("Content {} faild", sc.getKey());
                        faildContents.put(sc.getKey(), this.synchContents.get(sc.getKey()));
                        continue;
                    }

                    ClusterMessage decoded = new ClusterMessage();

                    decoded.deserialize(m);

                    SynchProtocolOutput out = new SynchProtocolOutput();

                    boolean result = clusterCallback.callBack(new MinaToISession(session), decoded, sc.getAwareIds(), out);

                    List<IMessage> responses = out.getMessages();

                    if (!result) {

                        if (isRing) {

                            faildContents.put(sc.getKey(), this.synchContents.get(sc.getKey()));
                        }

                        if (responses != null) {

                            for (Iterator<IMessage> respIt = responses.iterator(); respIt.hasNext(); ) {

                                ClusterMessage response = (ClusterMessage) respIt.next();

                                if (response != null) {

                                    Member node = synchContext.getFrNodeById(response.getId());

                                    Set<Short> awareIds = null;

                                    if (node != null) {

                                        awareIds = node.getAwareIds();
                                    }

                                    responseContents.add(new SynchContent(sc.getKey(), response.getVersion(),
                                            awareIds, response.serialize()));
                                }
                            }
                        } else {

                            HashSet<Short> iFaild = new HashSet<Short>();
                            iFaild.add(me.getId());

                            Set<Short> awareNodes = msg.getExpectedIds();
                            SynchResult sr = synchFeature.get(sc.getKey());
                            if (isRing && awareNodes != null) {

                                short nodeId = 0;

                                for (int i = 0; i < ids.length; i++) {

                                    nodeId = ids[i];

                                    if (awareNodes.contains(nodeId)) {

                                        sr.addFaildNode(nodeId);
                                        sr.removeSynchedNode(nodeId);
                                    }
                                }
                            } else {

                                sr.addFaildNode(msg.getId());
                                sr.removeSynchedNode(msg.getId());
                            }

                            numberOfNull++;

                            responseContents.add(new SynchContent(sc.getKey(),
                                    0/*sc.getVersion()*/,
                                    iFaild, null));
                        }
						
						/*
						 * I think it is not right to do this here
						 * if(response == null) {
							
							synchFeature.get(sc.getKey()).addFaildNode(msg.getId());
						}*/
                    } else {

                        Member node = synchContext.getFrNodeById(decoded.getId());

                        Set<Short> awareIds = sc.getAwareIds();

                        if (node != null) {

                            awareIds = node.getAwareIds();
                        } else {

                            log.warn("how come node is null!!");
                        }

                        Set<Short> awareNodes = awareIds;

                        SynchResult r = synchFeature.get(sc.getKey());

                        if (r != null) {

                            Set<Short> expectedIds = msg.getExpectedIds();

                            if (isRing && expectedIds != null) {

                                expectedIds.addAll(awareNodes);

                                short nodeId = 0;

                                for (int i = 0; i < ids.length; i++) {

                                    nodeId = ids[i];

                                    if (expectedIds.contains(nodeId)) {

                                        r.removeFaildNode(nodeId);
                                        r.addSynchedNode(nodeId);
                                    }
                                }
                            } else {

                                short nodeId = 0;

                                for (int i = 0; i < ids.length; i++) {

                                    nodeId = ids[i];

                                    if (awareNodes.contains(nodeId)) {

                                        r.removeFaildNode(nodeId);
                                        r.addSynchedNode(nodeId);
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

                        if (responses != null) {

                            for (Iterator<IMessage> respIt = responses.iterator(); respIt.hasNext(); ) {

                                ClusterMessage response = (ClusterMessage) respIt.next();

                                node = synchContext.getFrNodeById(response.getId());

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

                SynchMessage m = new SynchMessage();
                m.setId(me.getId());
                m.setSynchMode(SynchMode.SYNCH_CLUSTER);
                m.setSynchType(this.synch);
                m.setType(SynchMessage.TYPE_CHECK);
                m.setContents(responseContents);
                m.setSequence((byte) (msg.getSequence() + 1));

                session.write(m);

                if (log.isDebugEnabled()) {

                    log.debug("message responded {}, type {}, sequence {}", responseContents, m.getType(), m.getSequence());
                }
            }
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

            SynchSession synch = (SynchSession) session.getAttribute("SynchSession");

            workCallback(synch, STATE_WORK_FAILED, link);
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

                    Member e = session.getFrNode();

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
                        addFaildNodeToSynchFeatures(session.getFrNode().getId());
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

                    sessions
                            .get(currentSocket)
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
                        addFaildNodeToSynchFeatures(session.getFrNode().getId());

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

                        sessions
                                .get(currentSocket)
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

                    Member e = session.getFrNode();

                    if (e != null && e.getState() == Member.STATE_DEL) {

                        log.warn("Setting edge {} as DOWN", e.getId());

                        synchContext.synchronizedStateChange(e, Member.STATE_DWN);

                        numberOfTrieds++;
                        addFaildNodeToSynchFeatures(e.getId());

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
                            addFaildNodeToSynchFeatures(session.getFrNode().getId());
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
                            addFaildNodeToSynchFeatures(session.getFrNode().getId());
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

            for (Iterator<Entry<String, SynchResult>> it = synchFeature.entrySet().iterator(); it.hasNext(); ) {
                Entry<String, SynchResult> eit = it.next();

                SynchResult s = eit.getValue();

                if (isQueriom) {

                    if (synchingNodesLength == 0) {

                        s.setSuccessful(false);
                    } else if (s.getFaildNodes().size() * 2 < synchingNodesLength) {

                        s.setSuccessful(true);
                    } else if (s.getSynchedNodes().size() * 2 > synchingNodesLength) {

                        s.setSuccessful(true);
                    } else {

                        s.setSuccessful(false);
                    }
                } else {

                    if (synchingNodesLength == 0) {

                        s.setSuccessful(false);
                    } else if (s.getSynchedNodes().size() > 0) {

                        s.setSuccessful(true);
                    } else if (synch == SynchType.UNICAST_ONE_OF
                            && s.getFaildNodes().size() > 0) {

                        s.setSuccessful(false);
                    } else if (s.getFaildNodes().size() < synchingNodesLength) {

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

            sessions
                    .get(currentSocket)
                    .sendMsg(message);
        }
    }

    private void addFaildNodeToSynchFeatures(Short id) {

        for (Iterator<Entry<String, SynchResult>> it = synchFeature.entrySet().iterator(); it.hasNext(); ) {

            SynchResult s = it.next().getValue();
            s.addFaildNode(id);
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
            Map<Short, List<IMessage>> e2Mag = new HashMap<Short, List<IMessage>>();
            if (ids == null || ids.length == 0) {
                cluster = snapshot.getAliveCluster();
                if (cluster.size() > 0) {
                    ids = new short[cluster.size()];
                }
                int i = 0;
                for (Iterator<Member> eIt = cluster.iterator();
                     eIt.hasNext(); ) {
                    Member e = eIt.next();
                    ids[i++] = e.getId();
                    List<IMessage> nodeMsgz = new ArrayList<IMessage>();
                    for (Iterator<IMessage> it = msg.iterator();
                         it.hasNext(); ) {
                        IMessage m = it.next();
                        Set<Short> nodes = null;
                        if (this.mode == SynchMode.SYNCH_MESSAGE) {
                            nodes = synchContext.getAwareNodes(m.getKey(), m.getVersion());
                            if (nodes == null || nodes.size() == 0) {
                                nodes = new HashSet<Short>();
                                nodes.add(synchContext.myId);
                                synchContext.addAwareNodes(m.getKey(), m.getVersion(), nodes);
                            }
                        } else {
                            ClusterMessage cm = (ClusterMessage) m;
                            Member node = synchContext.getFrNodeById(cm.getId());
                            nodes = node == null ? null : node.getAwareIds();
                        }

                        if (nodes != null && nodes.contains(e.getId())) {
                            continue;
                        }
                        nodeMsgz.add(m);
                    }

                    if (nodeMsgz.size() > 0) {
                        e2Mag.put(e.getId(), nodeMsgz);
                    }
                }
            } else {

                if (synchModel) {

                    for (int i = 0; i < ids.length; i++) {

                        Member e = snapshot.getById(ids[i], 2);

                        if (e != null) {

                            List<IMessage> nodeMsgz = new ArrayList<IMessage>();

                            for (Iterator<IMessage> it = msg.iterator();
                                 it.hasNext(); ) {

                                IMessage m = it.next();

                                Set<Short> nodes = null;

                                if (this.mode == SynchMode.SYNCH_MESSAGE) {

                                    nodes = synchContext.getAwareNodes(m.getKey(), m.getVersion());

                                    if (nodes == null || nodes.size() == 0) {

                                        nodes = new HashSet<Short>();
                                        nodes.add(synchContext.myId);
                                        synchContext.addAwareNodes(m.getKey(), m.getVersion(), nodes);
                                    }
                                } else {

                                    ClusterMessage cm = (ClusterMessage) m;
                                    Member node = synchContext.getFrNodeById(cm.getId());

                                    nodes = node == null ? null : node.getAwareIds();
                                }

                                if (nodes != null && nodes.contains(e.getId())) {

                                    continue;
                                }

                                nodeMsgz.add(m);
                            }

                            if (nodeMsgz.size() > 0) {

                                e2Mag.put(e.getId(), nodeMsgz);
                            }
                        }
                    }
                } else {

                    cluster = snapshot.getAliveCluster();

                    ArrayList<Short> idss = new ArrayList<Short>();
                    boolean found = false;

                    for (Iterator<Member> it = cluster.iterator();
                         it.hasNext(); ) {

                        Member e = it.next();
                        found = false;

                        for (int i = 0; i < ids.length; i++) {

                            if (ids[i] == e.getId()) {

                                found = true;
                                break;
                            }
                        }

                        if (found) {

                            continue;
                        }

                        idss.add(e.getId());
                        List<IMessage> nodeMsgz = new ArrayList<IMessage>();

                        for (Iterator<IMessage> mit = msg.iterator();
                             mit.hasNext(); ) {

                            IMessage m = mit.next();

                            Set<Short> nodes = null;

                            if (this.mode == SynchMode.SYNCH_MESSAGE) {

                                nodes = synchContext.getAwareNodes(m.getKey(), m.getVersion());

                                if (nodes == null || nodes.size() == 0) {

                                    nodes = new HashSet<Short>();
                                    nodes.add(synchContext.myId);
                                    synchContext.addAwareNodes(m.getKey(), m.getVersion(), nodes);
                                }
                            } else {

                                ClusterMessage cm = (ClusterMessage) m;
                                Member node = synchContext.getFrNodeById(cm.getId());

                                nodes = node == null ? null : node.getAwareIds();
                            }

                            if (nodes != null && nodes.contains(e.getId())) {

                                continue;
                            }

                            nodeMsgz.add(m);
                        }

                        if (nodeMsgz.size() > 0) {

                            e2Mag.put(e.getId(), nodeMsgz);
                        }
                    }

                    this.ids = ArrayUtils.toPrimitive(idss.toArray(new Short[0]));
                }
            }

            if (e2Mag.size() == 0) {

                nonasyncLock = null;

                for (Iterator<IMessage> xIt = msg.iterator(); xIt.hasNext(); ) {

                    IMessage m = xIt.next();
                    SynchResult sr = new SynchResult();
                    sr.setSuccessful(true);
                    this.synchFeature.put(m.getKey(), sr);
                }

                return;
            }

            for (Iterator<Entry<Short, List<IMessage>>> fIt = e2Mag.entrySet().iterator();
                 fIt.hasNext(); ) {

                Entry<Short, List<IMessage>> ee = fIt.next();
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

            Set<Member> edges = new HashSet<Member>();

            if (ids == null || ids.length == 0) {

                cluster = snapshot.getAliveCluster();

                if (cluster.size() > 0) {

                    ids = new short[cluster.size()];
                }

                int i = 0;

                for (Iterator<Member> it = cluster.iterator();
                     it.hasNext(); ) {

                    Member e = it.next();
                    ids[i++] = e.getId();
                    edges.add(e);
                }
            } else {

                if (synchModel) {

                    for (int i = 0; i < ids.length; i++) {

                        Member e = snapshot.getById(ids[i], 2);

                        if (e != null) {

                            edges.add(e);
                        }
                    }
                } else {

                    cluster = snapshot.getAliveCluster();

                    ArrayList<Short> idss = new ArrayList<Short>();
                    boolean found = false;

                    for (Iterator<Member> it = cluster.iterator();
                         it.hasNext(); ) {

                        Member e = it.next();
                        found = false;

                        for (int i = 0; i < ids.length; i++) {

                            if (ids[i] == e.getId()) {

                                found = true;
                                break;
                            }
                        }

                        if (found) {

                            continue;
                        }

                        idss.add(e.getId());
                        edges.add(e);
                    }

                    this.ids = ArrayUtils.toPrimitive(idss.toArray(new Short[0]));
                }
            }

            if (edges.size() > 0) {

                Map<IMessage, Set<Short>> m2E = new HashMap<IMessage, Set<Short>>();

                for (Iterator<IMessage> it = msg.iterator(); it.hasNext(); ) {

                    IMessage m = it.next();

                    Set<Short> eges4M = new HashSet<Short>();

                    for (Iterator<Member> eIt = edges.iterator(); eIt.hasNext(); ) {

                        Member e = eIt.next();

                        Set<Short> nodes = null;

                        if (this.mode == SynchMode.SYNCH_MESSAGE) {

                            nodes = synchContext.getAwareNodes(m.getKey(), m.getVersion());

                            if (nodes == null || nodes.size() == 0) {

                                nodes = new HashSet<Short>();
                                nodes.add(synchContext.myId);
                                synchContext.addAwareNodes(m.getKey(), m.getVersion(), nodes);
                            }
                        } else {

                            ClusterMessage cm = (ClusterMessage) m;
                            Member node = synchContext.getFrNodeById(cm.getId());

                            nodes = node == null ? null : node.getAwareIds();
                        }

                        if (nodes != null && nodes.contains(e.getId())) {

                            continue;
                        }

                        eges4M.add(e.getId());
                    }

                    if (log.isDebugEnabled()) {

                        log.debug("Created edge groups {} for message {}", eges4M, m.getKey());
                    }

                    if (eges4M.size() > 0) {

                        m2E.put(m, eges4M);
                    } else {

                        SynchResult sr = new SynchResult();
                        sr.setSuccessful(true);
                        this.synchFeature.put(m.getKey(), sr);
                    }
                }

                if (m2E.size() > 0) {

                    Set<String> proccessedMessages = new HashSet<String>();

                    Map<List<IMessage>, Set<Short>> finalMessages2Nodes = new HashMap<List<IMessage>, Set<Short>>();

                    for (Iterator<Entry<IMessage, Set<Short>>> it = m2E.entrySet().iterator();
                         it.hasNext(); ) {

                        Entry<IMessage, Set<Short>> ee = it.next();

                        if (ee.getValue().size() == 0 ||
                                proccessedMessages.contains(ee.getKey().getKey())) {

                            continue;
                        }

                        List<IMessage> mList = new ArrayList<IMessage>();

                        proccessedMessages.add(ee.getKey().getKey());
                        mList.add(ee.getKey());

                        for (Iterator<Entry<IMessage, Set<Short>>> innerIt = m2E.entrySet().iterator();
                             innerIt.hasNext(); ) {

                            Entry<IMessage, Set<Short>> eeInner = innerIt.next();

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

                        finalMessages2Nodes.put(mList, ee.getValue());
                    }

                    if (finalMessages2Nodes.size() > 0) {

                        for (Iterator<Entry<List<IMessage>, Set<Short>>> it = finalMessages2Nodes
                                .entrySet().iterator();
                             it.hasNext(); ) {

                            Entry<List<IMessage>, Set<Short>> ee = it.next();

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

            for (Iterator<IMessage> xIt = msg.iterator(); xIt.hasNext(); ) {

                IMessage m = xIt.next();
                SynchResult sr = new SynchResult();
                sr.setSuccessful(true);
                this.synchFeature.put(m.getKey(), sr);
            }

            nonasyncLock = null;
        }

        return;
    }

    void fixMonitorLastModified() {

    }
}
