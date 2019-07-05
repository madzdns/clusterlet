package com.github.madzdns.clusterlet;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.madzdns.clusterlet.codec.ClusterMessage;
import com.github.madzdns.clusterlet.codec.IMessage;
import com.github.madzdns.clusterlet.codec.SynchMessage;
import com.github.madzdns.clusterlet.codec.SynchMessage.SynchMode;
import com.github.madzdns.clusterlet.config.SynchConfig;

public class SynchContext {

    private static Logger log = LoggerFactory.getLogger(SynchContext.class);
    private IClusterStore clusterStore;
    private IMessageStore messageStore;
    private SynchConfig config;

    short myId;
    boolean inStartup = true;
    private volatile long virtualLastModified = new Date().getTime();
    private volatile ClusterSnapshot snapshot = null;

    /**
     * Creates a new SynchContext with specified parametes.
     * In order to use Frsynch to create a synchronising layout
     * for your cluster, you must create a context using this
     * constructor. There must be only one context per each local node id
     *
     * @param myId   is the ID of the local node. Every node in cluster must has a unique ID and it must be greater than 0
     * @param config @see SynchConfig
     */
    public SynchContext(short myId, SynchConfig config) throws Exception {

        this.myId = myId;
        this.config = config;
			/*System.setProperty("net.sf.ehcache.enableShutdownHook","true");
			clusterStore = new EhCacheClusterStore(config.getClusterStorageConfigPath());
			messageStore = new EhCacheMessageStore(config.getClusterStorageConfigPath());*/
        clusterStore = new JcsCacheClusterStore(config.getClusterStorageConfigPath());
        messageStore = new JcsCacheMessageStore(config.getClusterStorageConfigPath());
        ClusterNode node = getFrNodeById(myId);
        if (node == null) {
            final Set<Short> awareIds = new HashSet<Short>();
            node = new ClusterNode(myId, null, null,
                    true, true, "", new Date().getTime(),
                    awareIds, ClusterNode.STATE_VLD,
                    ClusterNode.MONITOR_DELAY,
                    ClusterNode.MONITOR_INTERVAL,
                    ClusterNode.REPORT_DELAY,
                    ClusterNode.REPORT_INTERVAL);
            updateFrNode(node);
        }
    }

    /**
     * Creates a synchhandler instance suitable for receiving messages from other nodes
     * in the cluster. So it should be created one per context
     *
     * @return a server instance of @see SynchHandler
     */
    public SynchHandler make() {
        SynchHandler synch = new SynchHandler(this);
        synch.me = this.getMyInfo();
        return synch;
    }

    /**
     * Every request for synching messages are created using this method.
     * After creating a SynchHandler using this method, messges can be sent
     * to cluster
     *
     * @param type is of type @see SynchType
     * @return a client instance of @see SynchHandler
     */
    public SynchHandler make(SynchType type) {

        SynchHandler s = new SynchHandler(this, type);
        inStartup = false;
        return s;
    }

    Set<Short> getAwareNodes(String key, long version) {

        return messageStore.getAwareNodes(key, version);
    }

    void addAwareNodes(String key, long version, Set<Short> awareNodes) {

        messageStore.updateAwareNodes(key, version, awareNodes);
    }

    public ClusterNode getFrNodeById(short id) {

        return clusterStore.get(id);
    }

    void updateFrNode(ClusterNode node) {

        if (node == null)

            return;

        node.addAwareId(myId);

        /*
         * Check if received edge is aware of himself (the target edge received this update before).
         * If not, we should use its previous key chain to inform him
         */
        if (!node.getAwareIds().contains(node.getId())) {

            ClusterNode e = null;

            e = clusterStore.get(node.getId());

            if (e != null) {

                if (!e.getKey().equals(node.getKey())) {

                    node.addKeyChain(e.getKeyChain());
                }
            }
        }

        virtualLastModified = new Date().getTime();

        clusterStore.update(node);
        invalidateMonitor();
    }

    /**
     * Starts to synch a new Cluster node instance or update
     * a node with the cluster and returns result.
     * It makes Frsynch to use default synch type to synchronise
     * node
     *
     * @param node of type @see ClusterNode
     * @return true if it was synched with cluster
     */
    public boolean synchCluster(ClusterNode node) {

        return synchCluster(node, SynchType.UNICAST_BALANCE);
    }

    /**
     * Starts to synch a new Cluster node instance or update
     * a node with the cluster and returns result.
     * Using withType you can force Frsynch a specified type to use
     * as synchronising method
     *
     * @param node     node of type @see ClusterNode
     * @param withType of type @SynchType
     * @return true if it was synched with cluster
     */
    public boolean synchCluster(ClusterNode node, SynchType withType) {

        if (node == null) {

            return false;
        }

        updateFrNode(node);

        SynchHandler handler = make(withType)
                .withoutCluster(myId)
                .withCallBack(new ClusterSynchCallback(this));
        handler.mode = SynchMode.SYNCH_CLUSTER;

        final List<IMessage> messages = new ArrayList<IMessage>();

        ClusterSnapshot snapshot = getSnapshot();

        if (snapshot != null) {

            for (Iterator<ClusterNode> it = snapshot.cluster.iterator(); it.hasNext(); ) {

                ClusterNode n = it.next();

                ClusterMessage msg = new ClusterMessage(n.getId(),
                        n.isUseSsl(), n.isAuthByKey(), n.getKey(),
                        n.getLastModified(), n.getSynchAddresses(),
                        n.getBackendAddresses(),
                        n.isValid() ? SynchMessage.COMMAND_TAKE_THis :
                                SynchMessage.COMMAND_DEL_THis,
                        n.getMonitorDelay(),
                        n.getMonitorInterval(),
                        n.getReportDelay(),
                        n.getReportInterval());

                messages.add(msg);
            }
        }

        SynchFeature feature = handler.synch(messages).get();

        if (feature == null) {

            if (node.getId() == myId) {


                inStartup = false;
                return true;
            }

            return false;
        }

        if (feature.get(String.valueOf(node.getId())).isSuccessful()) {

            inStartup = false;
            return true;
        }

        return false;
    }

    /**
     * Check to see if a key is uptodate with cluster
     * Right now it is not implemented becase I don't see
     * any usecase for that
     *
     * @param key
     * @return false
     */
    public boolean isFullySynched(String key) {

        return false;
    }

    /**
     * Check to see if a key is uptodate with querium number of nodes.
     * Right now it is not implemented becase I don't see
     * any usecase for that
     *
     * @param key
     * @return false
     */
    public boolean isQueriumSynched(String key) {

        return false;
    }

    /**
     * @return a snapshot of current node in the cluster
     */
    public ClusterSnapshot getSnapshot() {

        /*
         * Only where validCluster is changed
         * is in updateClusterWlock that is a WLocked method, so it
         * is completely safe doing this here
         */

        if (snapshot != null)

            return snapshot;

        final ClusterSnapshot tmpMonitor = new ClusterSnapshot();

        clusterStore.iterator(new IClusterStoreIteratorCallback() {

            @Override
            public void next(ClusterNode node) {

                if (node.isValid()) {

                    tmpMonitor.validClusterIDs.add(node.getId());

                    tmpMonitor.validCluster.add(node);

                    tmpMonitor.aliveCluster.add(node);
                } else if (!node.isDown()) {

                    tmpMonitor.aliveCluster.add(node);

                    if (node.getId() != myId)

                        tmpMonitor.inValidClusterIDs.add(node.getId());
                } else if (node.getId() != myId) {

                    tmpMonitor.inValidClusterIDs.add(node.getId());
                }

                tmpMonitor.cluster.add(node);

                tmpMonitor.idClusterMap.put(node.getId(), node);
            }
        });

        /*
         * No need to worry about if lists inside
         * tmpMonitor have no elements. Because even if
         * clusterMeta has no elements, once it gets an element,
         * invalidates clusterMonitor
         */

        return snapshot = tmpMonitor;
    }

    private void invalidateMonitor() {

        if (snapshot == null)

            return;

        //this invalidates previous validCluster, So they need to calculate new one

        snapshot.invalidateMonitor();

        snapshot = null;
    }

    void synchronizedStateChange(ClusterNode node, byte state) {

        node.setState(state);
        invalidateMonitor();
        virtualLastModified = new Date().getTime();
    }

    /**
     * @return a @see ClusterNode of local node
     */
    public ClusterNode getMyInfo() {

        return clusterStore.get(myId);
    }

    /**
     * Resets local node's password with the new one. Other nodes will not be updated using this method so
     * it should be called only after you brought up the node for the first time.
     * For most usecases, id parameter should be local node's id
     *
     * @param id  of the node you want to reset its password
     * @param key new key
     * @return true if it was ok
     */
    public boolean resetFrNodeKeyById(short id, String key) {

        ClusterNode node = getFrNodeById(id);

        if (node != null && key != null) {

            if (!node.isValid()) {

                throw new IllegalStateException("Node is disabld");
            }

            /*
             * I don't think its possible e.getKey() returns null
             */
            if (node.getKey().equals(key))

                return true;

            node.resetKey(key);

            clusterStore.update(node);
        }

        return false;
    }

    boolean isInStartup() {

        return this.inStartup;
    }

    /**
     * @return a timestamp of last time cluster was modified
     */
    public long getClusterLastModified() {

        return virtualLastModified;
    }

    void setVirtualLastModified(long virtualLastModified) {

        this.virtualLastModified = virtualLastModified;
    }

    /**
     * @return config of this context
     */
    public SynchConfig getConfig() {

        return config;
    }

}
