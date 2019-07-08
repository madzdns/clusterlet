package com.github.madzdns.clusterlet;

import com.github.madzdns.clusterlet.codec.ClusterMessage;
import com.github.madzdns.clusterlet.codec.IMessage;
import com.github.madzdns.clusterlet.codec.SyncMessage;
import com.github.madzdns.clusterlet.codec.SyncMessage.SyncMode;
import com.github.madzdns.clusterlet.config.SyncConfig;
import lombok.extern.slf4j.Slf4j;

import java.util.*;

@Slf4j
public class SyncContext {
    private IClusterStore clusterStore;
    private IMessageStore messageStore;
    private SyncConfig config;

    short myId;
    boolean inStartup = true;
    private volatile long virtualLastModified = new Date().getTime();
    private volatile ClusterSnapshot snapshot = null;

    /**
     * Creates a new SyncContext with specified parametes.
     * In order to use Clusterlet to create a synchronising layout
     * for your cluster, you must create a context using this
     * constructor. There must be only one context per each local node id
     *
     * @param myId   is the ID of the local node. Every node in cluster must has a unique ID and it must be greater than 0
     * @param config @see SyncConfig
     */
    public SyncContext(short myId, SyncConfig config) throws Exception {
        this.myId = myId;
        this.config = config;
        /*System.setProperty("net.sf.ehcache.enableShutdownHook","true");
        clusterStore = new EhCacheClusterStore(config.getClusterStorageConfigPath());
        messageStore = new EhCacheMessageStore(config.getClusterStorageConfigPath());*/
        clusterStore = new JcsCacheClusterStore(config.getClusterStorageConfigPath());
        messageStore = new JcsCacheMessageStore(config.getClusterStorageConfigPath());
        Member member = getMemberById(myId);
        if (member == null) {
            final Set<Short> awareIds = new HashSet<>();
            member = new Member(myId, null,
                    config.getTrustStorePath() != null &&
                            config.getKeyStorePath() != null, true, "",
                    new Date().getTime(),
                    awareIds, Member.STATE_VLD);
            updateMember(member);
        }
    }

    /**
     * Creates a sync Handler instance suitable for receiving messages from other nodes
     * in the cluster. So it should be created one per context
     *
     * @return a server instance of @see SyncHandler
     */
    public SyncHandler make() {
        SyncHandler sync = new SyncHandler(this);
        sync.me = this.getMyInfo();
        return sync;
    }

    /**
     * Every request for syncing messages are created using this method.
     * After creating a SyncHandler using this method, messages can be sent
     * to cluster
     *
     * @param type is of type @see SyncType
     * @return a client instance of @see SyncHandler
     */
    public SyncHandler make(SyncType type) {
        SyncHandler s = new SyncHandler(this, type);
        inStartup = false;
        return s;
    }

    Set<Short> getAwareNodes(String key, long version) {
        return messageStore.getAwareNodes(key, version);
    }

    void addAwareNodes(String key, long version, Set<Short> awareNodes) {
        messageStore.updateAwareNodes(key, version, awareNodes);
    }

    public Member getMemberById(short id) {
        return clusterStore.get(id);
    }

    void updateMember(Member member) {
        if (member == null) {
            return;
        }
        member.addAwareId(myId);
        /*
         * Check if received edge is aware of himself (the target edge received this update before).
         * If not, we should use its previous key chain to inform him
         */
        if (!member.getAwareIds().contains(member.getId())) {
            Member mem = null;
            mem = clusterStore.get(member.getId());
            if (mem != null) {
                if (!mem.getKey().equals(member.getKey())) {
                    member.addKeyChain(mem.getKeyChain());
                }
            }
        }
        virtualLastModified = new Date().getTime();
        clusterStore.update(member);
        invalidateMonitor();
    }

    /**
     * Starts to sync a new Cluster node instance or update
     * a node with the cluster and returns result.
     * It makes Clusterlet to use default sync type to synchronise
     * node
     *
     * @param node of type @see Member
     * @return true if it was synced with cluster
     */
    public boolean syncCluster(Member node) {
        return syncCluster(node, SyncType.UNICAST_BALANCE);
    }

    /**
     * Starts to sync a new Cluster node instance or update
     * a node with the cluster and returns result.
     * Using withType you can force Clusterlet a specified type to use
     * as synchronising method
     *
     * @param node     node of type @see Member
     * @param withType of type @SyncType
     * @return true if it was synced with cluster
     */
    public boolean syncCluster(Member node, SyncType withType) {
        if (node == null) {
            return false;
        }
        updateMember(node);
        SyncHandler handler = make(withType)
                .withoutCluster(myId)
                .withCallBack(new ClusterSyncCallback(this));
        handler.mode = SyncMode.SYNC_CLUSTER;
        final List<IMessage> messages = new ArrayList<>();
        ClusterSnapshot snapshot = getSnapshot();
        if (snapshot != null) {
            for (Member n : snapshot.cluster) {
                ClusterMessage msg = new ClusterMessage(n.getId(),
                        n.isUseSsl(), n.isAuthByKey(), n.getKey(),
                        n.getLastModified(), n.getSyncAddresses(),
                        n.isValid() ? SyncMessage.COMMAND_TAKE_THis :
                                SyncMessage.COMMAND_DEL_THis);
                messages.add(msg);
            }
        }
        SyncFeature feature = handler.sync(messages).get();
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
    public boolean isFullySynced(String key) {
        return false;
    }

    /**
     * Check to see if a key is uptodate with quorum number of nodes.
     * Right now it is not implemented because I don't see
     * any usecase for that
     *
     * @param key
     * @return false
     */
    public boolean isQuorumSynced(String key) {
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
        if (snapshot != null) {
            return snapshot;
        }

        final ClusterSnapshot tmpMonitor = new ClusterSnapshot();
        clusterStore.iterator(node -> {
            if (node.isValid()) {
                tmpMonitor.validClusterIDs.add(node.getId());
                tmpMonitor.validCluster.add(node);
                tmpMonitor.aliveCluster.add(node);
            } else if (!node.isDown()) {
                tmpMonitor.aliveCluster.add(node);
                if (node.getId() != myId) {
                    tmpMonitor.inValidClusterIDs.add(node.getId());
                }
            } else if (node.getId() != myId) {
                tmpMonitor.inValidClusterIDs.add(node.getId());
            }
            tmpMonitor.cluster.add(node);
            tmpMonitor.idClusterMap.put(node.getId(), node);
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
        if (snapshot == null) {
            return;
        }
        //this invalidates previous validCluster, So they need to calculate new one
        snapshot.invalidateMonitor();
        snapshot = null;
    }

    void synchronizedStateChange(Member node, byte state) {
        node.setState(state);
        invalidateMonitor();
        virtualLastModified = new Date().getTime();
    }

    /**
     * @return a @see Member of local node
     */
    public Member getMyInfo() {
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
        Member node = getMemberById(id);
        if (node != null && key != null) {
            if (!node.isValid()) {
                throw new IllegalStateException("Node is disabld");
            }

            /*
             * I don't think its possible e.getKey() returns null
             */
            if (node.getKey().equals(key)) {
                return true;
            }
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
    public SyncConfig getConfig() {
        return config;
    }
}
