package com.github.madzdns.clusterlet;

import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.net.InetAddress;
import java.security.MessageDigest;
import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

@Slf4j
public class Member implements INode, Externalizable {
    public static class ClusterAddress extends StorableSocketAddress {

        public ClusterAddress() {
        }

        public ClusterAddress(InetAddress addr, int port) {
            super(addr, port);
        }

        public ClusterAddress(String hostname, int port) {
            super(hostname, port);
        }
    }

    private ReentrantReadWriteLock idsLock = new ReentrantReadWriteLock();

    private ReentrantReadWriteLock keysLock = new ReentrantReadWriteLock();

    public final static byte STATE_DEL = 0;
    public final static byte STATE_VLD = 1;//valid
    public final static byte STATE_DWN = 2;//down

    public final static byte SYNCHED = 0;

    public final static byte NOT_SYNCHED = -1;

    public final static short MONITOR_INTERVAL = 60;

    public final static short MONITOR_DELAY = 10;

    public final static short REPORT_INTERVAL = 0;

    public final static short REPORT_DELAY = 0;

    private Set<ClusterAddress> synchAddresses;

    /*
     * This field is only used to determine changes in in Edge class implementation
     * and has nothing to do with the validating of received edge informations
     */
    private byte version = 0;

    private short id;

    private boolean useSsl = true;

    private boolean authByKey = true;

    private LinkedList<String> keyChain;

    private volatile long lastModified = 0;

    private Set<Short> awareIds = null;

    private byte state;

    private String name = "";

    /***
     * This determines if a res has been scheduled or not.
     * -1 means not scheduled.
     * 0 means scheduled
     * any number greater than 0 means this is sent to that id
     * but has not got receipt as scheduled
     */
    private short schedule = NOT_SYNCHED;

    private short monitorInterval = MONITOR_INTERVAL;

    private short monitorDelay = MONITOR_DELAY;

    private short reportInterval = 0;

    private short reportDelay = 0;

    private int currentSocketIndex = 0;

    public Member() {
    }

    public Member(final short id,
                  final Set<ClusterAddress> synchAddresses,
                  final boolean useSsl,
                  final boolean authByKey,
                  final String key,
                  final long lastModified,
                  final Set<Short> awareIds,
                  final byte state) {
        this.id = id;
        this.synchAddresses = synchAddresses;
        this.useSsl = useSsl;
        this.authByKey = authByKey;
        //put new key in the keychain
        this.keyChain = new LinkedList<String>();
        this.keyChain.add(key);
        this.awareIds = awareIds;
        this.state = state;

        this.schedule = NOT_SYNCHED;
        this.lastModified = lastModified;
        this.monitorDelay = monitorDelay;
        this.monitorInterval = monitorInterval;
        this.reportDelay = reportDelay;
        this.reportInterval = reportInterval;

        this.name = createName(id);
    }

    private String createName(short id) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            String text = new StringBuilder("Node-").append(id * 256).toString();
            md.update(text.getBytes("UTF-8")); // Change this to "UTF-16" if needed
            byte[] digest = md.digest();
            return org.apache.commons.codec.binary.Hex.encodeHexString(digest);
        } catch (Exception e) {
            log.error("", e);
        }
        return "";
    }

    public SynchSession createSynchSession(SynchHandler handler) {
        return new SynchSession(this, handler);
    }

    /*
     * TODO here I don't create a Set, so be careful, no one should alter this Set
     * without proper care
     */
    public Set<ClusterAddress> getSynchAddresses() {
        return synchAddresses;
    }

    protected void setSynchAddresses(Set<ClusterAddress> synchAddresses) {
        this.synchAddresses = synchAddresses;
    }

    @Override
    public short getId() {
        return id;
    }

    public boolean isUseSsl() {
        return useSsl;
    }

    protected void setUseSsl(boolean useSsl) {
        this.useSsl = useSsl;
    }

    public boolean isAuthByKey() {
        return authByKey;
    }

    protected void setAuthenticateByKey(boolean authByKey) {
        this.authByKey = authByKey;
    }

    public String getKey() throws NoSuchElementException {
        try {
            keysLock.readLock().lock();
            return keyChain.getFirst();
        } catch (Exception e) {
            throw e;
        } finally {
            keysLock.readLock().unlock();
        }
    }

    public List<String> getKeyChain() {
        try {
            keysLock.readLock().lock();
            return keyChain;
        } finally {
            keysLock.readLock().unlock();
        }
    }

    public void addKey(String key) {
        try {
            keysLock.writeLock().lock();
            keyChain.addFirst(key);
        } finally {
            keysLock.writeLock().unlock();
        }
    }

    public void addKeyChain(List<String> keys) {
        try {
            keysLock.writeLock().lock();
            //FIXME may be index out of range
            keyChain.addAll(keyChain.size(), keys);
        } finally {
            keysLock.writeLock().unlock();
        }
    }

    public void resetKey(String key) {
        try {
            keysLock.writeLock().lock();
            keyChain.clear();
            keyChain.addFirst(key);
        } finally {
            keysLock.writeLock().unlock();
        }
    }

    public long getLastModified() {

        return lastModified;
    }

    protected void setLastModified(long lastModified) {

        this.lastModified = lastModified;
    }

    public Set<Short> getAwareIds() {

        try {
            idsLock.readLock().lock();
            return awareIds;
        } finally {
            idsLock.readLock().unlock();
        }
    }

    protected void setAwareIds(Set<Short> awareIds) {

        this.awareIds = awareIds;
    }

    protected boolean addAwareId(final short id) {
        try {
            idsLock.writeLock().lock();
            if (this.awareIds == null)

                this.awareIds = new HashSet<Short>();
            return this.awareIds.add(id);
        } finally {
            idsLock.writeLock().unlock();
        }
    }

    protected boolean addAwareId(final Set<Short> ids) {
        try {
            idsLock.writeLock().lock();
            if (this.awareIds == null)
                this.awareIds = new HashSet<Short>();
            return this.awareIds.addAll(ids);
        } finally {
            idsLock.writeLock().unlock();
        }
    }

    public byte getState() {
        return state;
    }

    protected void setState(byte state) {
        this.state = state;
    }

    public boolean isDeleted() {

        return state == STATE_DEL;
    }

    public boolean isDown() {

        return state == STATE_DWN;
    }

    public boolean isValid() {

        return state == STATE_VLD;
    }

    public boolean isFullyScheduled() {

        return schedule == SYNCHED;
    }

    public boolean isScheduled() {

        return schedule > NOT_SYNCHED;
    }

    public void setScheduled(boolean scheduled) {

        this.schedule = (short) (scheduled ? SYNCHED : NOT_SYNCHED);
    }

    public short getSchedule() {

        return schedule;
    }

    public void setSchedule(short schedule) {

        this.schedule = schedule;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeByte(version);
        out.writeShort(id);
        out.writeBoolean(useSsl);
        out.writeBoolean(authByKey);
        if (keyChain != null) {
            out.writeByte(keyChain.size());
            for (String key : keyChain) {
                out.writeUTF(key);
            }
        } else {
            out.writeByte(0);
        }

        if (awareIds != null) {
            out.writeByte(awareIds.size());
            for (Short awareId : awareIds) {
                out.writeShort(awareId);
            }
        } else {
            out.writeByte(0);
        }

        out.writeLong(lastModified);
        out.writeByte(state);
        out.writeShort(monitorDelay);
        out.writeShort(monitorInterval);
        out.writeShort(reportDelay);
        out.writeShort(reportInterval);
        out.writeObject(synchAddresses);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {

        this.version = in.readByte();
        this.id = in.readShort();
        this.useSsl = in.readBoolean();
        this.authByKey = in.readBoolean();
        byte len = in.readByte();
        if (len > 0) {
            LinkedList<String> keys = new LinkedList<>();
            for (int i = 0; i < len; i++) {
                String key = (String) in.readUTF();
                keys.add(key);
            }
            this.keyChain = keys;
        } else
            this.keyChain = new LinkedList<String>();

        len = in.readByte();
        this.awareIds = null;
        if (len > 0) {
            Set<Short> ids = new HashSet<>();
            for (int i = 0; i < len; i++)
                ids.add(in.readShort());
            this.awareIds = ids;
        }

        this.lastModified = in.readLong();
        this.state = in.readByte();
        this.monitorDelay = in.readShort();
        this.monitorInterval = in.readShort();
        this.reportDelay = in.readShort();
        this.reportInterval = in.readShort();
        this.synchAddresses = (Set<ClusterAddress>) in.readObject();

        this.schedule = NOT_SYNCHED;
        this.name = createName(this.id);
        /*
         * This makes previous down state to STATE_DEL
         */
        if (state == STATE_DWN) {
            state = STATE_DEL;
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof Member) {
            return ((Member) obj).getId() == this.getId();
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Short.valueOf(getId()).hashCode();
    }

    @Override
    public byte getVersion() {
        return this.version;
    }

    @Override
    public void setVersion(byte version) {

        this.version = version;
    }

    public short getMonitorInterval() {

        return monitorInterval;
    }

    public void setMonitorInterval(short monitorInterval) {

        this.monitorInterval = monitorInterval;
    }

    public short getReportInterval() {

        return reportInterval;
    }

    public void setReportInterval(short reportInterval) {

        this.reportInterval = reportInterval;
    }

    public short getMonitorDelay() {

        return monitorDelay;
    }

    public void setMonitorDelay(short monitorDelay) {

        this.monitorDelay = monitorDelay;
    }

    public short getReportDelay() {

        return reportDelay;
    }

    public void setReportDelay(short reportDelay) {

        this.reportDelay = reportDelay;
    }

    public int getCurrentSocketIndex() {

        return currentSocketIndex;
    }

    public void setCurrentSocketIndex(int currentSocketIndex) {

        this.currentSocketIndex = currentSocketIndex;
    }

    @Override
    public String toString() {

        StringBuffer buf = new StringBuffer();
        buf.append("id=")
                .append(this.id)
                .append(",state=")
                .append(isValid())
                .append(",version=")
                .append(lastModified)
                .append(",use ssl=")
                .append(useSsl)
                .append(",synch addrs:")
                .append(String.valueOf(synchAddresses))
                .append(", monitor delay=")
                .append(monitorDelay)
                .append(", monitor interval")
                .append(monitorInterval)
                .append(", report delay=")
                .append(reportDelay)
                .append(", report interval")
                .append(reportInterval);
        return buf.toString();
    }

    public String toXmlString() {

        StringBuffer buf = new StringBuffer();

        buf.append("<edge>")
                .append("<id>")
                .append(this.id)
                .append("</id>")
                .append("<state>")
                .append(isValid())
                .append("</state>")
                .append("<version>")
                .append(this.lastModified)
                .append("</version>")
                .append("<aware-ids>")
                .append(this.awareIds)
                .append("</aware-ids>")
                .append("<synch-addrz>")
                .append(synchAddresses)
                .append("</synch-addrz>")
                .append("</edge>");
        return buf.toString();
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}
