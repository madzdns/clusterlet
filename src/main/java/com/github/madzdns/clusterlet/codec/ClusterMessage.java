package com.github.madzdns.clusterlet.codec;

import com.github.madzdns.clusterlet.Member.ClusterAddress;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.InetAddress;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

@Slf4j
public class ClusterMessage implements IMessage {
    private short id = -1;
    private boolean useSsl = true;
    private boolean authByKey = true;
    private String credentionalKey = "";
    private long version = 0;
    private Set<ClusterAddress> synchAddresses = null;
    private byte command = 0;

    public ClusterMessage() {
    }

    public ClusterMessage(short id, boolean useSsl, boolean authByKey, String key,
                          long version, Set<ClusterAddress> synchAddresses, byte command) {
        this.id = id;
        this.useSsl = useSsl;
        this.authByKey = authByKey;
        this.credentionalKey = key;
        this.version = version;
        this.synchAddresses = synchAddresses;
        this.command = command;
    }

    public short getId() {
        return id;
    }

    public void setId(short id) {
        this.id = id;
    }

    public boolean isUseSsl() {
        return useSsl;
    }

    public void setUseSsl(boolean useSsl) {
        this.useSsl = useSsl;
    }

    public boolean isAuthByKey() {
        return authByKey;
    }

    public void setAuthByKey(boolean authByKey) {
        this.authByKey = authByKey;
    }

    public void setCredentionalKey(String key) {
        this.credentionalKey = key;
    }

    public String getCredentionalKey() {
        return this.credentionalKey;
    }

    public void setVersion(long version) {
        this.version = version;
    }

    public Set<ClusterAddress> getSynchAddresses() {
        return synchAddresses;
    }

    public void setSynchAddresses(Set<ClusterAddress> synchAddresses) {
        this.synchAddresses = synchAddresses;
    }

    public byte getCommand() {
        return command;
    }

    public void setCommand(byte command) {
        this.command = command;
    }

    @Override
    public byte[] serialize() {
        try (ByteArrayOutputStream stream = new ByteArrayOutputStream()) {
            DataOutputStream out = new DataOutputStream(stream);
            out.writeShort(this.id);
            out.writeBoolean(this.useSsl);
            out.writeBoolean(this.authByKey);
            out.writeUTF(credentionalKey);
            out.writeLong(version);
            out.writeByte(command);

            if (synchAddresses != null) {
                out.writeByte(synchAddresses.size());
                for (ClusterAddress addr : synchAddresses) {
                    out.writeByte(addr.getAddress().getAddress().length);
                    out.write(addr.getAddress().getAddress());
                    out.writeInt(addr.getPort());
                }
            } else {
                out.writeByte(0);
            }
            return stream.toByteArray();
        } catch (Exception e) {
            log.error("", e);
            return null;
        }
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
            id = in.readShort();
            useSsl = in.readBoolean();
            authByKey = in.readBoolean();
            credentionalKey = in.readUTF();
            version = in.readLong();
            command = in.readByte();
            int len = in.readByte();
            if (len > 0) {
                synchAddresses = new HashSet<>();
                byte[] ip;
                byte ip_len;
                int port;
                for (int i = 0; i < len; i++) {
                    ip = null;
                    ip_len = in.readByte();
                    if (ip_len > 0) {
                        ip = new byte[ip_len];
                        in.read(ip);
                    }
                    port = in.readInt();
                    if (ip != null) {
                        ClusterAddress ca = new ClusterAddress(InetAddress.getByAddress(ip), port);
                        synchAddresses.add(ca);
                    }
                }
            }
        } catch (Exception e) {
            log.error("", e);
        }
    }

    @Override
    public String getKey() {
        return String.valueOf(this.id);
    }

    @Override
    public long getVersion() {
        return this.version;
    }
}
