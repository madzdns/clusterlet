package com.github.madzdns.clusterlet.codec;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.madzdns.clusterlet.ClusterNode.ClusterAddress;

public class ClusterMessage implements IMessage {
	
	private static Logger log = LoggerFactory.getLogger(ClusterMessage.class);
	
	private short id = -1;
	
	private boolean useSsl = true;
	
	private boolean authByKey = true;
	
	private String credentionalKey = "";
	
	private long version = 0;
	
	private Set<ClusterAddress> synchAddresses = null;
	
	private Set<ClusterAddress> backendAddresses = null;
	
	private byte command = 0;

	private short monitorDelay = 25;
	
	private short monitorInterval = 605;
	
	private short reportDelay = 26;
	
	private short reportInterval = 606;
	
	public ClusterMessage() {
		
	}
	
	public ClusterMessage(short id, boolean useSsl, boolean authByKey,
			String key, long version, Set<ClusterAddress> synchAddresses,
			Set<ClusterAddress> backendAddresses, byte command,
			short monitorDelay, short monitorInterval, short reportDelay,
			short reportInterval) {

		this.id = id;
		this.useSsl = useSsl;
		this.authByKey = authByKey;
		this.credentionalKey = key;
		this.version = version;
		this.synchAddresses = synchAddresses;
		this.backendAddresses = backendAddresses;
		this.command = command;
		this.monitorDelay = monitorDelay;
		this.monitorInterval = monitorInterval;
		this.reportDelay = reportDelay;
		this.reportInterval = reportInterval;
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

	public Set<ClusterAddress> getBackendAddresses() {
		
		return backendAddresses;
	}

	public void setBackendAddrz(Set<ClusterAddress> backendAddresses) {
		
		this.backendAddresses = backendAddresses;
	}

	public byte getCommand() {
		
		return command;
	}

	public void setCommand(byte command) {
		
		this.command = command;
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

	@Override
	public byte[] serialize() {

		ByteArrayOutputStream stream = new ByteArrayOutputStream();
		DataOutputStream out = new DataOutputStream(stream);
		
		try {
			
			out.writeShort(this.id);
			out.writeBoolean(this.useSsl);
			out.writeBoolean(this.authByKey);
			out.writeUTF(credentionalKey);
			out.writeLong(version);
			out.writeByte(command);
			
			if(synchAddresses!=null) {
				
				out.writeByte(synchAddresses.size());
				
				for(Iterator<ClusterAddress> it2 = synchAddresses.iterator();
						it2.hasNext();) {
					
					ClusterAddress addr = it2.next();
					out.writeByte(addr.getAddress().getAddress().length);
					out.write(addr.getAddress().getAddress());
					out.writeInt(addr.getPort());
				}	
			}
			else
				out.writeByte(0);
			
			if(backendAddresses!=null) {
				
				out.writeByte(backendAddresses.size());
				
				for(Iterator<ClusterAddress> it2 = backendAddresses.iterator();
						it2.hasNext();) {
					
					ClusterAddress addr = it2.next();
					out.writeByte(addr.getAddress().getAddress().length);
					out.write(addr.getAddress().getAddress());
					out.writeInt(addr.getPort());
				}	
			}
			else
				out.writeByte(0);
			
			out.writeShort(monitorDelay);
			out.writeShort(monitorInterval);
			out.writeShort(reportDelay);
			out.writeShort(reportInterval);
			
			return stream.toByteArray();
			
		} catch (Exception e) {
			
			log.error("",e);
			return null;
		}
		finally {
			
			try {
				
				stream.close();
			} catch (IOException e) {
			}
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
		  
		DataInputStream in = new DataInputStream(new ByteArrayInputStream(data));
		
		try {
			
			id = in.readShort();
			useSsl = in.readBoolean();
			authByKey = in.readBoolean();
			credentionalKey = in.readUTF();
			version = in.readLong();
			command = in.readByte();
			
			int len = in.readByte();
			
			if(len > 0) {
				
				synchAddresses = new HashSet<ClusterAddress>();
				
				byte[] ip;
				
				byte ip_len;
				
				int port;
				
				for(int i=0; i<len; i++) {
					
					ip = null;
					ip_len = in.readByte();
					
					if(ip_len > 0) {
						
						ip = new byte[ip_len];
						in.read(ip);
					}
					
					port = in.readInt();
					
					ClusterAddress ca = new ClusterAddress(InetAddress.getByAddress(ip), port);
					synchAddresses.add(ca);
				}	
			}
			
			len = in.readByte();
			
			if(len > 0) {
				
				backendAddresses = new HashSet<ClusterAddress>();
				
				byte[] ip;
				
				byte ip_len;
				
				int port;
				
				for(int i=0; i<len; i++) {
					
					ip = null;
					ip_len = in.readByte();
					
					if(ip_len > 0) {
						
						ip = new byte[ip_len];
						in.read(ip);
					}
					
					port = in.readInt();

					ClusterAddress ca = new ClusterAddress(InetAddress.getByAddress(ip), port);
					backendAddresses.add(ca);
				}	
			}
			
			monitorDelay = in.readShort();
			monitorInterval = in.readShort();
			
			reportDelay = in.readShort();
			reportInterval = in.readShort();
			
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
