package com.github.madzdns.clusterlet;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.net.InetAddress;
import java.net.InetSocketAddress;

public abstract class StorableSocketAddress implements Externalizable{
	
	InetSocketAddress addr = null;

	public StorableSocketAddress(){}
	
	public StorableSocketAddress(final InetAddress addr, final int port) {
		
		this.addr = new InetSocketAddress(addr, port);
	}
	
	public StorableSocketAddress(final String hostname, final int port) {
		
		this.addr = new InetSocketAddress(hostname, port);
	}
	
	public InetAddress getAddress() {
		
		return addr.getAddress();
	}
	
	public int getPort() {
		
		return addr.getPort();
	}

	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		
		out.writeShort(getAddress().getAddress().length);
		out.write(getAddress().getAddress());
		out.writeInt(addr.getPort());
	}

	@Override
	public void readExternal(ObjectInput in) throws IOException,
			ClassNotFoundException {
		
		short len = in.readShort();
		InetAddress iaddr = null;
		
		if(len>0) {
			
			byte[] addr = new byte[len];
			in.read(addr);
			iaddr = InetAddress.getByAddress(addr);
		}
		
		int port = in.readInt();
		this.addr = new InetSocketAddress(iaddr, port);
	}
	
	@Override
	public boolean equals(Object obj) {
		
		if(obj instanceof StorableSocketAddress)
		{
			StorableSocketAddress o = (StorableSocketAddress) obj;
			return this.getAddress().equals(o.getAddress());
		}
		return false;
	}
	
	@Override
	public int hashCode() {
		
		if(addr==null)
			
			return 0;
		
		return addr.hashCode();
	}
	
	@Override
	public String toString() {
		return String.valueOf(addr);
	}

}
