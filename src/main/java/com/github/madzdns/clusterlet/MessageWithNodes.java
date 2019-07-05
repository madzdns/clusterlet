package com.github.madzdns.clusterlet;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public class MessageWithNodes implements Externalizable {

	long version;
	Set<Short> awareNodes;
	
	public MessageWithNodes() {
		
	}
	
	public MessageWithNodes(long version, Set<Short> awareNodes) {

		this.version = version;
		this.awareNodes = awareNodes;
	}

	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		
		out.writeLong(version);
		if(awareNodes != null) {
			
			out.writeShort(awareNodes.size());
			
			for(Iterator<Short> it = awareNodes.iterator(); it.hasNext();) {
				
				out.writeShort(it.next());
			}
		}
		else {
			
			out.writeShort(0);
		}
	}

	@Override
	public void readExternal(ObjectInput in) throws IOException,
			ClassNotFoundException {
		
		version = in.readLong();
		
		short len = in.readShort();
		
		if(len > 0) {
			
			awareNodes = new HashSet<Short>();
			
			for(int i =0; i<len; i++) {
				
				awareNodes.add(in.readShort());
			}
		}
	}
}
