package com.github.madzdns.clusterlet.codec;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.frfra.frsynch.SynchType;

public class SynchMessage implements ISynchMessage {

	public final static byte TYPE_BAD_KEY = 0;
	public final static byte TYPE_BAD_SEQ = 1;
	public final static byte TYPE_BAD_ID = 2;
	public final static byte TYPE_OK = 3;
	public final static byte TYPE_FULL_CHECK = 4;
	public final static byte TYPE_CHECK = 5;
	public final static byte TYPE_NOT_VALID_EDGE = 6;
	public final static byte TYPE_BOTH_STARTUP = 7;
	public final static byte TYPE_FAILD_RING = 8;
	public final static byte TYPE_STARTUP_CHECK = 9;
	
	public final static byte COMMAND_TAKE_THis = 0;
	public final static byte COMMAND_GIVE_THis = 1;
	public final static byte COMMAND_DEL_THis = 2;
	public final static byte COMMAND_OK = 3;
	public final static byte COMMAND_RCPT_THis = 4;
	
	public final static byte SCHEDULED = 1;
	public final static byte NOT_SCHEDULED = 0;
	public final static byte IN_STARTUP = 1;
	public final static byte NOT_IN_STARTUP = 0;
	
	public enum SynchMode {
		
		SYNCH_CLUSTER((byte)1),
		SYNCH_MESSAGE((byte)0);
		
		private byte mode;
		
		private SynchMode(byte mode) {
			
			this.mode = mode;
		}
		
		public byte getMode() {
			
			return this.mode;
		}
	}
	
	@SuppressWarnings("unused")
	private static Logger log = LoggerFactory.getLogger(SynchMessage.class);
	
	/*
	 * I think maximum sequence of 4 is sufficient
	 */
	public final static byte SEQ_MAX = 4;
	
	private List<String> keyChain = null;
	
	private short id = 0;
	
	private byte type = 0;
	
	private byte sequence = 0;
	
	private SynchMode synchMode = SynchMode.SYNCH_CLUSTER;
	
	private boolean inStartup = false;
	
	private List<SynchContent> contents;
	
	private SynchType synchType = SynchType.UNICAST;
	
	private Set<Short> expectedIds = null;
	
	public SynchMessage() {
		
		contents = new ArrayList<SynchContent>();
	}
	
	public byte getType() {
		
		return type;
	}
	
	public void setType(byte type) {
		
		this.type = type;
	}
	
	public List<String> getKeyChain() {
		
		return keyChain;
	}

	public void setKeyChain(List<String> keyChain) {
		
		this.keyChain = keyChain;
	}
	
	public short getId() {
		
		return id;
	}
	
	public void setId(short id) {
		
		this.id = id;
	}
	
	public byte getSequence() {
		
		return sequence;
	}
	
	public void setSequence(byte sequence) {
		
		this.sequence = sequence;
	}
	
	public SynchMode getSynchMode() {
		
		return this.synchMode;
	}
	
	public void setSynchMode(final SynchMode mode) {
		
		this.synchMode = mode;
	}

	public boolean isInStartup() {
		
		return inStartup;
	}

	public void setInStartup(boolean inStartup) {
		
		this.inStartup = inStartup;
	}

	public List<SynchContent> getContents() {
		
		return this.contents;
	}
	
	public void setContents(List<SynchContent> contents) {
		
		this.contents = contents;
	}
	
	public void setContents(Collection<SynchContent> contents) {
		
		this.contents = new ArrayList<SynchContent>(contents);
	}

	public void addContents(SynchContent content) {
		
		this.contents.add(content);
	}
	
	public SynchType getSynchType() {
		
		return synchType;
	}

	public void setSynchType(SynchType synchType) {
		
		this.synchType = synchType;
	}
	
	public Set<Short> getExpectedIds() {
		
		return expectedIds;
	}

	public void setExpectedIds(Set<Short> expectedIds) {
		
		this.expectedIds = expectedIds;
	}


	@Override
	public void deserialize(DataInputStream in) throws IOException {
		
		id = in.readShort();
		type = in.readByte();
		sequence = in.readByte();
		inStartup = in.readBoolean();
		
		byte mode = in.readByte();
		
		if(mode == SynchMode.SYNCH_CLUSTER.getMode()) {
			
			synchMode = SynchMode.SYNCH_CLUSTER;
		}
		else {
			
			synchMode = SynchMode.SYNCH_MESSAGE;
		}
		
		mode = in.readByte();
		
		synchType = SynchType.getByValue(mode);

		int len = in.readByte();
		
		if(len > 0) {
			
			keyChain = new ArrayList<String>();
			String key;
			for(int i=0; i<len; i++) {
				
				key = in.readUTF();
				keyChain.add(key);
			}	
		}
		
		len = in.readShort();
		
		if(len > 0) {
			
			this.expectedIds = new HashSet<Short>(len);
			
			for(int i = 0; i < len; i++) {
				
				this.expectedIds.add(in.readShort());
			}
		}
		
		len = in.readInt();
		
		if(len > 0) {
			
			int contentLen = 0;
			byte[] message = null;
			long version = 0;
			String key = null;
			for(int i=0; i<len; i++ ) {
				
				contentLen = in.readInt();
				
				if(contentLen > 0) {
					
					message = new byte[contentLen];
					in.read(message);
				}
				else {
					
					message = null;
				}
				
				version = in.readLong();
				key = in.readUTF();
				
				contentLen = in.readShort();
				
				Set<Short> awareIds = null;
				
				if(contentLen > 0) {
					
					awareIds = new HashSet<Short>();
					for(int j=0; j<contentLen; j++) {
						
						awareIds.add(in.readShort());
					}
				}
				
				SynchContent s = new SynchContent(key, version, awareIds, message);
				this.contents.add(s);
			}
		}
	}
	
	@Override
	public void serialize(DataOutputStream out) 
			throws IOException {
		
		out.writeShort(id);
		out.writeByte(type);
		out.writeByte(sequence);
		out.writeBoolean(inStartup);
		out.writeByte(synchMode.getMode());

		out.writeByte(synchType.getValue());

		List<String> keys = getKeyChain();
		
		if(keys != null) {
			
			out.writeByte(keys.size());
			String key = null;
			for(int i=0;i<keys.size();i++) {
				
				key = keys.get(i);
				out.writeUTF(key);
			}
			
			key = null;
			keys = null;
		}
		else {
			
			out.writeByte(0);
		}
		
		if(this.expectedIds == null ||
				this.expectedIds.size() == 0) {
			
			out.writeShort(0);
		}
		else {
			
			out.writeShort(this.expectedIds.size());
			
			for(Iterator<Short> it = this.expectedIds.iterator(); it.hasNext();) {
				
				out.writeShort(it.next());
			}
		}
		
		out.writeInt(contents.size());

		if(contents.size() == 0) {
			
			return;
		}
		
		for(Iterator<SynchContent> it = contents.iterator(); 
				it.hasNext();) {
			
			SynchContent c = it.next();
			
			if(c.getContent() == null ||
					c.getContent().length == 0) {

				out.writeInt(0);
			}
			else {
			
				out.writeInt(c.getContent().length);
				out.write(c.getContent());
			}
			
			out.writeLong(c.getVersion());
			out.writeUTF(c.getKey());
			
			if(c.getAwareIds() == null 
					|| c.getAwareIds().size() == 0) {
				
				out.writeShort(0);
			}
			else {
				
				out.writeShort(c.getAwareIds().size());
				for(Iterator<Short> idIt = c.getAwareIds().iterator();
						idIt.hasNext();) {
					
					out.writeShort(idIt.next());
				}
			}
		}
	}
}
