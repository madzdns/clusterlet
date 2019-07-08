package com.github.madzdns.clusterlet.codec;

import java.util.Set;

public class SyncContent {

	private String key;
	private long version;
	private Set<Short> awareIds;
	private byte[] content;
	
	public SyncContent(String key, long version, Set<Short> awareIds,
					   byte[] content) {

		this.key = key;
		this.version = version;
		this.awareIds = awareIds;
		this.content = content;
	}

	public String getKey() {
		
		return key;
	}
	
	public long getVersion() {
		
		return version;
	}
	
	public Set<Short> getAwareIds() {
		
		return awareIds;
	}
	
	public void addAwareId(short node) {
		
		if(awareIds == null) {
			
			return;
		}
		
		awareIds.add(node);
	}
	
	public void addAwareId(Set<Short> nodes) {
		
		if(awareIds == null) {
			
			return;
		}
		
		awareIds.addAll(nodes);
	}
	
	public byte[] getContent() {
		
		return content;
	}
	
	@Override
	public int hashCode() {
		// TODO Auto-generated method stub
		return this.getKey().hashCode();
	}
	
	@Override
	public boolean equals(Object obj) {
		
		if(obj instanceof SyncContent) {
			
			SyncContent s = (SyncContent) obj;
			
			if(s.getKey() == null || getKey() == null) {
				
				return false;
			}
			
			if(s.getKey().equals(getKey())) {
				
				return true;
			}
		}
		
		return false;
	}
	
	@Override
	public String toString() {
		
		return new StringBuilder("key=").append(key)
				.append(",version=").append(version)
				.append(", awareNodes=").append(this.awareIds)
				.toString();
	}
}
