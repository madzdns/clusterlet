package com.github.madzdns.clusterlet;

import java.util.HashSet;
import java.util.Set;

public class SynchResult {

	private boolean successful;
	private Set<Short> faildNodes;
	private Set<Short> synchedNodes;
	
	public SynchResult() {
		
		this.faildNodes = new HashSet<Short>();
		this.synchedNodes = new HashSet<Short>();
	}
	
	/**
	 * 
	 * @return true if message successfully was synched 
	 */
	public boolean isSuccessful() {
		
		return successful;
	}
	
	public void setSuccessful(boolean successful) {
		
		this.successful = successful;
	}
	
	/**
	 * 
	 * @return id of nodes that message was failed to synched with them
	 */
	public Set<Short> getFaildNodes() {
		
		return faildNodes;
	}
	
	public void setFaildNodes(Set<Short> faildNodes) {
		
		this.faildNodes = faildNodes;
	}
	
	/**
	 * 
	 * @return id of nodes that message was successfully synched with them
	 */
	public Set<Short> getSynchedNodes() {
		
		return synchedNodes;
	}
	
	public void setSynchedNodes(Set<Short> synchedNodes) {
		
		this.synchedNodes = synchedNodes;
	}
	
	public void addSynchedNode(Short id) {
		
		this.synchedNodes.add(id);
	}
	
	public void addSynchedNode(Set<Short> ids) {
		
		if(ids == null) {
			
			return;
		}
		
		this.synchedNodes.addAll(ids);
	}
	
	public void removeSynchedNode(Set<Short> ids) {
		
		if(ids == null) {
			
			return;
		}

		this.synchedNodes.removeAll(ids);
	}
	
	public void removeSynchedNode(Short id) {
		
		this.synchedNodes.remove(id);
	}
	
	public void addFaildNode(Short id) {
		
		this.faildNodes.add(id);
	}
	
	public void addFaildNode(Set<Short> ids) {
		
		if(ids == null) {
			
			return;
		}

		this.faildNodes.addAll(ids);
	}
	
	public void removeFaildNode(Set<Short> ids) {
		
		if(ids == null) {
			
			return;
		}

		this.faildNodes.removeAll(ids);
	}
	
	public void removeFaildNode(Short id) {
		
		this.faildNodes.remove(id);
	}
}
