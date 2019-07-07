package com.github.madzdns.clusterlet;

import java.util.HashSet;
import java.util.Set;

public class SynchResult {

	private boolean successful;
	private Set<Short> failedMembers;
	private Set<Short> synchedMembers;
	public SynchResult() {
		this.failedMembers = new HashSet<>();
		this.synchedMembers = new HashSet<>();
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
	public Set<Short> getFailedMembers() {
		return failedMembers;
	}
	
	public void setFailedMembers(Set<Short> failedMembers) {
		this.failedMembers = failedMembers;
	}
	
	/**
	 * 
	 * @return id of nodes that message was successfully synched with them
	 */
	public Set<Short> getSynchedMembers() {
		return synchedMembers;
	}
	
	public void setSynchedMembers(Set<Short> synchedNodes) {
		this.synchedMembers = synchedNodes;
	}
	
	public void addSynchedMember(Short id) {
		this.synchedMembers.add(id);
	}
	
	public void addSynchedMember(Set<Short> ids) {
		if(ids == null) {
			return;
		}
		this.synchedMembers.addAll(ids);
	}
	
	public void removeSynchedMember(Set<Short> ids) {
		if(ids == null) {
			return;
		}
		this.synchedMembers.removeAll(ids);
	}
	
	public void removeSynchedMember(Short id) {
		this.synchedMembers.remove(id);
	}
	
	public void addFailedMember(Short id) {
		this.failedMembers.add(id);
	}
	
	public void addFailedMember(Set<Short> ids) {
		if(ids == null) {
			return;
		}
		this.failedMembers.addAll(ids);
	}
	
	public void removeFailedMember(Set<Short> ids) {
		if(ids == null) {
			return;
		}
		this.failedMembers.removeAll(ids);
	}
	
	public void removeFailedMember(Short id) {
		this.failedMembers.remove(id);
	}
}
