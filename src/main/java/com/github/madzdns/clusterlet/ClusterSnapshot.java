package com.github.madzdns.clusterlet;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ClusterSnapshot {

	//removed volatile of these
	Set<Short> validClusterIDs = null;
	
	Set<Short> inValidClusterIDs = null;
	
	List<Member> validCluster = null;
	
	List<Member> aliveCluster = null;
	
	List<Member> cluster = null;
	
	Map<Short, Member> idClusterMap = null;
	
	public ClusterSnapshot() {
		
		validCluster = new ArrayList<Member>();
		
		aliveCluster = new ArrayList<Member>();
		
		cluster = new ArrayList<Member>();
		
		validClusterIDs = new HashSet<Short>();
		
		inValidClusterIDs = new HashSet<Short>();
		
		idClusterMap = new HashMap<Short, Member>();
	}

	/**
	 * Returns only those IDs of edges marked as valid.
	 * An edge is valid if and only if its not deleted
	 * and not marked as down
	 * @return
	 */
	public Set<Short> getValidClusterIDs() {
		
		return validClusterIDs;
	}
	
	/**
	 * Returns only those IDs of edges marked as invalid.
	 * An edge is valid if and only if its not deleted
	 * and not marked as down
	 * @return
	 */
	public Set<Short> getInValidClusterIDs() {
		
		return inValidClusterIDs;
	}

	/**
	 * Returns only those edges marked as valid.
	 * An edge is valid if and only if its not deleted
	 * and not marked as down
	 * @return
	 */
	public List<Member> getValidCluster() {
		
		return validCluster;
	}

	/**
	 * Returns those edges that are alive.
	 * An edge is alive if and only if its not marked
	 * as down. So a deleted edge can still be alive
	 * @return
	 */
	public List<Member> getAliveCluster() {
		
		return aliveCluster;
	}
	
	public Member getById(short id, int check) {
		
		Member e = idClusterMap.get(id);
		
		if(e == null) {
			
			return null;
		}
		
		if(check == 0) {
			
			return e;
		}
		else if(check == 1) {
			
			if(e.isValid() || e.isDown()) {
				
				return e;
			}
		}
		
		if(e.isValid()) {
			
			return e;
		}
		
		return null;
	}

	 void invalidateMonitor() {
		
		validCluster = null;
		
		validClusterIDs = null;
		
		inValidClusterIDs = null;
		
		aliveCluster = null;
		
		cluster = null;
		
	}

	public List<Member> getCluster() {
		
		return cluster;
	}
}
