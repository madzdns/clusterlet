package com.github.madzdns.clusterlet;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ClusterSnapshot {
    public final static int MEMBER_CHECK_NONE = 0;
    public final static int MEMBER_CHECK_VALID_OR_DOWN = 1;
    public final static int MEMBER_CHECK_VALID = 2;
    //removed volatile of these
    Set<Short> validClusterIDs = null;
    Set<Short> inValidClusterIDs = null;
    List<Member> validCluster = null;
    List<Member> aliveCluster = null;
    List<Member> cluster = null;

    Map<Short, Member> idClusterMap = null;

    public ClusterSnapshot() {
        validCluster = new ArrayList<>();
        aliveCluster = new ArrayList<>();
        cluster = new ArrayList<>();
        validClusterIDs = new HashSet<>();
        inValidClusterIDs = new HashSet<>();
        idClusterMap = new HashMap<>();
    }

    /**
     * Returns only those IDs of edges marked as valid.
     * An edge is valid if and only if its not deleted
     * and not marked as down
     *
     * @return
     */
    public Set<Short> getValidClusterIDs() {
        return validClusterIDs;
    }

    /**
     * Returns only those IDs of edges marked as invalid.
     * An edge is valid if and only if its not deleted
     * and not marked as down
     *
     * @return
     */
    public Set<Short> getInValidClusterIDs() {
        return inValidClusterIDs;
    }

    /**
     * Returns only those edges marked as valid.
     * An edge is valid if and only if its not deleted
     * and not marked as down
     *
     * @return
     */
    public List<Member> getValidCluster() {
        return validCluster;
    }

    /**
     * Returns those edges that are alive.
     * An edge is alive if and only if its not marked
     * as down. So a deleted edge can still be alive
     *
     * @return
     */
    public List<Member> getAliveCluster() {
        return aliveCluster;
    }

    public Member getById(short id, int memberCheck) {
        Member member = idClusterMap.get(id);
        if (member == null) {
            return null;
        }
        if (memberCheck == MEMBER_CHECK_NONE) {
            return member;
        } else if (memberCheck == MEMBER_CHECK_VALID_OR_DOWN) {
            if (member.isValid() || member.isDown()) {
                return member;
            }
        }
        if (member.isValid()) {
            return member;
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
