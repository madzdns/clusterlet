package com.github.madzdns.clusterlet;

import java.util.HashSet;
import java.util.Set;

public class SyncResult {

    private boolean successful;
    private Set<Short> failedMembers;
    private Set<Short> syncedMembers;

    public SyncResult() {
        this.failedMembers = new HashSet<>();
        this.syncedMembers = new HashSet<>();
    }

    /**
     * @return true if message successfully was synced
     */
    public boolean isSuccessful() {
        return successful;
    }

    public void setSuccessful(boolean successful) {
        this.successful = successful;
    }

    /**
     * @return id of nodes that message was failed to synced with them
     */
    public Set<Short> getFailedMembers() {
        return failedMembers;
    }

    public void setFailedMembers(Set<Short> failedMembers) {
        this.failedMembers = failedMembers;
    }

    /**
     * @return id of nodes that message was successfully synced with them
     */
    public Set<Short> getSyncedMembers() {
        return syncedMembers;
    }

    public void setSyncedMembers(Set<Short> syncedMembers) {
        this.syncedMembers = syncedMembers;
    }

    public void addSyncedMember(Short id) {
        this.syncedMembers.add(id);
    }

    public void addSyncedMember(Set<Short> ids) {
        if (ids == null) {
            return;
        }
        this.syncedMembers.addAll(ids);
    }

    public void removeSyncedMember(Set<Short> ids) {
        if (ids == null) {
            return;
        }
        this.syncedMembers.removeAll(ids);
    }

    public void removeSyncedMember(Short id) {
        this.syncedMembers.remove(id);
    }

    public void addFailedMember(Short id) {
        this.failedMembers.add(id);
    }

    public void addFailedMember(Set<Short> ids) {
        if (ids == null) {
            return;
        }
        this.failedMembers.addAll(ids);
    }

    public void removeFailedMember(Set<Short> ids) {
        if (ids == null) {
            return;
        }
        this.failedMembers.removeAll(ids);
    }

    public void removeFailedMember(Short id) {
        this.failedMembers.remove(id);
    }
}
