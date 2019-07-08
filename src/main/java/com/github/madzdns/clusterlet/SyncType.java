package com.github.madzdns.clusterlet;

public enum SyncType {

    UNICAST((byte) 1),
    RING((byte) 2),
    UNICAST_QUERIOM((byte) 3),
    RING_QUERIOM((byte) 4),
    UNICAST_BALANCE((byte) 5),
    UNICAST_BALANCE_QUERIOM((byte) 6),
    RING_BALANCE((byte) 7),
    RING_BALANCE_QUERIOM((byte) 8),
    UNICAST_ONE_OF((byte) 9);

    public static SyncType getByValue(byte value) {

        if (value == SyncType.RING.getValue()) {
            return SyncType.RING;
        } else if (value == SyncType.RING_QUERIOM.getValue()) {
            return SyncType.RING_QUERIOM;
        } else if (value == SyncType.UNICAST.getValue()) {
            return SyncType.UNICAST;
        } else if (value == SyncType.UNICAST_QUERIOM.getValue()) {
            return SyncType.UNICAST_QUERIOM;
        } else if (value == SyncType.RING_BALANCE.getValue()) {
            return SyncType.RING_BALANCE;
        } else if (value == SyncType.RING_BALANCE_QUERIOM.getValue()) {
            return SyncType.RING_BALANCE_QUERIOM;
        } else if (value == SyncType.UNICAST_BALANCE.getValue()) {
            return SyncType.UNICAST_BALANCE;
        } else if (value == SyncType.UNICAST_BALANCE_QUERIOM.getValue()) {
            return SyncType.UNICAST_BALANCE;
        } else if (value == SyncType.UNICAST_ONE_OF.getValue()) {
            return SyncType.UNICAST_ONE_OF;
        }
        return null;
    }

    private byte value;

    SyncType(byte value) {
        this.value = value;
    }

    public byte getValue() {
        return this.value;
    }

    public static boolean checkIfBalanceType(SyncType syncType) {
        return syncType == RING_BALANCE ||
                syncType == RING_BALANCE_QUERIOM ||
                syncType == UNICAST_BALANCE ||
                syncType == UNICAST_BALANCE_QUERIOM;
    }

    public static boolean checkIfUnicastType(SyncType syncType) {
        return syncType == UNICAST ||
                syncType == UNICAST_QUERIOM ||
                syncType == UNICAST_BALANCE ||
                syncType == UNICAST_BALANCE_QUERIOM ||
                syncType == UNICAST_ONE_OF;
    }

    public static boolean checkIfRingType(SyncType syncType) {
        return syncType == RING ||
                syncType == RING_QUERIOM ||
                syncType == RING_BALANCE ||
                syncType == RING_BALANCE_QUERIOM;
    }
}
