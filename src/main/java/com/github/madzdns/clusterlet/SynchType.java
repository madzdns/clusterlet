package com.github.madzdns.clusterlet;

public enum SynchType {

    UNICAST((byte) 1),
    RING((byte) 2),
    UNICAST_QUERIOM((byte) 3),
    RING_QUERIOM((byte) 4),
    UNICAST_BALANCE((byte) 5),
    UNICAST_BALANCE_QUERIOM((byte) 6),
    RING_BALANCE((byte) 7),
    RING_BALANCE_QUERIOM((byte) 8),
    UNICAST_ONE_OF((byte) 9);

    public static SynchType getByValue(byte value) {

        if (value == SynchType.RING.getValue()) {
            return SynchType.RING;
        } else if (value == SynchType.RING_QUERIOM.getValue()) {
            return SynchType.RING_QUERIOM;
        } else if (value == SynchType.UNICAST.getValue()) {
            return SynchType.UNICAST;
        } else if (value == SynchType.UNICAST_QUERIOM.getValue()) {
            return SynchType.UNICAST_QUERIOM;
        } else if (value == SynchType.RING_BALANCE.getValue()) {
            return SynchType.RING_BALANCE;
        } else if (value == SynchType.RING_BALANCE_QUERIOM.getValue()) {
            return SynchType.RING_BALANCE_QUERIOM;
        } else if (value == SynchType.UNICAST_BALANCE.getValue()) {
            return SynchType.UNICAST_BALANCE;
        } else if (value == SynchType.UNICAST_BALANCE_QUERIOM.getValue()) {
            return SynchType.UNICAST_BALANCE;
        } else if (value == SynchType.UNICAST_ONE_OF.getValue()) {
            return SynchType.UNICAST_ONE_OF;
        }
        return null;
    }

    private byte value;

    SynchType(byte value) {
        this.value = value;
    }

    public byte getValue() {
        return this.value;
    }

    public static boolean checkIfBalanceType(SynchType synchType) {
        return synchType == RING_BALANCE ||
                synchType == RING_BALANCE_QUERIOM ||
                synchType == UNICAST_BALANCE ||
                synchType == UNICAST_BALANCE_QUERIOM;
    }

    public static boolean checkIfUnicastType(SynchType synchType) {
        return synchType == UNICAST ||
                synchType == UNICAST_QUERIOM ||
                synchType == UNICAST_BALANCE ||
                synchType == UNICAST_BALANCE_QUERIOM ||
                synchType == UNICAST_ONE_OF;
    }

    public static boolean checkIfRingType(SynchType synchType) {
        return synchType == RING ||
                synchType == RING_QUERIOM ||
                synchType == RING_BALANCE ||
                synchType == RING_BALANCE_QUERIOM;
    }
}
