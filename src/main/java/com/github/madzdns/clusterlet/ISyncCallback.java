package com.github.madzdns.clusterlet;

import com.github.madzdns.clusterlet.codec.IMessage;

import java.util.Set;

public interface ISyncCallback {
    boolean callBack(ISession session, IMessage message,
                     Set<Short> withNodes, ISyncProtocolOutput out);

    void result(SyncFeature syncFeature);
}
