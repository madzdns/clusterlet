package com.github.madzdns.clusterlet;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import lombok.extern.slf4j.Slf4j;

import com.github.madzdns.clusterlet.codec.ClusterMessage;
import com.github.madzdns.clusterlet.codec.IMessage;
import com.github.madzdns.clusterlet.codec.SyncMessage;
import com.github.madzdns.clusterlet.codec.SyncMessage.SyncMode;

@Slf4j
public class StartupManager {

	private SyncContext syncContext;
	StartupManager(SyncContext syncContext) {
		this.syncContext = syncContext;
	}
	
	boolean startClusterSyncing() {
		ClusterMessage msg = new ClusterMessage();
		SyncHandler handler = new SyncHandler(syncContext, SyncType.UNICAST_ONE_OF)
				.withCallBack(new ClusterSyncCallback(syncContext))
				.withEncoder(null)
				.withoutCluster(syncContext.myId);
		handler.mode = SyncMode.SYNC_CLUSTER;
		SyncFeature feature = handler.sync(msg).get();
		if(feature != null && feature.get("-1").isSuccessful()) {
			syncContext.inStartup = false;
		}
		
		ClusterSnapshot snapshot = syncContext.getSnapshot();
		List<IMessage> messages = new ArrayList<>();
		for(Iterator<Member> it = snapshot.cluster.iterator(); it.hasNext();) {
			Member node = it.next();
			msg = new ClusterMessage(node.getId(),
					node.isUseSsl(),node.isAuthByKey(),node.getKey(),
					node.getLastModified(),
					node.getSyncAddresses(),
					node.isValid()? SyncMessage.COMMAND_TAKE_THis
							: SyncMessage.COMMAND_DEL_THis);
			messages.add(msg);
		}

		handler = new SyncHandler(syncContext, SyncType.UNICAST_ONE_OF)
				.withCallBack(new ClusterSyncCallback(syncContext))
				.withEncoder(null)
				.withoutCluster(syncContext.myId);
		handler.mode = SyncMode.SYNC_CLUSTER;
		handler.sync(messages).get();
		return !syncContext.inStartup;
	}
}
