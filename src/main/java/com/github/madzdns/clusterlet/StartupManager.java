package com.github.madzdns.clusterlet;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.madzdns.clusterlet.codec.ClusterMessage;
import com.github.madzdns.clusterlet.codec.IMessage;
import com.github.madzdns.clusterlet.codec.SynchMessage;
import com.github.madzdns.clusterlet.codec.SynchMessage.SynchMode;

public class StartupManager {

	@SuppressWarnings("unused")
	private static Logger log = LoggerFactory.getLogger(StartupManager.class);
	
	private SynchContext synchContext = null;
	
	StartupManager(SynchContext synchContext) {
		
		this.synchContext = synchContext;
	}
	
	boolean startClusterSynching() {
		
		ClusterMessage msg = new ClusterMessage();
		
		SynchHandler handler = new SynchHandler(synchContext, SynchType.UNICAST_ONE_OF)
				.withCallBack(new ClusterSynchCallback(synchContext))
				.withEncoder(null)
				.withoutCluster(synchContext.myId);
		
		handler.mode = SynchMode.SYNCH_CLUSTER;
		
		SynchFeature feature = handler.synch(msg).get();	
		
		if(feature != null && feature.get("-1").isSuccessful()) {

			synchContext.inStartup = false;
		}
		
		ClusterSnapshot snapshot = synchContext.getSnapshot();
		
		List<IMessage> messages = new ArrayList<IMessage>();
		
		for(Iterator<Member> it = snapshot.cluster.iterator(); it.hasNext();) {
			
			Member node = it.next();

			msg = new ClusterMessage(node.getId(),
					node.isUseSsl(),node.isAuthByKey(),node.getKey(),
					node.getLastModified(),
					node.getSynchAddresses(),
					node.isValid()?SynchMessage.COMMAND_TAKE_THis
							:SynchMessage.COMMAND_DEL_THis);
			messages.add(msg);
		}
		
		handler = new SynchHandler(synchContext, SynchType.UNICAST_ONE_OF)
				.withCallBack(new ClusterSynchCallback(synchContext))
				.withEncoder(null)
				.withoutCluster(synchContext.myId);
		handler.mode = SynchMode.SYNCH_CLUSTER;
		feature = handler.synch(messages).get();
		return !synchContext.inStartup;
	}
}
