package com.github.madzdns.clusterlet;

import com.github.madzdns.clusterlet.codec.ClusterMessage;
import com.github.madzdns.clusterlet.codec.IMessage;
import com.github.madzdns.clusterlet.codec.SyncMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

public class ClusterSyncCallback implements ISyncCallback {

    private static Logger log = LoggerFactory.getLogger(ClusterSyncCallback.class);

    SyncContext syncContext;

    public ClusterSyncCallback(SyncContext syncContext) {

        this.syncContext = syncContext;
    }

    @Override
    public boolean callBack(ISession session, IMessage message,
                            Set<Short> withNodes, ISyncProtocolOutput out) {

        boolean result = false;

        ClusterMessage outMsg = null;

        ClusterMessage e = (ClusterMessage) message;

        if (e.getId() == -1) {
            //Means its in startup

            ClusterSnapshot snapshot = syncContext.getSnapshot();

            if (snapshot == null) {

                return result;
            }

            if (snapshot.cluster == null ||
                    snapshot.cluster.size() == 0) {

                return result;
            }

            List<IMessage> responses = new ArrayList<IMessage>();

            for (Iterator<Member> it = snapshot.cluster.iterator(); it.hasNext(); ) {

                Member node = it.next();

                outMsg = new ClusterMessage(node.getId(),
                        node.isUseSsl(), node.isAuthByKey(), node.getKey(),
                        node.getLastModified(),
                        node.getSyncAddresses(),
                        node.isValid() ? SyncMessage.COMMAND_TAKE_THis
                                : SyncMessage.COMMAND_DEL_THis);

                responses.add(outMsg);
            }

            out.write(responses);
            return true;
        }

        //TODO check if sending Node is invalid in our database and its modification version is higher

        Member subjectNode = syncContext.getMemberById(e.getId());
        Member node = null;

        if (subjectNode == null) {

            if (e.getCommand() == SyncMessage.COMMAND_DEL_THis) {

                node = new Member(e.getId(),
                        e.getSyncAddresses(),
                        e.isUseSsl(), e.isAuthByKey(),
                        e.getCredentionalKey(), e.getVersion(),
                        withNodes, Member.STATE_DEL);

                syncContext.updateMember(node);
                //Send an OK

                outMsg = new ClusterMessage(e.getId(),
                        e.isUseSsl(), e.isAuthByKey(),
                        e.getCredentionalKey(),
                        e.getVersion(), null, SyncMessage.COMMAND_OK);

                result = true;
            } else if (e.getCommand() == SyncMessage.COMMAND_GIVE_THis
                    || e.getCommand() == SyncMessage.COMMAND_OK
                    || e.getCommand() == SyncMessage.COMMAND_RCPT_THis) {
                /*
                 * I checked every possibilities and this situation can't happen.
                 * Anyway for the sake of completeness, I put it here
                 */
                result = false;
            } else /*TAKE_THIS*/ {

                node = new Member(e.getId(),
                        e.getSyncAddresses(),
                        e.isUseSsl(), e.isAuthByKey(),
                        e.getCredentionalKey(), e.getVersion(),
                        withNodes, Member.STATE_VLD);

                syncContext.updateMember(node);

                outMsg = new ClusterMessage(e.getId(),
                        e.isUseSsl(), e.isAuthByKey(),
                        e.getCredentionalKey(),
                        e.getVersion(), null,
                        SyncMessage.COMMAND_OK);
                result = true;
            }
        } else {

            log.debug("my node[{},{}], rcvd node []", subjectNode.getId(), subjectNode.getLastModified(), e.getId(), e.getVersion());
            //Thats why everyone starts synchronizing, first asks others to TAKE its edges
            if (e.getCommand() == SyncMessage.COMMAND_GIVE_THis) {
                outMsg = new ClusterMessage(e.getId(),
                        subjectNode.isUseSsl(), subjectNode.isAuthByKey(), subjectNode.getKey(),
                        subjectNode.getLastModified(),
                        subjectNode.getSyncAddresses(),
                        subjectNode.isValid() ? SyncMessage.COMMAND_TAKE_THis
                                : SyncMessage.COMMAND_DEL_THis);

                result = false;
            } else if (e.getCommand() == SyncMessage.COMMAND_DEL_THis) {

                if (subjectNode.isValid()) {

                    if (e.getVersion() > subjectNode.getLastModified()) {

                        node = new Member(e.getId(),
                                e.getSyncAddresses(),

                                e.isUseSsl(), e.isAuthByKey(),
                                e.getCredentionalKey(), e.getVersion(),
                                withNodes, Member.STATE_DEL);

                        syncContext.updateMember(node);

                        outMsg = new ClusterMessage(e.getId(),
                                e.isUseSsl(), e.isAuthByKey(), e.getCredentionalKey(),
                                e.getVersion(), null, SyncMessage.COMMAND_OK);
                        result = true;
                    } else if (e.getVersion() < subjectNode.getLastModified()) {

                        outMsg = new ClusterMessage(e.getId(),
                                subjectNode.isUseSsl(), subjectNode.isAuthByKey(), subjectNode.getKey(),
                                subjectNode.getLastModified(),
                                subjectNode.getSyncAddresses(),
                                SyncMessage.COMMAND_TAKE_THis);
                        result = false;
                    }
                    //else I assume its impossible for the same lastModified, one has an edge deleted, one does not
                } else {
                    /*
                     * This means we are both have this deleted. So just send OK
                     */
                    /*
                     * I don't think its matter to check modification here. But be careful,
                     * if use send back OK here using your own lastModified, it leads to a loop.
                     * Thats why I've used e.getLastModified() here
                     */

                    subjectNode.addAwareId(withNodes);
                    outMsg = new ClusterMessage(e.getId(),
                            e.isUseSsl(), e.isAuthByKey(),
                            e.getCredentionalKey(),
                            e.getVersion(),
                            null, SyncMessage.COMMAND_OK);
                    result = true;
                }
            } else /*He sent TAKE or OK or RCPT*/ {

                if (e.getVersion() == subjectNode.getLastModified()) {

                    if (e.getCommand() == SyncMessage.COMMAND_TAKE_THis) {

                        /*
                         * If we both have the same edge, I just update my edges awareIds From
                         * what he has and send him OK
                         */

                        /*
                         * If we got a take message that we already
                         * have that one, there might be coming from
                         * a restarted edge. So for a temporary fix,
                         * we make that edge as not scheduled
                         */

                        subjectNode.setScheduled(false);

                        subjectNode.addAwareId(withNodes);

                        outMsg = new ClusterMessage(e.getId(),
                                e.isUseSsl(), e.isAuthByKey(), e.getCredentionalKey(),
                                e.getVersion(),
                                null, SyncMessage.COMMAND_OK);
                        result = true;
                    } else { //OK or RCPT
                        withNodes.add(e.getId());
                        subjectNode.addAwareId(withNodes);
                        if (e.getCommand() == SyncMessage.COMMAND_RCPT_THis) {
                            outMsg = new ClusterMessage(e.getId(),
                                    subjectNode.isUseSsl(), subjectNode.isAuthByKey(),
                                    subjectNode.getKey(),
                                    subjectNode.getLastModified(),
                                    null, SyncMessage.COMMAND_OK);
                        } else {//this is an OK

                        }
                        result = true;
                    }
                } else if (e.getVersion() > subjectNode.getLastModified()) {
                    if (e.getCommand() == SyncMessage.COMMAND_TAKE_THis) {
                        node = new Member(e.getId(),
                                e.getSyncAddresses(),
                                e.isUseSsl(), e.isAuthByKey(),
                                e.getCredentionalKey(), e.getVersion(),
                                withNodes, Member.STATE_VLD);

                        syncContext.updateMember(node);

                        outMsg = new ClusterMessage(e.getId(),
                                e.isUseSsl(), e.isAuthByKey(),
                                e.getCredentionalKey(),
                                e.getVersion(), null, SyncMessage.COMMAND_OK);

                        result = true;
                    } else {

                        /*
                         * If its an OK or RCPT message but its modification somehow is bigger
                         * than mine, I know its partial, so I ask complete information
                         * about the edge
                         */

                        outMsg = new ClusterMessage(e.getId(),
                                e.isUseSsl(), e.isAuthByKey(),
                                e.getCredentionalKey(),
                                e.getVersion(), null, SyncMessage.COMMAND_GIVE_THis);

                        result = false;
                    }
                } else /*if(e.getLastModified()<edge.getLastModified())*/ {

                    /*
                     * It does not matter this is a TAKE or OK command anymore. Its modification time
                     * is smaller than mine and should send him the new one
                     */


                    outMsg = new ClusterMessage(e.getId(),
                            subjectNode.isUseSsl(), subjectNode.isAuthByKey(),
                            subjectNode.getKey(), subjectNode.getLastModified(),
                            subjectNode.getSyncAddresses(),
                            subjectNode.isValid() ? SyncMessage.COMMAND_TAKE_THis
                                    : SyncMessage.COMMAND_DEL_THis);

                    result = false;
                }
            }
        }

        out.write(outMsg);
        return result;
    }

    @Override
    public void result(SyncFeature syncFeature) {
        // TODO Auto-generated method stub
    }
}
