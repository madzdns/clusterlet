package com.github.madzdns.clusterlet;

import java.net.ConnectException;
import java.util.ArrayList;
import java.util.List;

import javax.net.ssl.SSLContext;

import lombok.extern.slf4j.Slf4j;
import org.apache.mina.core.future.ConnectFuture;
import org.apache.mina.core.future.IoFutureListener;
import org.apache.mina.filter.ssl.SslFilter;

import com.github.madzdns.clusterlet.Member.ClusterAddress;
import com.github.madzdns.clusterlet.api.net.NetProvider;
import com.github.madzdns.clusterlet.api.net.compress.filter.MinaCompressionFilter;
import com.github.madzdns.clusterlet.codec.SynchMessage;
import com.github.madzdns.clusterlet.codec.mina.SynchMinaDecoder;
import com.github.madzdns.clusterlet.codec.mina.SynchMinaEncoder;

@Slf4j
class SynchSession {
    class MinaConnectListener implements IoFutureListener<ConnectFuture> {
        private String link = "";
        private SynchMessage msg;
        public MinaConnectListener(SynchMessage msg) {
            this.msg = msg;
        }

        @Override
        public void operationComplete(ConnectFuture connection) {
            Throwable e = connection.getException();
            if (e != null) {
                if (e instanceof ConnectException) {
                    log.error("{} to member {}", e.getMessage(), SynchSession.this.getFrNodeId());
                } else {
                    log.error("", e);
                }
                if (e instanceof ConnectException)
                    handler.workCallback(SynchSession.this, SynchHandler.STATE_WORK_FAILED, link);
            } else if (!connection.isConnected()) {
                handler.workCallback(SynchSession.this, SynchHandler.STATE_WORK_FAILED, link);

            } else {
                connection.getSession().setAttribute("SynchSession", SynchSession.this);
                log.debug("Connection is stablished in link {} ", link);
                connection.getSession().write(msg);
            }
        }

        public String getLink() {
            return link;
        }

        public void setLink(String remote, int port) {
            this.link = remote + ":" + port;
        }

    }

    private int currentSocket = 0;

    private int lastSocket = 0;

    private List<ClusterAddress> sockets = null;

    private Object mutx = new Object();

    private SynchHandler handler;

    private Member member = null;

    private boolean allTried = false;

    private boolean unproper = false;

    Object unproperMutx = new Object();

    public SynchSession(Member member, SynchHandler handler) {
        this.sockets = new ArrayList<>(member.getSynchAddresses());
        this.handler = handler;
        this.member = member;

        if (this.sockets.size() == 0) {
            member.setCurrentSocketIndex(currentSocket = -1);
        } else {
            currentSocket = member.getCurrentSocketIndex();
            lastSocket = currentSocket;
        }
    }

    public void setupNextSocket() throws Exception {
        if (currentSocket == -1) {
            log.error("No Socket Containers is defind");
            throw new Exception("No Socket Containers is defind");
        }
        currentSocket = (++currentSocket) % sockets.size();
        ClusterAddress c = sockets.get(currentSocket);
        if (log.isDebugEnabled()) {
            log.debug("Setting next socket {}:{} of edge {}", c.getAddress().getHostAddress(), c.getPort(), member.getId());
        }
        member.setCurrentSocketIndex(currentSocket);
        if (currentSocket == lastSocket) {
            allTried = true;
        }
    }

    public List<ClusterAddress> getSokets() {

        return sockets;
    }

    public void setSokets(List<ClusterAddress> sockets) {

        this.sockets = sockets;
    }

    public void sendMsg(SynchMessage msg) {

        synchronized (mutx) {

            if (currentSocket > -1
                    && handler != null) {

                ClusterAddress currentEdgeSocketAddr = sockets.get(currentSocket);

                SynchSocket socket =
                        new SynchSocket(currentEdgeSocketAddr.getAddress().getHostAddress(),
                                currentEdgeSocketAddr.getPort());

                socket.setHandler(handler);

                if (!member.isAuthByKey()) {

                    log.warn("no need to authenticate by key for member {}", getFrNodeId());
                    msg.setKeyChain(null);
                } else {

                    msg.setKeyChain(member.getKeyChain());
                }

                if (member.isUseSsl()) {

                    String cer = handler.synchContext.getConfig().getCertificatePath();
                    SSLContext ssl = null;

                    if (cer == null) {

                        ssl = NetProvider.getClientSslContext();
                        log.warn("Could not find any certificate file. Using SSL without verification enable");
                    } else {

                        ssl = NetProvider.getClientSslContext(cer);
                    }

                    if (ssl != null) {

                        SslFilter sslFilter = new SslFilter(ssl);

                        sslFilter.setUseClientMode(true);

                        socket.setFilter("ssl_filter", sslFilter);
                    }
                }

                socket.setFilter("compress_filter",
                        new MinaCompressionFilter());

                socket.setFilter("synchsocket_codec",
                        new SynchMinaEncoder(),
                        new SynchMinaDecoder());

                try {

                    socket.connect(this.new MinaConnectListener(msg));

                } catch (Exception e) {
                    log.error("", e);
                    handler.workCallback(this, SynchHandler.STATE_WORK_FAILED,
                            currentEdgeSocketAddr.getAddress().getHostAddress());
                }
            } else if (handler != null) {
                handler.workCallback(this, SynchHandler.STATE_WORK_FAILED, "");
            }
        }
    }

    public SynchHandler getHandler() {
        return handler;
    }

    public void setHandler(SynchHandler handler) {
        this.handler = handler;
    }

    public boolean isAllTried() {
        return allTried || currentSocket < 0;
    }

    public void setAllTried(boolean allTried) {
        this.allTried = allTried;
    }

    public short getFrNodeId() {
        if (member == null){
            return 0;}
        return member.getId();
    }

    public Member getMember() {
        return member;
    }

    public boolean isUnproper() {

        return unproper;
    }

    public void setUnproper(boolean unproper) {

        this.unproper = unproper;
    }

    @Override
    public String toString() {

        return String.valueOf(getFrNodeId());
    }
}
