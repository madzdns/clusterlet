package com.github.madzdns.clusterlet;

import com.github.madzdns.clusterlet.Member.ClusterAddress;
import com.github.madzdns.clusterlet.config.SocketBindConfig;
import com.github.madzdns.clusterlet.config.SocketConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.mina.transport.socket.nio.NioSocketAcceptor;

import java.io.IOException;
import java.net.*;
import java.util.*;

@Slf4j
public class SyncServer {
    private SocketBindConfig syncBindings;
    private SyncHandler handler;
    private SyncContext syncContext;
    private NioSocketAcceptor socket;

    public SyncServer(SyncHandler handler, SocketBindConfig syncBindings) {
        this.syncBindings = Objects.requireNonNull(syncBindings);
        this.handler = Objects.requireNonNull(handler);
        if (handler.isSender) {
            throw new IllegalStateException("SyncHandler should be of type server");
        }
        this.syncContext = Objects.requireNonNull(handler.syncContext);
    }

    public synchronized void start() throws IOException {
        if (socket != null) {
            throw new IllegalStateException("socket is already activated");
        }
        socket = new NioSocketAcceptor();
        socket.setHandler(handler);
        socket.setReuseAddress(true);
        List<SocketAddress> addz = new ArrayList<>();
        for (SocketConfig s : syncBindings.getSocketConfigs()) {
            if (s.getIp().equals(SocketConfig.ANY)) {
                for (InetAddress ia : NetHelper.getAllAddresses()) {
                    addz.add(new InetSocketAddress(ia, s.getPort()));
                }
            } else {
                addz.add(new InetSocketAddress(s.getIp(), s.getPort()));
            }
        }

        Set<ClusterAddress> myAddrzForSynch = new HashSet<>();
        for (SocketAddress so : addz) {
            myAddrzForSynch.add(new ClusterAddress(((InetSocketAddress) so).getAddress(), ((InetSocketAddress) so).getPort()));
        }

        if (addz.size() == 0) {
            return;
        }

        Member me = syncContext.getMyInfo();
        if (!me.isValid()) {
            log.error("I was disabled");
            return;
        }

        boolean changed = false;
        long lastModified = new Date().getTime();

        if (me.getSyncAddresses() == null) {
            changed = prepareSyncAddresses(myAddrzForSynch, me, lastModified);
        } else if (!me.getSyncAddresses().equals(myAddrzForSynch)) {
            changed = prepareSyncAddresses(myAddrzForSynch, me, lastModified);
        }
        if (changed) {
            syncContext.updateMember(me);
            syncContext.setVirtualLastModified(lastModified);
        }
        socket.bind(addz);
        log.debug("Clusterlet is listning on {} ", addz);
        new StartupManager(syncContext).startClusterSyncing();
    }

    private boolean prepareSyncAddresses(Set<ClusterAddress> myAddrzForSynch, Member me, long lastModified) {
        boolean changed;
        final Set<Short> awareIds = new HashSet<>();
        awareIds.add(syncContext.myId);
        me.setSyncAddresses(myAddrzForSynch);
        me.setLastModified(lastModified);
        me.setAwareIds(awareIds);
        changed = true;
        return changed;
    }

    private static class NetHelper {
        private static List<InetAddress> addresses = null;

        public static List<InetAddress> getAllAddresses() throws SocketException {
            if (addresses != null) {
                return addresses;
            }
            addresses = new ArrayList<>();
            Enumeration<NetworkInterface> nets = NetworkInterface.getNetworkInterfaces();
            for (NetworkInterface netint : Collections.list(nets)) {
                Enumeration<InetAddress> inetAddresses = netint.getInetAddresses();
                addresses.addAll(Collections.list(inetAddresses));
            }
            if (addresses.size() == 0) {
                addresses = null;
            }
            return addresses;
        }
    }

    public synchronized void stop() {
        if (socket == null) {
            return;
        }
        socket.unbind();
        socket.dispose();
    }
}
