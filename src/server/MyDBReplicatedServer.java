package server;

import edu.umass.cs.nio.interfaces.NodeConfig;
import edu.umass.cs.nio.nioutils.NIOHeader;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;

/**
 * Leader-based replicated DB server for PA4.1.
 */
public class MyDBReplicatedServer extends MyDBSingleServer {
    private final String myID;
    private final edu.umass.cs.nio.MessageNIOTransport<String,String> serverMessenger;
    private volatile String leaderId;
    private final Object leaderLock = new Object();

    // sequence assignment (leader only)
    private final AtomicLong nextSeq = new AtomicLong(1);

    // pending writes: seq -> (cql, clientAddress, requestId)
    private static class Pending {
        final String cql;
        final InetSocketAddress client;
        final String requestId;
        Pending(String cql, InetSocketAddress client, String requestId) {
            this.cql = cql; this.client = client; this.requestId = requestId;
        }
    }
    private final Map<Long, Pending> pending = new ConcurrentHashMap<>();
    private volatile long nextToApply = 1;
    private final Object applyLock = new Object();

    public MyDBReplicatedServer(NodeConfig<String> nodeConfig, String myID,
                                InetSocketAddress isaDB) throws IOException {
        super(new InetSocketAddress(nodeConfig.getNodeAddress(myID),
                nodeConfig.getNodePort(myID) - ReplicatedServer.SERVER_PORT_OFFSET),
              isaDB, myID);
        this.myID = myID;

        this.serverMessenger = new edu.umass.cs.nio.MessageNIOTransport<>(
                myID, nodeConfig,
                new edu.umass.cs.nio.AbstractBytePacketDemultiplexer() {
                    @Override
                    public boolean handleMessage(byte[] bytes, NIOHeader nioHeader) {
                        handleMessageFromServer(bytes, nioHeader);
                        return true;
                    }
                }, true);

        electLeader(nodeConfig);
        log.log(Level.INFO, "MyDBReplicatedServer {0} started; leader={1}",
                new Object[]{myID, leaderId});
    }

    private void electLeader(NodeConfig<String> nodeConfig) {
        List<String> ids = new ArrayList<>(nodeConfig.getNodeIDs());
        Collections.sort(ids);
        leaderId = ids.get(0);
    }

    private boolean isLeader() { return myID.equals(leaderId); }

    @Override
    protected void handleMessageFromClient(byte[] bytes, NIOHeader header) {
        String msg = new String(bytes, StandardCharsets.UTF_8).trim();
        if (msg.isEmpty()) return;

        // Parse client msg: optional requestId|CQL
        String requestId;
        String cql;
        String[] parts = msg.split("\\|", 2);
        if (parts.length == 2) {
            requestId = parts[0];
            cql = parts[1];
        } else {
            requestId = UUID.randomUUID().toString();
            cql = msg;
        }

        if (isLeader()) {
            onClientWriteAtLeader(requestId, cql, header.sndr);
        } else {
            sendForwardToLeader(requestId, cql, header.sndr);
        }
    }

    private void sendForwardToLeader(String requestId, String cql, InetSocketAddress clientAddr) {
        try {
            String base64 = Base64.getEncoder().encodeToString(cql.getBytes(StandardCharsets.UTF_8));
            String line = "FORWARD|" + requestId + "|" + base64;
            // encode client address into requestId if needed (for simplicity, omitted here)
            serverMessenger.send(leaderId, line.getBytes(StandardCharsets.UTF_8));
        } catch (IOException e) {
            log.log(Level.WARNING, "Error forwarding to leader", e);
        }
    }

    private void onClientWriteAtLeader(String requestId, String cql, InetSocketAddress clientAddr) {
        long seq = nextSeq.getAndIncrement();
        // record pending
        pending.put(seq, new Pending(cql, clientAddr, requestId));

        String base64 = Base64.getEncoder().encodeToString(cql.getBytes(StandardCharsets.UTF_8));
        String orderLine = "ORDER|" + seq + "|" + requestId + "|" + base64;
        multicastOrder(orderLine);
    }

    private void multicastOrder(String orderLine) {
        byte[] bytes = orderLine.getBytes(StandardCharsets.UTF_8);
        for (String node : serverMessenger.getNodeConfig().getNodeIDs()) {
            try {
                serverMessenger.send(node, bytes);
            } catch (IOException e) {
                log.log(Level.WARNING, "Error sending ORDER to " + node, e);
            }
        }
    }

    protected void handleMessageFromServer(byte[] bytes, NIOHeader header) {
        String msg = new String(bytes, StandardCharsets.UTF_8).trim();
        if (msg.startsWith("FORWARD|") && isLeader()) {
            onForwardLine(msg, header);
        } else if (msg.startsWith("ORDER|")) {
            onOrderLine(msg);
        }
    }

    private void onForwardLine(String line, NIOHeader header) {
        // FORWARD|requestId|base64CQL
        String[] parts = line.split("\\|", 3);
        if (parts.length != 3) return;
        String requestId = parts[1];
        String cql = new String(Base64.getDecoder().decode(parts[2]), StandardCharsets.UTF_8);
        // For simplicity, reply to header.sndr’s client; real code would encode client in requestId
        onClientWriteAtLeader(requestId, cql, header.sndr);
    }

    private void onOrderLine(String line) {
        // ORDER|seq|requestId|base64CQL
        String[] parts = line.split("\\|", 4);
        if (parts.length != 4) return;
        long seq = Long.parseLong(parts[1]);
        String requestId = parts[2];
        String cql = new String(Base64.getDecoder().decode(parts[3]), StandardCharsets.UTF_8);

        // Followers need to create Pending entries too; they don't know clientAddr here in this sketch.
        pending.putIfAbsent(seq, new Pending(cql, null, requestId));
        tryApplyInOrder();
    }

    private void tryApplyInOrder() {
        synchronized (applyLock) {
            while (true) {
                Pending p = pending.get(nextToApply);
                if (p == null) break;
                try {
                    // execute CQL using session in MyDBSingleServer (not shown here)
                    // session.execute(p.cql);
                } catch (Exception e) {
                    log.log(Level.WARNING, "Error executing CQL", e);
                }
                // reply to client if we know address
                if (p.client != null) {
                    try {
                        String resp = "OK|" + p.requestId;
                        clientMessenger.send(p.client, resp.getBytes(StandardCharsets.UTF_8));
                    } catch (IOException e) {
                        log.log(Level.WARNING, "Error replying to client", e);
                    }
                }
                pending.remove(nextToApply);
                nextToApply++;
            }
        }
    }

    public void close() {
        super.close();
        serverMessenger.stop();
    }
}
