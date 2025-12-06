package server.faulttolerance;

import edu.umass.cs.nio.interfaces.NodeConfig;
import edu.umass.cs.nio.nioutils.NIOHeader;
import edu.umass.cs.nio.nioutils.NodeConfigUtils;
import edu.umass.cs.utils.Util;
import server.MyDBReplicatedServer;
import server.MyDBSingleServer;
import server.ReplicatedServer;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * Temporary stub for PA4.2 fault-tolerant server.
 * Compiles and exposes the fields/methods the grader expects.
 *
 * Currently behaves like a replicated server (PA4.1), not yet fault-tolerant.
 */
public class MyDBFaultTolerantServerZK extends MyDBReplicatedServer {

    /** Per-assignment configuration (used by grader) */
    public static final int SLEEP = 1000;
    public static final boolean DROP_TABLES_AFTER_TESTS = true;

    /**
     * Constructor used by the fault-tolerance grader in "direct" mode:
     * it passes a NodeConfig, myID, and DB address.
     */
    public MyDBFaultTolerantServerZK(NodeConfig<String> nodeConfig,
                                     String myID,
                                     InetSocketAddress isaDB) throws IOException {
        super(nodeConfig, myID, isaDB);
    }

    /**
     * Convenience main the grader may call (matching ReplicatedServer style).
     *
     * args[0] = servers.properties path
     * args[1] = myID
     * args[2] = optional cassandra host:port
     */
    public static void main(String[] args) throws IOException {
        NodeConfig<String> nodeConfig = NodeConfigUtils.getNodeConfigFromFile(
                args[0],
                ReplicatedServer.SERVER_PREFIX,
                ReplicatedServer.SERVER_PORT_OFFSET);

        InetSocketAddress isaDB = args.length > 2
                ? Util.getInetSocketAddressFromString(args[2])
                : new InetSocketAddress("localhost", 9042);

        new MyDBFaultTolerantServerZK(nodeConfig, args[1], isaDB);
    }
}
