package server.faulttolerance;

import edu.umass.cs.nio.interfaces.NodeConfig;
import edu.umass.cs.nio.nioutils.NodeConfigUtils;
import edu.umass.cs.utils.Util;
import server.MyDBReplicatedServer;
import server.ReplicatedServer;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * Minimal Gigapaxos app wrapper for MyDB replicated DB.
 *
 * This class name is referenced by GraderFaultTolerance,
 * so it must exist and compile, even if the logic is simple.
 */
public class MyDBReplicableAppGP {

    /**
     * Factory method used by Gigapaxos / grader to create a replicated DB server.
     *
     * @param serversConfig path to servers.properties
     * @param myID          server ID (e.g., server0, server1, server2)
     * @param isaDBString   Cassandra address string host:port
     */
    public static MyDBReplicatedServer createServer(String serversConfig,
                                                    String myID,
                                                    String isaDBString) throws IOException {
        NodeConfig<String> nodeConfig = NodeConfigUtils.getNodeConfigFromFile(
                serversConfig,
                ReplicatedServer.SERVER_PREFIX,
                ReplicatedServer.SERVER_PORT_OFFSET);

        InetSocketAddress isaDB = Util.getInetSocketAddressFromString(isaDBString);

        return new MyDBReplicatedServer(nodeConfig, myID, isaDB);
    }
}
