package server.faulttolerance;

import edu.umass.cs.nio.nioutils.NIOHeader;
import server.MyDBSingleServer;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * Temporary stub for PA4.2 fault-tolerant server.
 * Compiles but does not yet implement fault tolerance.
 *
 * This class just behaves like a single server so that
 * the autograder can compile and run tests.
 */
public class MyDBFaultTolerantServerZK extends MyDBSingleServer {

    /**
     * Constructor used by the grader when starting this server directly.
     */
    public MyDBFaultTolerantServerZK(InetSocketAddress isa,
                                     InetSocketAddress isaDB,
                                     String keyspace) throws IOException {
        super(isa, isaDB, keyspace);
    }

    /**
     * Constructor matching the replicated server-style constructor
     * (nodeConfig-based) may be added later if needed.
     */

    @Override
    protected void handleMessageFromClient(byte[] bytes, NIOHeader header) {
        // For now, just behave like a normal single server (echo / DB op)
        super.handleMessageFromClient(bytes, header);
    }

    // No extra server-to-server handling in this stub.
}
