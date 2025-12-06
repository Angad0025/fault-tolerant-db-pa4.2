package server.faulttolerance;

import com.datastax.driver.core.*;
import edu.umass.cs.nio.nioutils.NIOHeader;
import edu.umass.cs.nio.interfaces.NodeConfig;
import edu.umass.cs.nio.nioutils.NodeConfigUtils;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import server.ReplicatedServer;

import java.io.IOException;
import java.io.FileInputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;

/**
 * ZooKeeper-backed fault-tolerant server.
 */
public class MyDBFaultTolerantServerZK extends server.MyDBSingleServer implements Watcher {

	public static final int SLEEP = 1000;
	public static final boolean DROP_TABLES_AFTER_TESTS = true;

	private static final String ZK_REQUESTS_PARENT = "/zk_requests";

	private final ZooKeeper zk;
	private final String zkConnect = "127.0.0.1:2181";
	private final int zkSessionTimeoutMs = 30_000;

	private final Cluster cluster;
	private final Session session;
	private final String keyspace;

	private final ScheduledExecutorService bgExecutor = Executors.newSingleThreadScheduledExecutor();

	private volatile long lastApplied = 0L;
	private final Object applyLock = new Object();

	private final ConcurrentHashMap<String, CountDownLatch> pendingLatches = new ConcurrentHashMap<>();

	private static final int MAX_LOG_SIZE = 10000;
	private static final long CLIENT_WAIT_MS = 30_000L;

	public MyDBFaultTolerantServerZK(NodeConfig<String> nodeConfig,
			String myID,
			InetSocketAddress isaDB)
			throws IOException {

		super(new InetSocketAddress(
				nodeConfig.getNodeAddress(myID),
				nodeConfig.getNodePort(myID) - ReplicatedServer.SERVER_PORT_OFFSET),
				isaDB,
				myID);

		this.keyspace = myID;

		try {
			String cassHost = isaDB.getAddress().getHostAddress();
			int cassPort = isaDB.getPort();
			this.cluster = Cluster.builder()
					.addContactPoint(cassHost)
					.withPort(cassPort)
					.build();
			this.session = cluster.connect(this.keyspace);
		} catch (Exception e) {
			throw new IOException("Failed to connect to Cassandra keyspace " + this.keyspace, e);
		}

		try {
			// We create the grade table here
			String createGradeTable = "CREATE TABLE IF NOT EXISTS " + keyspace + ".grade " +
					"(id int PRIMARY KEY, events list<int>)";
			session.execute(createGradeTable);
			System.out.println("[" + keyspace + "] Created grade table");

			String createMetaTable = "CREATE TABLE IF NOT EXISTS " + keyspace + ".zk_meta " +
					"(id text PRIMARY KEY, last_applied bigint)";
			session.execute(createMetaTable);

			ResultSet rs = session.execute(
					"SELECT last_applied FROM " + keyspace + ".zk_meta WHERE id='meta'");
			Row r = rs.one();
			if (r != null) {
				this.lastApplied = r.getLong("last_applied");
				System.out.println("[" + keyspace + "] Recovered lastApplied: " + lastApplied);
			} else {
				session.execute("INSERT INTO " + keyspace + ".zk_meta (id, last_applied) VALUES ('meta', 0)");
				this.lastApplied = 0L;
				System.out.println("[" + keyspace + "] Initialized lastApplied to 0");
			}
		} catch (Exception e) {
			System.err.println("[" + keyspace + "] ERROR creating/reading tables: " + e.getMessage());
			e.printStackTrace();
			throw new IOException("Failed to create/read tables: " + e.getMessage(), e);
		}

		try {
			this.zk = new ZooKeeper(zkConnect, zkSessionTimeoutMs, this);
			ensurePathExists(ZK_REQUESTS_PARENT);

			// Check if grade table is empty but lastApplied > 0
			// We MUST reset lastApplied to 0 to reprocess requests from ZK
			// Otherwise, servers will look for sequences > lastApplied which may not exist
			// Only do this check after ZK is connected so we can check for pending requests
			if (lastApplied > 0) {
				try {
					ResultSet gradeRs = session.execute("SELECT COUNT(*) as cnt FROM " + keyspace + ".grade");
					Row gradeRow = gradeRs.one();
					if (gradeRow != null && gradeRow.getLong("cnt") == 0) {
						// Table is empty - reset lastApplied to 0 so we can process new requests
						// This handles the case where the test framework cleared the table
						// We reset regardless of pending ZK requests to ensure we can process new requests
						List<String> pendingChildren = zk.getChildren(ZK_REQUESTS_PARENT, false);
						if (pendingChildren != null && !pendingChildren.isEmpty()) {
							System.out.println("[" + keyspace + "] Grade table empty but " + pendingChildren.size()
									+ " requests in ZK, resetting lastApplied from " + lastApplied
									+ " to 0 to reprocess from ZK");
						} else {
							System.out.println("[" + keyspace + "] Grade table empty, resetting lastApplied from "
									+ lastApplied + " to 0 (no pending requests in ZK)");
						}
						this.lastApplied = 0L;
						session.execute("UPDATE " + keyspace + ".zk_meta SET last_applied = 0 WHERE id='meta'");
					}
				} catch (Exception e) {
					System.out.println(
							"[" + keyspace + "] Could not check grade table (might not exist yet): " + e.getMessage());
				}
			}
		} catch (KeeperException e) {
			try {
				session.close();
			} catch (Exception ignore) {
			}
			try {
				cluster.close();
			} catch (Exception ignore) {
			}
			throw new IOException("Failed to initialize ZooKeeper: " + e.getMessage(), e);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			try {
				session.close();
			} catch (Exception ignore) {
			}
			try {
				cluster.close();
			} catch (Exception ignore) {
			}
			throw new IOException("Failed to initialize ZooKeeper (interrupted): " + e.getMessage(), e);
		} catch (Exception e) {
			try {
				session.close();
			} catch (Exception ignore) {
			}
			try {
				cluster.close();
			} catch (Exception ignore) {
			}
			throw e;
		}

		bgExecutor.scheduleWithFixedDelay(() -> {
			try {
				applyPendingRequests();
				gcOldRequestsIfNeeded();
			} catch (Throwable t) {
				t.printStackTrace();
			}
		}, 100, 100, TimeUnit.MILLISECONDS); // More frequent polling

		// Initial processing
		applyPendingRequests();

	}

	@Override
	protected void handleMessageFromClient(byte[] bytes, NIOHeader header) {
		String message = new String(bytes, StandardCharsets.UTF_8).trim();
		System.out.println("[" + keyspace + "] Received from client: " + message);

		if (message.isEmpty()) {
			try {
				clientMessenger.send(header.sndr, "".getBytes(StandardCharsets.UTF_8));
			} catch (IOException ignore) {
			}
			return;
		}

		String reqID;
		String cql;
		if (!message.contains("::")) {
			reqID = String.valueOf(System.currentTimeMillis());
			cql = message.trim();
		} else {
			String[] parts = message.split("::", 2);
			reqID = parts[0].trim();
			cql = parts[1].trim();
		}

		if (cql.isEmpty()) {
			try {
				String resp = reqID + "::Empty query";
				clientMessenger.send(header.sndr, resp.getBytes(StandardCharsets.UTF_8));
			} catch (IOException ignore) {
			}
			return;
		}

		System.out.println("[" + keyspace + "] Processing CQL: " + cql);

		String nodeName = null;
		try {
			String path = zk.create(
					ZK_REQUESTS_PARENT + "/req_",
					cql.getBytes(StandardCharsets.UTF_8),
					ZooDefs.Ids.OPEN_ACL_UNSAFE,
					CreateMode.PERSISTENT_SEQUENTIAL);
			nodeName = path.substring(path.lastIndexOf('/') + 1);
			System.out.println("[" + keyspace + "] Created ZK node: " + nodeName);

			CountDownLatch latch = new CountDownLatch(1);
			pendingLatches.put(nodeName, latch);

			applyPendingRequests();

			boolean applied = latch.await(CLIENT_WAIT_MS, TimeUnit.MILLISECONDS);
			pendingLatches.remove(nodeName);

			System.out.println("[" + keyspace + "] Request applied: " + applied);

			String resp = applied
					? (reqID + "::Execution Completed!")
					: (reqID + "::ERR");
			try {
				clientMessenger.send(header.sndr, resp.getBytes(StandardCharsets.UTF_8));
			} catch (IOException ignore) {
			}

		} catch (KeeperException | InterruptedException e) {
			e.printStackTrace();
			Thread.currentThread().interrupt();
			try {
				String resp = reqID + "::ERR";
				clientMessenger.send(header.sndr, resp.getBytes(StandardCharsets.UTF_8));
			} catch (IOException ignore) {
			}
		} catch (Exception e) {
			e.printStackTrace();
			try {
				String resp = reqID + "::ERR";
				clientMessenger.send(header.sndr, resp.getBytes(StandardCharsets.UTF_8));
			} catch (IOException ignore) {
			}
		}
	}

	protected void handleMessageFromServer(byte[] bytes, NIOHeader header) {
	}

	private void applyPendingRequests() {
		synchronized (applyLock) {
			try {
				// Set watch=true so we can see when new children are added
				List<String> children = zk.getChildren(ZK_REQUESTS_PARENT, true);
				if (children == null || children.isEmpty()) {
					return;
				}

				// Sorting children to ensure sequential processing
				Collections.sort(children, (a, b) -> {
					long seqA = seqFromNodeName(a);
					long seqB = seqFromNodeName(b);
					return Long.compare(seqA, seqB);
				});

				for (String node : children) {
					long seq = seqFromNodeName(node);
					if (seq <= lastApplied) {
						CountDownLatch latch = pendingLatches.remove(node);
						if (latch != null)
							latch.countDown();
						// Only delete if we're far enough ahead to avoid race conditions
						// Letting garbage collection handle most cleanup
						if (lastApplied - seq > MAX_LOG_SIZE / 2) {
							try {
								String path = ZK_REQUESTS_PARENT + "/" + node;
								zk.delete(path, -1);
							} catch (Exception ignore) {
							}
						}
					}
				}

				// Process requests in strict sequential order
				long expectedSeq = lastApplied + 1;

				boolean foundExpected = false;
				for (String node : children) {
					long seq = seqFromNodeName(node);
					if (seq == expectedSeq) {
						foundExpected = true;
						break;
					}
				}

				if (!foundExpected && !children.isEmpty()) {

					long minSeq = Long.MAX_VALUE;
					for (String node : children) {
						long seq = seqFromNodeName(node);
						if (seq > lastApplied && seq < minSeq) {
							minSeq = seq;
						}
					}
					if (minSeq != Long.MAX_VALUE) {
						expectedSeq = minSeq;
						System.out.println("[" + keyspace + "] Catching up: processing minimum available sequence "
								+ expectedSeq + " (lastApplied=" + lastApplied + ")");
					}
				}

				
				int maxIterations = 1000; // This is the Safety limit
				int iterations = 0;
				while (iterations < maxIterations) {
					iterations++;

					children = zk.getChildren(ZK_REQUESTS_PARENT, true);
					if (children == null || children.isEmpty())
						break;

					Collections.sort(children, (a, b) -> {
						long seqA = seqFromNodeName(a);
						long seqB = seqFromNodeName(b);
						return Long.compare(seqA, seqB);
					});

					String targetNode = null;
					for (String node : children) {
						long seq = seqFromNodeName(node);
						if (seq == expectedSeq) {
							targetNode = node;
							break;
						}
					}

					if (targetNode == null) {
						break;
					}

					System.out.println("[" + keyspace + "] Processing sequence " + expectedSeq + " (lastApplied="
							+ lastApplied + ", expected=" + expectedSeq + ")");

					String path = ZK_REQUESTS_PARENT + "/" + targetNode;
					Stat st = zk.exists(path, false);
					if (st == null) {
						expectedSeq++;
						continue;
					}
					byte[] data = zk.getData(path, false, st);
					if (data == null)
						data = new byte[0];
					String cql = new String(data, StandardCharsets.UTF_8).trim();

					if (cql.isEmpty()) {
						// Empty request, just mark as applied
						lastApplied = expectedSeq;
						persistLastApplied(lastApplied);
						CountDownLatch latch = pendingLatches.remove(targetNode);
						if (latch != null)
							latch.countDown();
						try {
							zk.delete(path, -1);
						} catch (Exception ignore) {
						}
						expectedSeq++;
						continue;
					}


					String qualifiedCql = cql;
					String lowerCql = cql.toLowerCase();
					if (!lowerCql.contains(keyspace.toLowerCase() + ".")) {
						qualifiedCql = cql.replaceAll("(?i)\\b(insert\\s+into)\\s+(grade|zk_meta)(\\s|$)",
								"$1 " + keyspace + ".$2$3");
						qualifiedCql = qualifiedCql.replaceAll("(?i)\\b(update)\\s+(grade|zk_meta)(\\s|$)",
								"$1 " + keyspace + ".$2$3");
						qualifiedCql = qualifiedCql.replaceAll("(?i)\\b(from)\\s+(grade|zk_meta)(\\s|$)",
								"$1 " + keyspace + ".$2$3");
						qualifiedCql = qualifiedCql.replaceAll("(?i)\\b(truncate)\\s+(grade|zk_meta)(\\s|$)",
								"$1 " + keyspace + ".$2$3");
						qualifiedCql = qualifiedCql.replaceAll("(?i)\\b(drop\\s+table)\\s+(grade|zk_meta)(\\s|$)",
								"$1 " + keyspace + ".$2$3");
						qualifiedCql = qualifiedCql.replaceAll("(?i)\\b(create\\s+table)\\s+(grade|zk_meta)(\\s|$)",
								"$1 " + keyspace + ".$2$3");
						qualifiedCql = qualifiedCql.replaceAll(
								"(?i)\\btable\\s+if\\s+not\\s+exists\\s+(grade|zk_meta)(\\s|$)",
								"table if not exists " + keyspace + ".$2$3");

						System.out.println("[" + keyspace + "] Sequence " + expectedSeq + " - Qualified CQL: "
								+ qualifiedCql + " (original: " + cql + ")");
					} else {
						System.out.println("[" + keyspace + "] Sequence " + expectedSeq
								+ " - Executing CQL (already qualified): " + cql);
					}

					try {
						session.execute(qualifiedCql);
						System.out.println("[" + keyspace + "] Successfully executed CQL: " + qualifiedCql);
					} catch (Exception e) {
						System.err.println("[" + keyspace + "] ERROR executing CQL: " + qualifiedCql);
						System.err.println("[" + keyspace + "] Original CQL was: " + cql);
						e.printStackTrace();

						break;
					}

					lastApplied = expectedSeq;
					persistLastApplied(lastApplied);

					CountDownLatch latch = pendingLatches.remove(targetNode);
					if (latch != null)
						latch.countDown();


					expectedSeq++;
				}
			} catch (KeeperException | InterruptedException ke) {
				System.err.println("[" + keyspace + "] Error in applyPendingRequests: " + ke.getMessage());
				ke.printStackTrace();
			}
		}
	}

	private void persistLastApplied(long v) {
		try {
			session.execute("UPDATE " + keyspace + ".zk_meta SET last_applied = " + v + " WHERE id='meta'");
		} catch (Exception e) {
			System.err.println("[" + keyspace + "] ERROR persisting lastApplied: " + e.getMessage());
			e.printStackTrace();
		}
	}

	private long seqFromNodeName(String nodeName) {

		int idx = nodeName.lastIndexOf('_');
		if (idx < 0) {
			System.err.println("[" + keyspace + "] WARNING: Invalid node name format: " + nodeName);
			return 0L;
		}
		String suf = nodeName.substring(idx + 1);
		try {
			long seq = Long.parseLong(suf);
			return seq;
		} catch (NumberFormatException e) {
			System.err.println("[" + keyspace + "] WARNING: Could not parse sequence from node name: " + nodeName
					+ ", suffix: " + suf);
			return Math.abs(nodeName.hashCode());
		}
	}

	private void ensurePathExists(String path) throws KeeperException, InterruptedException {
		Stat st = zk.exists(path, false);
		if (st == null) {
			try {
				zk.create(path, new byte[0],
						ZooDefs.Ids.OPEN_ACL_UNSAFE,
						CreateMode.PERSISTENT);
				System.out.println("[" + keyspace + "] Created ZK path: " + path);
			} catch (KeeperException.NodeExistsException e) {
				// Another server might have created it
				System.out.println("[" + keyspace + "] ZK path already exists: " + path);
			} catch (KeeperException e) {
				System.err.println("[" + keyspace + "] ERROR creating ZK path " + path + ": " + e.getMessage());
				throw e;
			}
		} else {
			System.out.println("[" + keyspace + "] ZK path already exists: " + path);
		}
	}

	private void gcOldRequestsIfNeeded() {
		try {
			List<String> children = zk.getChildren(ZK_REQUESTS_PARENT, false);
			if (children == null)
				return;
			Collections.sort(children);
			if (children.size() <= MAX_LOG_SIZE)
				return;

			int toDelete = children.size() - MAX_LOG_SIZE;
			for (int i = 0; i < children.size() && toDelete > 0; i++) {
				String node = children.get(i);
				long seq = seqFromNodeName(node);
				if (seq <= lastApplied) {
					try {
						zk.delete(ZK_REQUESTS_PARENT + "/" + node, -1);
					} catch (Exception ignore) {
					}
					toDelete--;
				} else {
					break;
				}
			}
		} catch (KeeperException | InterruptedException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void process(WatchedEvent event) {
		if (event.getType() == Event.EventType.None) {
			if (event.getState() == Event.KeeperState.Expired) {
				System.err.println("ZooKeeper session expired; shutting down resources.");
				close();
			}
		} else {
			if (event.getPath() != null && event.getPath().equals(ZK_REQUESTS_PARENT)) {
				applyPendingRequests();
			}
		}
	}

	@Override
	public void close() {
		try {
			bgExecutor.shutdownNow();
		} catch (Exception ignored) {
		}
		try {
			zk.close();
		} catch (Exception ignored) {
		}
		try {
			session.close();
			cluster.close();
		} catch (Exception ignored) {
		}
		try {
			super.close();
		} catch (Exception ignore) {
		}
	}

    // This is the main method that is used to start the server
	public static void main(String[] args) {
		if (args.length < 2) {
			System.err.println("Usage: MyDBFaultTolerantServerZK <config_file> <server_name>");
			System.exit(1);
		}

		String configFile = args[0]; 
		String serverName = args[1]; 

		
		Properties props = new Properties();
		try (FileInputStream fis = new FileInputStream(configFile)) {
			props.load(fis);
		} catch (IOException e) {
			System.err.println("Failed to load config file: " + configFile);
			e.printStackTrace();
			System.exit(1);
		}

		
		String propertyKey = "server." + serverName;
		String serverAddress = props.getProperty(propertyKey);

		if (serverAddress == null) {
			System.err.println("Server property '" + propertyKey + "' not found in config file " + configFile);
			System.err.println("Available properties:");
			for (String key : props.stringPropertyNames()) {
				System.err.println("  " + key + " = " + props.getProperty(key));
			}
			System.exit(1);
		}

	
		String[] parts = serverAddress.split(":");
		if (parts.length != 2) {
			System.err.println("Invalid server address format: " + serverAddress +
					" (expected format: host:port)");
			System.exit(1);
		}
		String host = parts[0];
		int port = Integer.parseInt(parts[1]);

		NodeConfig<String> nodeConfig;
		try {
			nodeConfig = NodeConfigUtils.getNodeConfigFromFile(
					configFile,
					ReplicatedServer.SERVER_PREFIX,
					ReplicatedServer.SERVER_PORT_OFFSET);
		} catch (IOException e) {
			System.err.println("Failed to load node config: " + e.getMessage());
			e.printStackTrace();
			System.exit(1);
			return;
		}

		
		try {
			System.out.println("Starting MyDBFaultTolerantServerZK: " + serverName + " at " + host + ":" + port);
			MyDBFaultTolerantServerZK server = new MyDBFaultTolerantServerZK(
					nodeConfig,
					serverName,
					new InetSocketAddress("localhost", 9042) // Cassandra address
			);
			System.out.println("MyDBFaultTolerantServerZK " + serverName + " started successfully");

			// Keep the server running
			// The server will continue to run and process requests
		} catch (Exception e) {
			System.err.println("Failed to start MyDBFaultTolerantServerZK: " + e.getMessage());
			e.printStackTrace();
			System.exit(1);
		}
	}
}
