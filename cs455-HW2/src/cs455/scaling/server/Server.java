package cs455.scaling.server;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.Set;

import cs455.scaling.hash.Hash;

/**
 * There is exactly one server node in the system. The server node provides the following functions:
 * A. Accepts incoming network connections from the clients.
 * B. Accepts incoming traffic from these connections
 * C. Groups data from the clients together into batches
 * D. Replies to clients by sending back a hash code for each message received.
 * E. The server performs functions A, B, C, and D by relying on the thread pool.
 * 
 * Executed with the following command:
 * java cs455.scaling.server.Server portnum thread-pool-size batch-size batch-time
 */

public class Server {
	
	private final int portNumber;
	private final int threadPoolSize;
	private final int batchSize;
	private final int batchTime;
	private static ThreadPoolManager threadPoolManager;
	private ServerSocketChannel serverSocket;
	private Selector selector;
	private static ServerStatistics serverStatistics;
	private static Hash hash = new Hash();
	
	public Server(int portNumber, int threadPoolSize, int batchSize, int batchTime) {
		this.portNumber = portNumber;
		this.threadPoolSize = threadPoolSize;
		this.batchSize = batchSize;
		this.batchTime = batchTime;
		this.serverStatistics = new ServerStatistics();
		this.threadPoolManager = new ThreadPoolManager(threadPoolSize, batchSize, batchTime);
	}
	
	// sends an acknowledgement to the client
	public static void main(String[] args) {
	
		// requires 4 arguments to initialize a server
		if(args.length != 4) {
		    System.out.println("Invalid Arguments. Must include a Port Number, Thread Pool Size, Batch Size and Batch Time");
		    return;
		}
		
		int serverPortNumber = 0;
		int threadPoolSize = 0;
		int batchSize = 0;
		int batchTime = 0;
		
		try {
			serverPortNumber = Integer.parseInt(args[0]);
			threadPoolSize = Integer.parseInt(args[1]);
			batchSize = Integer.parseInt(args[2]);
			batchTime = Integer.parseInt(args[3]);
		} catch (NumberFormatException nfe) {
			System.out.println("Invalid argument. Argument must be a number.");
			nfe.printStackTrace();
		}
		Server server = new Server(serverPortNumber, threadPoolSize, batchSize, batchTime);

		try {
			server.startServer(serverPortNumber);
			server.startServerStatisticsThread();
			server.startThreadPoolManagerThread();
			server.serverLoop();
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
	}
	
	private void startServer(int portNumber) throws IOException {
		System.out.println("Starting Server begin.");
		try {
			// Open the selector
			this.selector = Selector.open();
			
			// Create our input channel
			serverSocket = ServerSocketChannel.open();
			serverSocket.bind(new InetSocketAddress(InetAddress.getLocalHost().getHostAddress(), portNumber));
			serverSocket.configureBlocking(false);
			
			// Register our channel to the selector
			serverSocket.register(this.selector, SelectionKey.OP_ACCEPT);

		} catch (UnknownHostException uhe) {
			uhe.printStackTrace();
		}
	}
	
	// start the thread for displaying server statistics
	private void startServerStatisticsThread() {
		new Thread(serverStatistics).start();
	}
	
	// start the thread for the Thread Pool Manager
	private void startThreadPoolManagerThread() {
		new Thread(threadPoolManager).start();
	}
	
	private void serverLoop() throws IOException {
		while (true) {
			// Loop on selector
			// Block here
            this.selector.select();

            // Key(s) are ready
            Set<SelectionKey> selectedKeys = selector.selectedKeys();
         // Loop over ready keys
            Iterator<SelectionKey> iter = selectedKeys.iterator();
            while (iter.hasNext()) {
            	// Grab current key
                SelectionKey key = iter.next();
                
                // consider turning off syncing here as it may be causing overhead
                synchronized (key) {
                	// Optional
                    if(key.isValid() == false) { 
                        continue; 
                    }

                    // New connection on serverSocket
                    if (key.isAcceptable()) {
                        register(selector, serverSocket);
                    }
     
                    // Previous connection has data to read
                    if (key.isReadable() && key.attachment() != null) {
                        readAndRespond(key);
                    }
                }
                // Remove it from our set
                iter.remove();
            }
		}
	}
	
	private synchronized void register(Selector selector, ServerSocketChannel serverSocket) throws IOException {
		// Grab the incoming socket from the serverSocket
        SocketChannel client = serverSocket.accept();
        // Configure it to be a new channel and key that our selector should monitor
        client.configureBlocking(false);

        // create a new Throughput to attach to this Client for tracking its statistics
        Throughput throughput = serverStatistics.addClient();
        client.register(selector, SelectionKey.OP_READ, throughput);
        serverStatistics.incremementServerThroughput();
        System.out.println("\t\tNew client registered.");
    }
	
	private static void readAndRespond(SelectionKey key) throws IOException {
		// create a task to send to the client
		Task task = new Task(key, serverStatistics, hash, threadPoolManager);
		task.readTask();
    }
}
