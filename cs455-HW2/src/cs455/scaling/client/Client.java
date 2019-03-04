package cs455.scaling.client;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Set;

import cs455.scaling.hash.Hash;
import cs455.scaling.server.Server;
import cs455.scaling.server.ThreadPoolManager;
import cs455.scaling.server.Throughput;

/**
 * A client provides the following functionalities:
 * (1) Connect and maintain an active connection to the server.
 * (2) Regularly send data packets to the server. The payloads for these data packets are 8 KB and the values for these bytes are randomly generated. The rate at which each connection will
 * generate packets is R per-second; include a Thread.sleep(1000/ R) in the client which ensures that you achieve the targeted production rate. The typical value of R is between 2-4.
 * (3) The client should track hashcodes of the data packets that it has sent to the server. A server will acknowledge every packet that it has received by sending the computed hash code back to the client.
 * 
 * Executed with the following command:
 * java cs455.scaling.client.Client server-host server-port message-rate
 */

public class Client {
	
	// maintains the hash codes in a linked list
	private final ClientStatistics clientStatistics;
	private static SocketChannel clientSocketChannel;
	private Selector selector;
	private final int bufferSize = 8192;
	private final int messageRate;
	private final static Hash hash = new Hash();
	
	public Client(int messageRate) {
		this.messageRate = messageRate;
		this.clientStatistics = new ClientStatistics();
	}
	// For every data packet that is published, the client adds the corresponding hashcode to the linked list
	
	// When an acknowledgement is received from the server, the client checks the hashcode in the acknowledgement by scanning through the linked list
	
	// Once the hashcode has been verified, it can be removed from the linked list
	
	public static void main(String[] args) {
		
		// requires 1 argument to initialize a registry
		if(args.length != 3) {
		    System.out.println("Invalid Arguments. Must include a Server Host Name, Server Port Number and Message Rate");
		    return;
		}

		String serverHostName = "";
		int serverPortNumber = 0;
		int messageRate = 0;
		
		try {
			serverHostName = args[0];
			serverPortNumber = Integer.parseInt(args[1]);
			messageRate = Integer.parseInt(args[2]);
		} catch (NumberFormatException nfe) {
			System.out.println("Invalid argument(s).");
			nfe.printStackTrace();
		}
		
		Client client = new Client(messageRate);

		try {
			client.connectToServer(serverHostName, serverPortNumber);
			client.clientLoop();
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
	}

	private void connectToServer(String serverHostName, int serverPortNumber) {
		try {
			// Open the selector
			this.selector = Selector.open();
			
			//System.out.println("Client attempting to Connect to Server.");
  			// Open the socket channel for incoming connections
			clientSocketChannel = SocketChannel.open();
			// non-blocking
			clientSocketChannel.configureBlocking(false);
			// Connect to the server
			clientSocketChannel.connect(new InetSocketAddress(serverHostName, serverPortNumber));
			// Set channel ready for accepting connections
			clientSocketChannel.register(selector, SelectionKey.OP_CONNECT);
        } catch (IOException e) {
            e.printStackTrace();
        }
	}
	
	private void startClientStatisticsThread() {
		// start the thread for displaying client statistics
		new Thread(clientStatistics).start();
	}
	
	private void startSenderThread(SelectionKey key) {
		SenderThread senderThread = new SenderThread(clientSocketChannel, messageRate, clientStatistics, key);
		new Thread(senderThread).start();
	}
	
	private void finishConnectionToServer(SelectionKey key) throws IOException {
		SocketChannel socketChannel = (SocketChannel)key.channel();
		socketChannel.finishConnect();
		startClientStatisticsThread();
		startSenderThread(key);
		key.interestOps(SelectionKey.OP_READ);
	}
	
	private void read(SelectionKey key) {
		System.out.println("Reading data from server...");
		SocketChannel socketChannel = (SocketChannel) key.channel();
        ByteBuffer readBuffer = ByteBuffer.allocate(40);
        int bytesRead = 0;
        
        try {
			// Read entire buffer
			while(readBuffer.hasRemaining() && bytesRead != -1) {
				bytesRead = socketChannel.read(readBuffer);
			}
			
			readBuffer.rewind();

			// Check for an error while reading
			if(bytesRead == -1) {
				System.out.println("Something went wrong...");
				socketChannel.close();
				key.cancel();
				return;
			}
			
			String hashedString = new String(readBuffer.array());
			
			if (this.clientStatistics.removeHashCode(hashedString.trim())) {
				this.clientStatistics.incrementMessagesReceived();
				System.out.println("Removed Task with Hashed String " + hashedString);
			} else {
				System.out.println("Incorrect work sent to Client");
			}
			
			// After reading, register an interest in Writing
	        key.interestOps(SelectionKey.OP_WRITE);
		} catch(Exception e) {
			e.printStackTrace();
		}finally {
			selector.wakeup();
		}
        
	}
	
	private void clientLoop() throws IOException {
		
		while (true) {
			//System.out.println("Client listening for incoming Messages.");
			
			// Block here
            this.selector.select();
            
            // Key(s) are ready
            Set<SelectionKey> selectedKeys = selector.selectedKeys();
            // Loop over ready keys
            Iterator<SelectionKey> iter = selectedKeys.iterator();
            while (iter.hasNext()) {
                // Grab current key
                SelectionKey key = iter.next();

            	// Optional
                if(key.isValid() == false) { 
                    continue; 
                }

                // New connection on serverSocket
                if(key.isConnectable()) {
                	this.finishConnectionToServer(key);
                }
                
                // Previous connection has data to read
                if (key.isReadable()) {
                    read(key);
                }

                // Remove it from our set
                iter.remove();

            }
		}
	}

}
