package cs455.scaling.client;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.Set;

import cs455.scaling.hash.Hash;

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
	
	private final ClientStatistics clientStatistics;
	private static SocketChannel clientSocketChannel;
	private Selector selector;
	private static final int BUFFER_SIZE = 8192;
	private final int messageRate;
	private final static Hash hash = new Hash();
	private DataInputStream dataInputStream;
	private Socket socket;
	private SenderThread senderThread;
	private static SelectionKey key;
	
	public Client(int messageRate) {
		this.messageRate = messageRate;
		this.clientStatistics = new ClientStatistics();
	}
	// For every data packet that is published, the client adds the corresponding hashcode to the linked list
	
	// When an acknowledgement is received from the server, the client checks the hashcode in the acknowledgement by scanning through the linked list
	
	// Once the hashcode has been verified, it can be removed from the linked list
	
	public static void main(String[] args) {
		
		// requires 1 argument to initialize a Client
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
			//client.connectToServer(serverHostName, serverPortNumber);
			
			client.improvedConnectToServer(serverHostName, serverPortNumber);
			client.improvedClientLoop();
			//client.finishConnectionToServer(key);
			//client.clientLoop();
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
	}
	
	private void improvedConnectToServer(String serverHostName, int serverPortNumber) throws UnknownHostException, IOException {
		socket = new Socket(serverHostName, serverPortNumber);
		dataInputStream = new DataInputStream(socket.getInputStream());
		DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());
		startClientStatisticsThread();
		improvedStartSenderThread(dataOutputStream);
		
	}
	
	private void improvedClientLoop() {
		while (true) {
			byte[] incomingData = new byte[40];
			try {
				this.dataInputStream.readFully(incomingData);
				this.senderThread.readHash(new String(incomingData));
			} catch (IOException ioe) {
				ioe.printStackTrace();
			}
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
			// testing with this connection commented out to get the SelectionKey
			//clientSocketChannel.register(selector, SelectionKey.OP_CONNECT);
			key = clientSocketChannel.register(selector, SelectionKey.OP_CONNECT);

			//socket = new Socket(serverHostName, serverPortNumber);

        } catch (IOException e) {
            e.printStackTrace();
        }
	}
	
	private void startClientStatisticsThread() {
		// start the thread for displaying client statistics
		new Thread(clientStatistics).start();
	}
	
	private void improvedStartSenderThread(DataOutputStream dataOutputStream) {
		senderThread = new SenderThread(dataOutputStream, messageRate, clientStatistics);
		new Thread(senderThread).start();
	}
	
	private void startSenderThread(SelectionKey key) {
		senderThread = new SenderThread(clientSocketChannel, messageRate, clientStatistics, key);
		new Thread(senderThread).start();
	}
	
	private void finishConnectionToServer(SelectionKey key) throws IOException {
		SocketChannel socketChannel = (SocketChannel)key.channel();
		socketChannel.finishConnect();
		startClientStatisticsThread();
		startSenderThread(key);
		// trying out being interested only in writing to the server
		// FRIDAY NIGHT, TURNING OFF WRITING ENTIRELY HERE
		//key.interestOps(SelectionKey.OP_WRITE);
		
		// trying out being interested in both reading and writing from the server
		//key.interestOps(SelectionKey.OP_READ & SelectionKey.OP_WRITE); 
		//key.interestOps(SelectionKey.OP_READ);
	}
	
	private void read(SelectionKey key, ByteBuffer readBuffer) {
		System.out.println("Reading data from server...");
		// testing out blocking on writing while we are reading
		//key.interestOps(key.interestOps() & ~SelectionKey.OP_WRITE);
		SocketChannel socketChannel = (SocketChannel) key.channel();
        //ByteBuffer readBuffer = ByteBuffer.allocate(40);
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
			
			/**
			if (this.clientStatistics.removeHashCode(hashedString.trim())) {
				this.clientStatistics.incrementMessagesReceived();
				this.clientStatistics.removeHashCode(hashedString.trim());
				System.out.println("Removed Task with Hashed String " + hashedString);
			} else {
				System.out.println("Incorrect work sent to Client");
			}
			**/
			readBuffer.clear();
			
			// After reading, register an interest in Writing
			// testing removing this line for now
	        //key.interestOps(SelectionKey.OP_WRITE);
		} catch(Exception e) {
			e.printStackTrace();
		}finally {
			selector.wakeup();
		}
        
	}
	
	private void clientLoop() throws IOException {
		System.out.println("In the Client Loop");
		while (true) {
			// Block here
            this.selector.select();
            
            // Key(s) are ready
            Set<SelectionKey> selectedKeys = selector.selectedKeys();
            
            // Prepare bytebuffer always ready for 40 bytes of data
 			ByteBuffer buffer = ByteBuffer.allocate(40);
            
            //System.out.println("Size of keys: " + selectedKeys.size());
            // Loop over ready keys
            Iterator<SelectionKey> iter = selectedKeys.iterator();
            while (iter.hasNext()) {
            	System.out.println("Iterating");
                // Grab current key
                SelectionKey key = iter.next();

            	// Optional
                if(key.isValid() == false) { 
                    continue; 
                }

                // New connection on serverSocket
                if(key.isConnectable()) {
                	System.out.println("Finish connecting");
                	this.finishConnectionToServer(key);
                }
                
                // Previous connection has data to read
                if (key.isReadable()) {
                	System.out.println("Reading a message");
                    read(key, buffer);
                }

                // Remove it from our set
                iter.remove();
            }
		}
	}

}
