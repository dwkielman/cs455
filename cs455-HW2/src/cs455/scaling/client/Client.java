package cs455.scaling.client;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.util.LinkedList;

import cs455.scaling.server.Server;
import cs455.scaling.server.ThreadPoolManager;

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
	private LinkedList<String> hashCodesList;
	private static SocketChannel clientSocketChannel;
	
	// For every data packet that is published, the client adds the corresponding hashcode to the linked list
	
	// When an acknowledgement is received from the server, the client checks the hashcode in the acknowledgement by scanning through the linked list
	
	// Once the hashcode has been verified, it can be removed from the linked list
	
	public static void main(String[] args) {
		
		// requires 1 argument to initialize a registry
		if(args.length != 3) {
		    System.out.println("Invalid Arguments. Must include a Server Host Name, Server Port Number and Message Rate");
		    return;
		}
		
		Client client = new Client();
		
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
		
		client.connectToServer(serverHostName, serverPortNumber);
		// create hashTracker
		// create a task if needed
		// start the atsk
		// senderThread create and start
		
	}

	private void connectToServer(String serverHostName, int serverPortNumber) {
		try {
			System.out.println("Client attempting to Connect to Server.");
  			// Open the socket channel for incoming connections
			clientSocketChannel = SocketChannel.open();
			// non-blocking
			clientSocketChannel.configureBlocking(false);
			// Connect to the server
			clientSocketChannel.connect(new InetSocketAddress(serverHostName, serverPortNumber));
			// might need to do more here
        } catch (IOException e) {
            e.printStackTrace();
        }
	}
	
	
	/**
	 * once every 20 seconds after starting up, every client should print the number of messages it has sent and received during the last 20 seconds. This log message should look similar to the following.
	 * 
	 * [timestamp] Total Sent Count: x, Total Received Count: y
	 * 
	 */

}
