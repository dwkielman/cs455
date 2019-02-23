package cs455.scaling.client;

import java.util.LinkedList;

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
	
	// For every data packet that is published, the client adds the corresponding hashcode to the linked list
	
	// When an acknowledgement is received from the server, the client checks the hashcode in the acknowledgement by scanning through the linked list
	
	// Once the hashcode has been verified, it can be removed from the linked list
	
	
	
	
	/**
	 * once every 20 seconds after starting up, every client should print the number of messages it has sent and received during the last 20 seconds. This log message should look similar to the following.
	 * 
	 * [timestamp] Total Sent Count: x, Total Received Count: y
	 * 
	 */

}
