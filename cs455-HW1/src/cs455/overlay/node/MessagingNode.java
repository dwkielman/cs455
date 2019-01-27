package cs455.overlay.node;

import cs455.overlay.wireformats.Event;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;

import cs455.overlay.transport.TCPReceiverThread;

/**
 * A messaging node provides two closely related functions: it initiates and accepts both communications and messages within the system.
 */

public class MessagingNode implements Node {

	private String localHostIPAddress;
	private String registryHostIPAddress;
	private int registryHostPortNumber;
	
	// represents the number of messages that were sent by that node
	private int sendTracker;
	// maintains information about the number of messages that were received
	private int receiveTracker;
	// tracking the number of messages that a node relays will depend on the overlay topology, link weights, and shortest paths that it belongs to
	private int relayTracker;
	// track the messages that it has sent. continuously sums the values of the random numbers that are sent
	private long sendSummation;
	// track the messages that it has received. sums values of the payloads that are received
	private long receiveSummation;
	
	private MessagingNode(String registryHostIPAddress, int registryHostPortNumber) {
		this.registryHostIPAddress = registryHostIPAddress;
		this.registryHostPortNumber = registryHostPortNumber;
		this.sendTracker = 0;
		this.receiveTracker = 0;
		this.relayTracker = 0;
		this.sendSummation = 0;
		this.receiveSummation = 0;
		
		try {
			localHostIPAddress = InetAddress.getLocalHost().getCanonicalHostName();
		} catch (UnknownHostException uhe) {
			uhe.printStackTrace();
		}	
	}
	
	@Override
	public void onEvent(Event event) {
		// TODO Auto-generated method stub
		
	}
	
	// Communications that nodes have with each other are based on TCP. Each messaging node needs to automatically configure the ports over which it listens for communications
	
	// TCPServerSocket is used to accept incoming TCP communications.

	// Once the initialization is complete, the node should send a registration request to the registry.
	
	// java cs455.overlay.node.MessagingNode registry-host registry-port
	public static void main(String[] args) {
		
		// requires 2 arguments to initialize a node
		if(args.length != 2) {
            System.out.println("Invalid Arguments. Must include host name and port number.");
            return;
        }
		
		
		
	}
}
