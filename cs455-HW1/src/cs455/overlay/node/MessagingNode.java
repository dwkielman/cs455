package cs455.overlay.node;

import cs455.overlay.transport.TCPReceiverThread;
import cs455.overlay.transport.TCPSender;
import cs455.overlay.transport.TCPServerThread;
import cs455.overlay.wireformats.DeregisterRequest;
import cs455.overlay.wireformats.DeregisterResponse;
import cs455.overlay.wireformats.Event;
import cs455.overlay.wireformats.LinkWeights;
import cs455.overlay.wireformats.Message;
import cs455.overlay.wireformats.MessagingNodesList;
import cs455.overlay.wireformats.Protocol;
import cs455.overlay.wireformats.RegisterRequest;
import cs455.overlay.wireformats.RegisterResponse;
import cs455.overlay.wireformats.TaskComplete;
import cs455.overlay.wireformats.TaskInitiate;
import cs455.overlay.wireformats.TaskSummaryRequest;
import cs455.overlay.wireformats.TaskSummaryResponse;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;

/**
 * A messaging node provides two closely related functions: it initiates and accepts both communications and messages within the system.
 */

public class MessagingNode implements Node {

	private static boolean DEBUG = true;
	
	private String localHostIPAddress;
	private int localHostPortNumber;
	private String registryHostName;
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
		this.registryHostName = registryHostIPAddress;
		this.registryHostPortNumber = registryHostPortNumber;
		this.sendTracker = 0;
		this.receiveTracker = 0;
		this.relayTracker = 0;
		this.sendSummation = 0;
		this.receiveSummation = 0;
		
		try {
			TCPServerThread serverThread = new TCPServerThread(0, this);
			serverThread.start();
			if (DEBUG) {
				System.out.println("My server port number is: " + this.localHostPortNumber);
			}
			this.localHostIPAddress = InetAddress.getLocalHost().getCanonicalHostName();
			if (DEBUG) {
				System.out.println("My host IP Address is: " + this.localHostIPAddress);
			}
			
		} catch (UnknownHostException uhe) {
			uhe.printStackTrace();
		}
		connectToRegistry();
	}
	
	@Override
	public void setLocalHostPortNumber(int portNumber) {
		this.localHostPortNumber = portNumber;
	}
	
	@Override
	public void onEvent(Event event) {
		int eventType = event.getType();
		switch(eventType) {
			// REGISTER_RESPONSE = 6001
			case Protocol.REGISTER_RESPONSE:
				handleRegisterResponse(event);	
				break;
			// DEREGISTER_RESPONSE = 6003
			case Protocol.DEREGISTER_RESPONSE:
				handleDeregisterResponse(event);
				break;
			// MESSAGING_NODES_LIST = 6004
			case Protocol.MESSAGING_NODES_LIST:
				handleMessagingNodesList(event);
				break;
			// LINK_WEIGHTS = 6005
			case Protocol.LINK_WEIGHTS:
				handleLinkWeights(event);
				break;
			// TASK_INITIATE = 6006
			case Protocol.TASK_INITIATE:
				handleTaskInitiate(event);
				break;
			// PULL_TRAFFIC_SUMMARY = 6008
			case Protocol.PULL_TRAFFIC_SUMMARY:
				handleTaskSummaryRequest(event);
				break;
			// TRAFFIC_SUMMARY = 6009
			case Protocol.TRAFFIC_SUMMARY:
				handleTaskSummaryResponse(event);
				break;
			// MESSAGE = 6010
			case Protocol.MESSAGE:
				handleMessage(event);
				break;
			default:
				System.out.println("Invalid Event to Node.");
				return;
			}
		}
		
	// Communications that nodes have with each other are based on TCP. Each messaging node needs to automatically configure the ports over which it listens for communications

	// Once the initialization is complete, the node should send a registration request to the registry.
	
	// java cs455.overlay.node.MessagingNode registry-host registry-port
	public static void main(String[] args) {
		
		// requires 2 arguments to initialize a node
		if(args.length != 2) {
            System.out.println("Invalid Arguments. Must include host name and port number.");
            return;
        }
		
		// testing for debugging, assuming that the IP address and arguments are valid commands
		if(DEBUG) {
			System.out.println("In Debug Mode.");
			try {
				System.out.println("My address is: " + InetAddress.getLocalHost().getCanonicalHostName());
			} catch (UnknownHostException uhe) {
				uhe.printStackTrace();
			}
		}
		
		String registryHostIPAddress = args[0];
		int registryHostPortNumber = 0;
		
		try {
			registryHostPortNumber = Integer.parseInt(args[1]);
		} catch (NumberFormatException nfe) {
			System.out.println("Invalid argument. Second argument must be a number.");
			nfe.printStackTrace();
		}
		
		MessagingNode messagingNode = new MessagingNode(registryHostIPAddress, registryHostPortNumber);
		handleUserInput(messagingNode);
	}
	
	private static void handleUserInput(Node messagingNode) {
		
	}
	
	/**
	 * Used for nodes requesting registering with the registry
	 * Message Type (int): REGISTER_REQUEST (6000)
	 * IP address (String)
	 * Port number (int)
	 */
	private synchronized void connectToRegistry() {
		try {
			System.out.println(String.format("Attempting to connect to registry at: %s:%d", this.registryHostName, this.registryHostPortNumber));
			Socket registrySocket = new Socket(this.registryHostName, this.registryHostPortNumber);
			TCPSender registrySender = new TCPSender(registrySocket);
			
			RegisterRequest registryRequest = new RegisterRequest(this.registryHostName, this.registryHostPortNumber);
			registrySender.sendData(registryRequest.getBytes());
			
		} catch (IOException ioe) {
			ioe.printStackTrace();
			System.exit(1);
		}
	}

	private void handleRegisterResponse(Event event) {
		RegisterResponse registerResponse = (RegisterResponse) event;
		// successful registration
		if (registerResponse.getStatusCode() == (byte) 1) {
			System.out.println("Registration Request Succeeded.");
			System.out.println(String.format("Message: %s", registerResponse.getAdditionalInfo()));
		// unsuccessful registration
		} else {
			System.out.println("Registration Request Failed. Exiting.");
            System.out.println(String.format("Message: %s", registerResponse.getAdditionalInfo()));
            System.exit(0);
		}
	}

	private void handleDeregisterResponse(Event event) {
		
	}

	private void handleMessagingNodesList(Event event) {
		
	}

	private void handleLinkWeights(Event event) {
		
	}

	private void handleTaskInitiate(Event event) {
		
	}

	private void handleTaskSummaryRequest(Event event) {
		
	}

	private void handleTaskSummaryResponse(Event event) {
		
	}

	private void handleMessage(Event event) {
		
	}
	
	/**
     * Create this as synchronized so that two threads can't update the counter simultaneously.
     */
    private synchronized void incrementSentCounter() {
        this.sendTracker++;
    }

    private synchronized void incrementReceivedCounter() {
        this.receiveTracker++;
    }

    private synchronized void incrementRelayCounter() {
        this.relayTracker++;
    }

    private synchronized void addReceiveSummation(int value) {
        this.receiveSummation += value;
    }

    private synchronized void addSentSummation(int value) {
        this.sendSummation += value;
    }

    private synchronized void clearSentCounter() {
        this.sendTracker = 0;
    }

    private synchronized void clearReceivedCounter() {
        this.receiveTracker = 0;
    }

    private synchronized void clearRelayCounter() {
        this.relayTracker = 0;
    }

    private synchronized void clearReceivedSummation() {
        this.receiveSummation = 0;
    }

    private synchronized void clearSendSummation() {
        this.sendSummation = 0;
    }

}
