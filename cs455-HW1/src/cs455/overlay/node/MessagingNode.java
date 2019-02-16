package cs455.overlay.node;

import cs455.overlay.dijkstra.Edge;
import cs455.overlay.dijkstra.RoutingCache;
import cs455.overlay.dijkstra.ShortestPath;
import cs455.overlay.transport.TCPReceiverThread;
import cs455.overlay.transport.TCPSender;
import cs455.overlay.transport.TCPServerThread;
import cs455.overlay.util.OverlayCreator;
import cs455.overlay.wireformats.DeregisterRequest;
import cs455.overlay.wireformats.DeregisterResponse;
import cs455.overlay.wireformats.Event;
import cs455.overlay.wireformats.LinkWeights;
import cs455.overlay.wireformats.Message;
import cs455.overlay.wireformats.MessagingNodesList;
import cs455.overlay.wireformats.NodeConnectionRequest;
import cs455.overlay.wireformats.NodeConnectionResponse;
import cs455.overlay.wireformats.Protocol;
import cs455.overlay.wireformats.RegisterRequest;
import cs455.overlay.wireformats.RegisterResponse;
import cs455.overlay.wireformats.TaskComplete;
import cs455.overlay.wireformats.TaskInitiate;
import cs455.overlay.wireformats.TaskSummaryResponse;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Random;
import java.util.Scanner;

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

	private ArrayList<NodeInformation> neighborNodes;
	private ArrayList<NodeInformation> nonNeighborNodes;
	private ArrayList<Edge> edgesList;
	private OverlayCreator overlay;
	private RoutingCache routingCache;
	
	private MessagingNode(String registryHostIPAddress, int registryHostPortNumber) {
		this.registryHostName = registryHostIPAddress;
		this.registryHostPortNumber = registryHostPortNumber;
		this.sendTracker = 0;
		this.receiveTracker = 0;
		this.relayTracker = 0;
		this.sendSummation = 0;
		this.receiveSummation = 0;
		this.neighborNodes = new ArrayList<>();
		this.nonNeighborNodes = new ArrayList<>();
		this.edgesList = new ArrayList<>();
		
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
		System.out.println("Event " + eventType + "Passed to MessagingNode.");
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
			// MESSAGE = 6010
			case Protocol.MESSAGE:
				handleMessage(event);
				break;
			// NODE_CONNECTION_REQUEST = 6011
			case Protocol.NODE_CONNECTION_REQUEST:
				handleNodeConnectionRequest(event);
				break;
			// NODE_CONNECTION_RESPONSE = 6012
			case Protocol.NODE_CONNECTION_RESPONSE:
				handleNodeConnectionResponse(event);
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
	
	private static void handleUserInput(MessagingNode messagingNode) {
		Scanner scan = new Scanner(System.in);
		System.out.println("MessagingNode main()");
		
		System.out.println("MessagingNode accepting commands ...");
        while(true) {
            System.out.println("Enter command: ");
            String response = scan.nextLine();
            System.out.println("You typed: " + response);
            
            if (response.equals("print-shortest-path")) {
            	System.out.println("Starting print-shortest-path:");
            } else if (response.equals("exit-overlay")) {
            	System.out.println("Starting exit-overlay:");
            	messagingNode.disconnectFromRegistry();
            } else {
            	System.out.println("Command unrecognized");
            }
        }
	}
	
	/**
	 * Used for nodes requesting registering with the registry
	 * Message Type (int): REGISTER_REQUEST (6000)
	 * IP address (String)
	 * Port number (int)
	 */
	private synchronized void connectToRegistry() {
		System.out.println("begin MessagingNode connectToRegistry");
		try {
			System.out.println(String.format("Attempting to connect to registry at: %s:%d", this.registryHostName, this.registryHostPortNumber));
			Socket registrySocket = new Socket(this.registryHostName, this.registryHostPortNumber);
			
			System.out.println("Sending to " + this.registryHostName + " on Port " + this.registryHostPortNumber);
			
			TCPSender registrySender = new TCPSender(registrySocket);
			
			RegisterRequest registryRequest = new RegisterRequest(this.localHostIPAddress, this.localHostPortNumber);
			registrySender.sendData(registryRequest.getBytes());
			
		} catch (IOException ioe) {
			ioe.printStackTrace();
			System.exit(1);
		}
		System.out.println("end MessagingNode connectToRegistry");
	}

	private void handleRegisterResponse(Event event) {
		System.out.println("begin MessagingNode handleRegisterResponse");
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
		System.out.println("end MessagingNode handleRegisterResponse");
	}
	
	private void disconnectFromRegistry() {
		System.out.println("begin MessagingNode disconnectFromRegistry");
		try {
			System.out.println(String.format("Attempting to disconnect to registry at: %s:%d", this.registryHostName, this.registryHostPortNumber));
			Socket registrySocket = new Socket(this.registryHostName, this.registryHostPortNumber);
			
			System.out.println("Sending Dergister Request to " + this.registryHostName + " on Port " + this.registryHostPortNumber);
			
			TCPSender registrySender = new TCPSender(registrySocket);
			
			DeregisterRequest deregistryRequest = new DeregisterRequest(this.localHostIPAddress, this.localHostPortNumber);
			registrySender.sendData(deregistryRequest.getBytes());
			
		} catch (IOException ioe) {
			ioe.printStackTrace();
			System.exit(1);
		}
		System.out.println("end MessagingNode disconnectFromRegistry");
	}

	private void handleDeregisterResponse(Event event) {
		System.out.println("begin MessagingNode handleDeregisterResponse");
		DeregisterResponse deregisterResponse = (DeregisterResponse) event;
		// successful deregistration
		if (deregisterResponse.getStatusCode() == (byte) 1) {
			System.out.println("Deregistration Request Succeeded.");
			System.out.println(String.format("Message: %s", deregisterResponse.getAdditionalInfo()));
		// unsuccessful registration
		} else {
			System.out.println("Deregistration Request Failed. Exiting.");
            System.out.println(String.format("Message: %s", deregisterResponse.getAdditionalInfo()));
            System.exit(0);
		}
		System.out.println("end MessagingNode handleDeregisterResponse");
	}

	private void handleMessagingNodesList(Event event) {
		System.out.println("begin MessagingNode handleMessagingNodesList");
		MessagingNodesList messagingNodesList = (MessagingNodesList) event;
		ArrayList<NodeInformation> nodesToConnectTo = new ArrayList<>(messagingNodesList.getMessagingNodesInfoList());
		
		for (NodeInformation ni : nodesToConnectTo) {
			connectToMessagingNode(ni);
		}
		
		if (this.neighborNodes.size() == messagingNodesList.getNumberOfPeerMessagingNodes()) {
			System.out.println("All connections are established. Number of connections: " + this.neighborNodes.size());
		} else {
			System.out.println("Number of Connections is currently: " + this.neighborNodes.size());
		}
		System.out.println("end MessagingNode handleMessagingNodesList");
	}
	
	private void connectToMessagingNode(NodeInformation nodeToConnectTo) {

		try {
			System.out.println(String.format("Attempting to connect to messagingNode at: %s:%d", nodeToConnectTo.getNodeIPAddress(), nodeToConnectTo.getNodePortNumber()));
			Socket nodeSocket = new Socket(nodeToConnectTo.getNodeIPAddress(), nodeToConnectTo.getNodePortNumber());
			
			System.out.println("Sending to " + nodeToConnectTo.getNodeIPAddress() + " on Port " + nodeToConnectTo.getNodePortNumber());
			
			TCPSender nodeSender = new TCPSender(nodeSocket);
			
			NodeConnectionRequest nodeConnectionRequest = new NodeConnectionRequest(new NodeInformation(this.localHostIPAddress, this.localHostPortNumber));
			nodeSender.sendData(nodeConnectionRequest.getBytes());
			
		} catch (IOException ioe) {
			ioe.printStackTrace();
			System.exit(1);
		}
	}
	
	private void handleNodeConnectionRequest(Event event) {
		System.out.println("begin handleNodeConnectionRequest");
		NodeConnectionRequest nodeConnectionRequest = (NodeConnectionRequest) event;
		
		NodeInformation requesterNode = nodeConnectionRequest.getNodeRequester();
		
		System.out.println("Got messagingNode request from IP: " + requesterNode.getNodeIPAddress() + " Port: " + String.valueOf(requesterNode.getNodePortNumber()) + ".");
		
		// success, node is not currently connected to as one of our neighbors
		if (!this.neighborNodes.contains(requesterNode)) {
			this.neighborNodes.add(requesterNode);
			System.out.println("MessagingNode request successful. The number of messaging nodes this node has as a neighbor is (" + this.neighborNodes.size() + ")");
			this.sendNodeConnectionResponse(nodeConnectionRequest, (byte) 1);
		} else {
			this.sendNodeConnectionResponse(nodeConnectionRequest, (byte) 0);
		}
		System.out.println("end handleNodeConnectionRequest");
	}
	
	private void sendNodeConnectionResponse(NodeConnectionRequest nodeConnectionRequest, byte status) {
		System.out.println("begin sendNodeConnectionResponse");
		try {
			Socket socket = new Socket(nodeConnectionRequest.getNodeRequester().getNodeIPAddress(), nodeConnectionRequest.getNodeRequester().getNodePortNumber());
			TCPSender sender = new TCPSender(socket);
			
			System.out.println("Sending to " + nodeConnectionRequest.getNodeRequester().getNodeIPAddress() + " on Port " + nodeConnectionRequest.getNodeRequester().getNodePortNumber());
			
			NodeConnectionResponse nodeConnectionResponse = new NodeConnectionResponse(status, new NodeInformation(this.localHostIPAddress, this.localHostPortNumber));
			sender.sendData(nodeConnectionResponse.getBytes());
			socket.close();
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
		System.out.println("end sendNodeConnectionResponse");
	}
	
	private void handleNodeConnectionResponse(Event event) {
		System.out.println("begin handleNodeConnectionResponse");
		NodeConnectionResponse nodeConnectionResponse = (NodeConnectionResponse) event;
		
		NodeInformation responderNode = nodeConnectionResponse.getNodeResponder();
		
		System.out.println("Got messagingNode request from IP: " + responderNode.getNodeIPAddress() + " Port: " + String.valueOf(responderNode.getNodePortNumber()) + ".");
		
		// successful connection
		if (nodeConnectionResponse.getStatusCode() == (byte) 1) {
			System.out.println("Connection Request Succeeded.");
			this.neighborNodes.add(responderNode);
		// unsuccessful connection
		} else {
			System.out.println("Connection Request Failed.");
		}
		System.out.println("end handleNodeConnectionResponse");
	}

	private void handleLinkWeights(Event event) {
		System.out.println("begin handleLinkWeights");
		LinkWeights linkWeights = (LinkWeights) event;
		
		this.edgesList = linkWeights.getlinkWeightsEdges();
		ArrayList<NodeInformation> nodes = new ArrayList<>();
		
		for (Edge e : this.edgesList) {
			// builds a list of NodeInformation for creating the overlay
			if (!nodes.contains(e.getSourceNode())) {
				nodes.add(e.getSourceNode());
			}
			
			// adds to the of non-neighbor nodes that this node is connected to
			if ((e.getSourceNode().getNodeIPAddress().equals(this.localHostIPAddress)) && (e.getSourceNode().getNodePortNumber() == this.localHostPortNumber) && (!this.nonNeighborNodes.contains(e.getSourceNode()))) {
				nonNeighborNodes.add(e.getSourceNode());
			}
		}
		
		this.overlay = new OverlayCreator(nodes);
		this.overlay.createOverlayFromEdges(edgesList);
		System.out.println("end handleLinkWeights");
	}

	private void handleTaskInitiate(Event event) {
		System.out.println("begin handleTaskInitiate");
		TaskInitiate taskInitiate = (TaskInitiate) event;
		
		int numberOfRounds = taskInitiate.getNumberOfRounds();
		this.routingCache = new RoutingCache();
		Random random = new Random();
		
		for (int i = 0; i < numberOfRounds; i++) {
			int randomNode = random.nextInt(this.nonNeighborNodes.size());
			prepMessageToMessagingNode(this.nonNeighborNodes.get(randomNode));
		}
		System.out.println("end handleTaskInitiate");
		sendTaskComplete();
	}
	
	private void prepMessageToMessagingNode(NodeInformation node) {
		ArrayList<NodeInformation> path;
		Random random = new Random();
		
		// destination node has not been routed yet
		if (!this.routingCache.isRoute(node)) {
			ShortestPath shortestPath = new ShortestPath(this.overlay);
			shortestPath.execute(node);
			path = new ArrayList<>(shortestPath.getPath(node));
			this.routingCache.addPath(node, path);
		// destination node has already been routed before
		} else {
			path = this.routingCache.getPathFromRoutingCache(node);
		}
		
		// payload of each message is a random integer with values that range from 2147483647 to -2147483648
		int payload = random.nextInt();
		Message message = new Message(new NodeInformation(this.localHostIPAddress, this.localHostPortNumber), node, payload, path);
		this.incrementSentCounter();
		this.addSentSummation(payload);
		
		// send Message to the next node on the pathlist, 0 index = this node
		sendMessageToMessagingNode(path.get(1), message);
	}
	
	private synchronized void sendMessageToMessagingNode(NodeInformation node, Message message) {
		System.out.println("begin sendMessageToMessagingNode");
		try {
			Socket socket = new Socket(node.getNodeIPAddress(), node.getNodePortNumber());
			TCPSender sender = new TCPSender(socket);
			
			if (DEBUG) {
				System.out.println("Sending to " + node.getNodeIPAddress() + " on Port " + node.getNodePortNumber());
			}
			
			sender.sendData(message.getBytes());
			socket.close();
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
		System.out.println("end sendMessageToMessagingNode");
	}
	
	private void sendTaskComplete() {
		System.out.println("begin sendTaskComplete");
		
		try {
			System.out.println(String.format("Attempting to connect to registry at: %s:%d", this.registryHostName, this.registryHostPortNumber));
			Socket registrySocket = new Socket(this.registryHostName, this.registryHostPortNumber);
			TCPSender sender = new TCPSender(registrySocket);
			TaskComplete taskComplete = new TaskComplete(new NodeInformation(this.registryHostName, this.registryHostPortNumber));
			
			if (DEBUG) {
				System.out.println("Sending to " + this.registryHostName + " on Port " + this.registryHostPortNumber);
			}
			
			sender.sendData(taskComplete.getBytes());
			registrySocket.close();
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
		
		System.out.println("end sendTaskComplete");
	}

	/**
	 * Node IP address (String):
	 * Node Port number (int):
	 * Number of messages sent (int)
	 * Summation of sent messages (long)
	 * Number of messages received (int)
	 * Summation of received messages (long)
	 * Number of messages relayed (int)
	 */
	private void handleTaskSummaryRequest(Event event) {
		try {
			System.out.println(String.format("Attempting to connect to registry at: %s:%d", this.registryHostName, this.registryHostPortNumber));
			Socket registrySocket = new Socket(this.registryHostName, this.registryHostPortNumber);
			TCPSender sender = new TCPSender(registrySocket);
			TaskSummaryResponse taskSummaryReponse = new TaskSummaryResponse(this.localHostIPAddress, this.localHostPortNumber, this.sendTracker, this.sendSummation, this.receiveTracker, this.receiveSummation, this.relayTracker);
			
			if (DEBUG) {
				System.out.println("Sending to " + this.registryHostName + " on Port " + this.registryHostPortNumber);
			}
			
			sender.sendData(taskSummaryReponse.getBytes());
			clearSentCounter();
			clearReceivedCounter();
			clearRelayCounter();
			clearReceivedSummation();
			clearSendSummation();
			registrySocket.close();
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
	}

	private void handleMessage(Event event) {
		System.out.println("begin handleMessage");
		Message message = (Message) event;
		
		ArrayList<NodeInformation> routePath = message.getRoutePath();
		
		// end of the list, this node is the destination for the route path
		if ((message.getDestinationNode().getNodeIPAddress().equals(this.localHostIPAddress)) && (message.getDestinationNode().getNodePortNumber() == this.registryHostPortNumber)) {
			this.incrementReceivedCounter();
			this.addReceiveSummation(message.getPayload());
		// this node is not the end, need to find the next node on the route path
		} else {
			NodeInformation nextNode = null;
			for (int i=0; i < routePath.size(); i++) {
				nextNode = routePath.get(i);
				if ((nextNode.getNodeIPAddress().equals(this.localHostIPAddress)) && (nextNode.getNodePortNumber() == this.localHostPortNumber)) {
					nextNode = routePath.get(i + 1);
					break;
				}
			}
			
			if (nextNode != null) {
				this.incrementRelayCounter();
				this.sendMessageToMessagingNode(nextNode, message);
			}
		}
		System.out.println("end handleMessage");
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
