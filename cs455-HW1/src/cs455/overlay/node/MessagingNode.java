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
import java.util.HashMap;
import java.util.ListIterator;
import java.util.Random;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;

/**
 * A messaging node provides two closely related functions: it initiates and accepts both communications and messages within the system.
 * The MessagingNode can be run after being compiled using the following format:
 * java cs455.overlay.node.MessagingNode registry-host(IP Address) registry-port (Integer)
 */

public class MessagingNode implements Node {

	private static boolean DEBUG = false;
	
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
	private ArrayList<NodeInformation> connectedNeighborNodes;
	private HashMap<NodeInformation, TCPSender> messagingNodeSenders;
	private ArrayList<Edge> edgesList;
	private OverlayCreator overlay;
	private RoutingCache routingCache;
	private TCPReceiverThread registryTCPReceiverThread;
	private TCPSender registrySender;
	
	private MessagingNode(String registryHostIPAddress, int registryHostPortNumber) {
		this.registryHostName = registryHostIPAddress;
		this.registryHostPortNumber = registryHostPortNumber;
		this.sendTracker = 0;
		this.receiveTracker = 0;
		this.relayTracker = 0;
		this.sendSummation = 0;
		this.receiveSummation = 0;
		this.messagingNodeSenders = new HashMap<NodeInformation, TCPSender>();
		this.edgesList = new ArrayList<>();
		
		try {
			TCPServerThread serverThread = new TCPServerThread(0, this);
			serverThread.start();
			
			if (DEBUG) { System.out.println("My server port number is: " + this.localHostPortNumber); }
			
			this.localHostIPAddress = InetAddress.getLocalHost().getCanonicalHostName();
			
			if (DEBUG) { System.out.println("My host IP Address is: " + this.localHostIPAddress); }
		} catch (UnknownHostException uhe) {
			uhe.printStackTrace();
		}
		// Once the initialization is complete, MessagingNode should send a registration request to the Registry.
		connectToRegistry();
	}
	
	@Override
	public void setLocalHostPortNumber(int portNumber) {
		this.localHostPortNumber = portNumber;
	}
	
	@Override
	public void onEvent(Event event) {
		int eventType = event.getType();
		System.out.println("Event " + eventType + " Passed to MessagingNode.");
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
		
		System.out.println("MessagingNode ready for input.");
        while(true) {
            System.out.println("Please enter your command: ");
            String response = scan.nextLine();
            if (DEBUG) { System.out.println("User enetered: " + response); }
            
            if (response.equals("print-shortest-path")) {
            	System.out.println("Starting print-shortest-path:");
            	messagingNode.printShortestPath();
            } else if (response.equals("exit-overlay")) {
            	System.out.println("Starting exit-overlay:");
            	messagingNode.disconnectFromRegistry();
            } else if (response.equals("print-nodes")) {
            	messagingNode.printNodes();
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
	// DANIEL: Still need to test multiple connections being made at the same time on the same server
	private void connectToRegistry() {
		if (DEBUG) { System.out.println("begin MessagingNode connectToRegistry"); }
		try {
			System.out.println(String.format("Attempting to connect to registry at: %s:%d", this.registryHostName, this.registryHostPortNumber));
			Socket registrySocket = new Socket(this.registryHostName, this.registryHostPortNumber);
			
			System.out.println("Starting TCPReceiverThread with Registry");
			registryTCPReceiverThread = new TCPReceiverThread(registrySocket, this);
			registryTCPReceiverThread.start();
			System.out.println("TCPReceiverThread with Registry started");
			System.out.println("Sending to " + this.registryHostName + " on Port " + this.registryHostPortNumber);
			
			this.registrySender = new TCPSender(registrySocket);
			
			RegisterRequest registryRequest = new RegisterRequest(this.localHostIPAddress, this.localHostPortNumber);

			if (DEBUG) { System.out.println("MessagingNode about to send message type: " + registryRequest.getType()); }
			
			this.registrySender.sendData(registryRequest.getBytes());
		} catch (IOException ioe) {
			ioe.printStackTrace();
			System.exit(1);
		}
		if (DEBUG) { System.out.println("end MessagingNode connectToRegistry"); }
	}

	private void handleRegisterResponse(Event event) {
		if (DEBUG) { System.out.println("begin MessagingNode handleRegisterResponse"); }
		RegisterResponse registerResponse = (RegisterResponse) event;
		if (DEBUG) { System.out.println("MessagingNode got a message type: " + registerResponse.getType()); }
		
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
		if (DEBUG) { System.out.println("end MessagingNode handleRegisterResponse"); }
	}
	
	private void disconnectFromRegistry() {
		if (DEBUG) { System.out.println("begin MessagingNode disconnectFromRegistry"); }
		try {
			System.out.println(String.format("Attempting to disconnect to registry at: %s:%d", this.registryHostName, this.registryHostPortNumber));
			
			System.out.println("Sending Dergister Request to " + this.registryHostName + " on Port " + this.registryHostPortNumber);
			
			DeregisterRequest deregistryRequest = new DeregisterRequest(this.localHostIPAddress, this.localHostPortNumber);
			
			if (DEBUG) { System.out.println("MessagingNode about to send message type: " + deregistryRequest.getType()); }
			
			this.registrySender.sendData(deregistryRequest.getBytes());
			
		} catch (IOException ioe) {
			ioe.printStackTrace();
			System.exit(1);
		}
		if (DEBUG) { System.out.println("end MessagingNode disconnectFromRegistry"); }
	}

	private void handleDeregisterResponse(Event event) {
		if (DEBUG) { System.out.println("begin MessagingNode handleDeregisterResponse"); }
		DeregisterResponse deregisterResponse = (DeregisterResponse) event;
		if (DEBUG) { System.out.println("MessagingNode got a message type: " + deregisterResponse.getType()); }
		
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
		if (DEBUG) { System.out.println("end MessagingNode handleDeregisterResponse"); }
	}

	private void handleMessagingNodesList(Event event) {
		if (DEBUG) { System.out.println("begin MessagingNode handleMessagingNodesList"); }
		
		MessagingNodesList messagingNodesList = (MessagingNodesList) event;
		
		if (DEBUG) { System.out.println("MessagingNode got a message type: " + messagingNodesList.getType()); }
		
		this.neighborNodes = new ArrayList<>(messagingNodesList.getMessagingNodesInfoList());
		
		if (DEBUG) { 
			System.out.println("I am going to connect to the following nodes: ");
			
			for (NodeInformation ni : this.neighborNodes) {
				System.out.println("Node " + ni.getNodeIPAddress());
			}
		}
		
		for (NodeInformation ni : this.neighborNodes) {
			connectToMessagingNode(ni);
		}

		if (DEBUG) { System.out.println("end MessagingNode handleMessagingNodesList"); }
	}

	// Communications that nodes have with each other are based on TCP. Each messaging node needs to automatically configure the ports over which it listens for communications
	private void connectToMessagingNode(NodeInformation nodeToConnectTo) {
		if (DEBUG) { System.out.println("begin MessagingNode connectToMessagingNode"); }
		try {
			System.out.println(String.format("Attempting to connect to messagingNode at: %s:%d", nodeToConnectTo.getNodeIPAddress(), nodeToConnectTo.getNodePortNumber()));
			Socket nodeSocket = new Socket(nodeToConnectTo.getNodeIPAddress(), nodeToConnectTo.getNodePortNumber());
			
			if (DEBUG) { System.out.println("Sending to " + nodeToConnectTo.getNodeIPAddress() + " on Port " + nodeToConnectTo.getNodePortNumber()); }
			
			TCPSender nodeSender = new TCPSender(nodeSocket);
			
			NodeConnectionRequest nodeConnectionRequest = new NodeConnectionRequest(new NodeInformation(this.localHostIPAddress, this.localHostPortNumber));
			if (DEBUG) { System.out.println("MessagingNode about to send message type: " + nodeConnectionRequest.getType()); }
			nodeSender.sendData(nodeConnectionRequest.getBytes());
			
		} catch (IOException ioe) {
			ioe.printStackTrace();
			System.exit(1);
		}
		if (DEBUG) { System.out.println("end MessagingNode connectToMessagingNode"); }
	}
	
	private synchronized void handleNodeConnectionRequest(Event event) {
		if (DEBUG) { System.out.println("begin MessagingNode handleNodeConnectionRequest"); }
		NodeConnectionRequest nodeConnectionRequest = (NodeConnectionRequest) event;
		if (DEBUG) { System.out.println("MessagingNode got a message type: " + nodeConnectionRequest.getType()); }
		NodeInformation requesterNode = nodeConnectionRequest.getNodeRequester();
		
		if (DEBUG) { System.out.println("Got messagingNode request from IP: " + requesterNode.getNodeIPAddress() + " on Port: " + String.valueOf(requesterNode.getNodePortNumber()) + "."); }
		
		try {
			Socket socket = new Socket(nodeConnectionRequest.getNodeRequester().getNodeIPAddress(), nodeConnectionRequest.getNodeRequester().getNodePortNumber());
			TCPSender sender = new TCPSender(socket);
			
			if (DEBUG) { System.out.println("Sending to " + nodeConnectionRequest.getNodeRequester().getNodeIPAddress() + " on Port " + nodeConnectionRequest.getNodeRequester().getNodePortNumber()); }
			
			// sending status of 1 to indicate successful response
			NodeConnectionResponse nodeConnectionResponse = new NodeConnectionResponse((byte) 1, new NodeInformation(this.localHostIPAddress, this.localHostPortNumber));
			if (DEBUG) { System.out.println("MessagingNode about to send message type: " + nodeConnectionResponse.getType()); }
			sender.sendData(nodeConnectionResponse.getBytes());
			socket.close();
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
		
		// success, node is not currently connected to as one of our neighbors
		//if (!this.neighborNodes.contains(requesterNode) && this.neighborNodes.size() < 4) {
		
		/**
		synchronized (neighborNodes) {
			if (!this.neighborNodes.contains(requesterNode)) {
				this.neighborNodes.add(requesterNode);
				System.out.println("MessagingNode request successful. The number of messaging nodes this node has as a neighbor is (" + this.neighborNodes.size() + ")");
				this.sendNodeConnectionResponse(nodeConnectionRequest, (byte) 1);
			} else {
				if (this.neighborNodes.contains(requesterNode)) {
					System.out.println("Requester Node " + requesterNode.getNodeIPAddress() + " is already connected to this Node.");
				} else {
					System.out.println("Connection error, no connection made.");
				}
				this.sendNodeConnectionResponse(nodeConnectionRequest, (byte) 0);
			}
		}
		**/
		if (DEBUG) { System.out.println("end MessagingNode handleNodeConnectionRequest"); }
	}
	
	// DANIEL: OLD METHOD, may be able to delete after successful testing
	//private synchronized void sendNodeConnectionResponse(NodeConnectionRequest nodeConnectionRequest, byte status) {
	/**
	private void sendNodeConnectionResponse(NodeConnectionRequest nodeConnectionRequest, byte status) {
		//System.out.println("begin MessagingNode sendNodeConnectionResponse");
		try {
			Socket socket = new Socket(nodeConnectionRequest.getNodeRequester().getNodeIPAddress(), nodeConnectionRequest.getNodeRequester().getNodePortNumber());
			TCPSender sender = new TCPSender(socket);
			
			//System.out.println("Sending to " + nodeConnectionRequest.getNodeRequester().getNodeIPAddress() + " on Port " + nodeConnectionRequest.getNodeRequester().getNodePortNumber());
			
			NodeConnectionResponse nodeConnectionResponse = new NodeConnectionResponse(status, new NodeInformation(this.localHostIPAddress, this.localHostPortNumber));
			//System.out.println("MessagingNode about to send message type: " + nodeConnectionResponse.getType());
			sender.sendData(nodeConnectionResponse.getBytes());
			socket.close();
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
		//System.out.println("end MessagingNode sendNodeConnectionResponse");
	}
	**/
	
	private synchronized void handleNodeConnectionResponse(Event event) {
		if (DEBUG) { System.out.println("begin MessagingNode handleNodeConnectionResponse"); }
		NodeConnectionResponse nodeConnectionResponse = (NodeConnectionResponse) event;
		if (DEBUG) { System.out.println("MessagingNode got a message type: " + nodeConnectionResponse.getType()); }
		NodeInformation responderNode = nodeConnectionResponse.getNodeResponder();
		
		if (DEBUG) { System.out.println("Got messagingNode request from IP: " + responderNode.getNodeIPAddress() + " on Port: " + String.valueOf(responderNode.getNodePortNumber()) + "."); }
		
		// successful connection
		if (nodeConnectionResponse.getStatusCode() == (byte) 1) {
			// first node responding, initialize connectedNeighborNodes or face errors
			if (this.connectedNeighborNodes == null) {
				this.connectedNeighborNodes = new ArrayList<>();
			}
			// add to our connectedNeighborNodes the nodes that just responded
			if (!this.connectedNeighborNodes.contains(responderNode)) {
				this.connectedNeighborNodes.add(responderNode);
				
				// initialize a thread with the socket of the responderNode to make sending multiple messages much easier
				try {
					Socket neighborSocket = new Socket(responderNode.getNodeIPAddress(), responderNode.getNodePortNumber());
					TCPSender sender = new TCPSender(neighborSocket);
					TCPReceiverThread thread = new TCPReceiverThread(neighborSocket, this);
					new Thread(thread).start();
					this.messagingNodeSenders.put(responderNode, sender);
				} catch (UnknownHostException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
			
		if (this.connectedNeighborNodes.size() == this.neighborNodes.size()) {
			System.out.println("All connections are established.  Number of connections: " + this.connectedNeighborNodes.size());
		} else {
			System.out.println("Number of Connections made is currently: " + this.connectedNeighborNodes.size());
		}
		if (DEBUG) { System.out.println("end MessagingNode handleNodeConnectionResponse"); }
	}

	private void handleLinkWeights(Event event) {
		if (DEBUG) { System.out.println("begin MessagingNode handleLinkWeights"); }
		LinkWeights linkWeights = (LinkWeights) event;
		if (DEBUG) { System.out.println("MessagingNode got a message type: " + linkWeights.getType()); }
		
		this.edgesList = linkWeights.getlinkWeightsEdges();
		ArrayList<NodeInformation> nodes = new ArrayList<>();

		 if (DEBUG) { 
			for (Edge e : this.edgesList) {
				System.out.println("Edge: Source: " + e.getSourceNode().getNodeIPAddress() + " Dest: " + e.getDestationNode() + " Weight: " + e.getWeight());
			}
		}

		for (Edge e : this.edgesList) {
			// builds a list of NodeInformation Nodes for creating the overlay
			if (!nodes.contains(e.getSourceNode())) {
				nodes.add(e.getSourceNode());
			}
		}
		
		// create an overlay for this MessagingNode that it can use to build ShortestPaths
		// does NOT create an overlay (that would require calling execute in the overlay) but instead creates an overlay by using the Edges that have been passed from the Registry
		this.overlay = new OverlayCreator(nodes);
		this.overlay.createOverlayFromEdges(this.edgesList);
		
		ArrayList<NodeInformation> otherNodes = new ArrayList<>();
		
		for (Edge e : this.edgesList) {
			// adds to the list of non-neighbor nodes that this node is connected to
			NodeInformation source = e.getSourceNode();
			NodeInformation dest = e.getDestationNode();
			
			// don't have the source node logged
			if (!this.neighborNodes.contains(source)) {
				// not equal to this node
				if ((!source.getNodeIPAddress().equals(this.localHostIPAddress) && source.getNodePortNumber() != this.localHostPortNumber)) {
					// make sure we didn't add it already
					if (!otherNodes.contains(source)) {
						otherNodes.add(source);
					}
				}
			// don't have the destination node logged
			} else if ((!this.neighborNodes.contains(dest))) {
				if ((!dest.getNodeIPAddress().equals(this.localHostIPAddress) && dest.getNodePortNumber() != this.localHostPortNumber)) {
					if (!otherNodes.contains(dest)) {
						otherNodes.add(dest);
					}
				}
			}
		}
			
		this.nonNeighborNodes = otherNodes;
		
		if (DEBUG) { System.out.println("end MessagingNode handleLinkWeights"); }
	}

	private synchronized void handleTaskInitiate(Event event) {
		if (DEBUG) { System.out.println("begin MessagingNode handleTaskInitiate"); }
		TaskInitiate taskInitiate = (TaskInitiate) event;
		if (DEBUG) { System.out.println("MessagingNode got a message type: " + taskInitiate.getType()); }
		
		int numberOfRounds = taskInitiate.getNumberOfRounds();
		this.routingCache = new RoutingCache();
		Random random = new Random();
		
		for (int i = 0; i < numberOfRounds; i++) {
			// get a random Node to build the path that we need for each round
			int randomNode = random.nextInt(this.nonNeighborNodes.size());
			prepMessageToMessagingNode(this.nonNeighborNodes.get(randomNode));
		}
		
		sendTaskComplete();
		if (DEBUG) { System.out.println("end MessagingNode handleTaskInitiate"); }
	}
	
	private void prepMessageToMessagingNode(NodeInformation node) {
		if (DEBUG) { System.out.println("begin MessagingNode prepMessageToMessagingNode"); }
		ArrayList<NodeInformation> path = new ArrayList<>();
		Random random = new Random();
		NodeInformation me = new NodeInformation(this.localHostIPAddress, this.localHostPortNumber);
		
		// already have the shortestPath to this MessagingNode, can grab the path from the cache
		if (this.routingCache.isRoute(node)) {
			path = routingCache.getPathFromRoutingCache(node);
		// haven't visited this MessagingNode yet, need to construct the shortestPath to it and cache it when complete
		} else {
			ShortestPath shortestPath = new ShortestPath(this.overlay);
			shortestPath.execute(me);
			path = new ArrayList<>(shortestPath.getPath(node));
			routingCache.addPath(node, path);
			System.out.println("My path is (starting, to): ");
			System.out.println(path);
		}
		
		// DANIEL: while testing tomorrow, attempt using this format instead
		/**
		// destination node has not been routed yet
		if (!this.routingCache.isRoute(node)) {
			ShortestPath shortestPath = new ShortestPath(this.overlay);
			shortestPath.execute(me);
			path = new ArrayList<>(shortestPath.getPath(node));
			routingCache.addPath(node, path);
			System.out.println("My path is (starting, to): ");
			System.out.println(path);
		// destination node has already been routed before
		} else {
			path = this.routingCache.getPathFromRoutingCache(node);
		}
		**/
		
		// payload of each message is a random integer with values that range from 2147483647 to -2147483648, same as all the values that an int can hold in java
		int payload = random.nextInt();
		Message message = new Message(me, node, payload, path);
		if (DEBUG) { System.out.println("MessagingNode about to send message type: " + message.getType()); }
		this.incrementSentCounter();
		this.addSentSummation(payload);
        
		// send Message to the next node on the pathlist, 0 index = this node, so choose the next node in the array
		sendMessageToMessagingNode(path.get(1), message);
		
		if (DEBUG) { System.out.println("end MessagingNode prepMessageToMessagingNode"); }
	}
	
	private void sendMessageToMessagingNode(NodeInformation node, Message message) {
		if (DEBUG) { System.out.println("begin MessagingNode sendMessageToMessagingNode"); }
		try {
			if (DEBUG) { System.out.println("Sending to " + node.getNodeIPAddress() + " on Port " + node.getNodePortNumber()); }
			
			messagingNodeSenders.get(node).sendData(message.getBytes());
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
		if (DEBUG) { System.out.println("end MessagingNode sendMessageToMessagingNode"); }
	}
	
	private void sendTaskComplete() {
		if (DEBUG) { System.out.println("begin MessagingNode sendTaskComplete"); }
		try {
			if (DEBUG) { System.out.println(String.format("Attempting to send a message to the Registry at: %s:%d", this.registryHostName, this.registryHostPortNumber)); }

			TaskComplete taskComplete = new TaskComplete(new NodeInformation(this.registryHostName, this.registryHostPortNumber));
			
			if (DEBUG) { System.out.println("MessagingNode about to send message type: " + taskComplete.getType()); }
			if (DEBUG) { System.out.println("Sending to " + this.registryHostName + " on Port " + this.registryHostPortNumber); }
			
			this.registrySender.sendData(taskComplete.getBytes());
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
		
		if (DEBUG) { System.out.println("end MessagingNode sendTaskComplete"); }
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
		if (DEBUG) { System.out.println("begin MessagingNode handleTaskSummaryRequest"); }
		try {
			if (DEBUG) { System.out.println(String.format("Attempting to connect to registry at: %s:%d", this.registryHostName, this.registryHostPortNumber)); }
			TaskSummaryResponse taskSummaryReponse = new TaskSummaryResponse(this.localHostIPAddress, this.localHostPortNumber, this.sendTracker, this.sendSummation, this.receiveTracker, this.receiveSummation, this.relayTracker);
			
			if (DEBUG) { System.out.println("MessagingNode about to send message type: " + taskSummaryReponse.getType()); }
			if (DEBUG) { System.out.println("Sending to " + this.registryHostName + " on Port " + this.registryHostPortNumber); }
			
			this.registrySender.sendData(taskSummaryReponse.getBytes());

			// after the taskSummary has been sent to the Registry, reset all values
			clearSentCounter();
			clearReceivedCounter();
			clearRelayCounter();
			clearReceivedSummation();
			clearSendSummation();
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
		if (DEBUG) { System.out.println("end MessagingNode handleTaskSummaryRequest"); }
	}

	private void handleMessage(Event event) {
		if (DEBUG) { System.out.println("begin MessagingNode handleMessage"); }
		Message message = (Message) event;
		if (DEBUG) { System.out.println("Got a Message from " + message.getSourceNode().getNodeIPAddress()); }
		
		ArrayList<NodeInformation> routePath = message.getRoutePath();
		
        NodeInformation me = new NodeInformation(this.localHostIPAddress, this.localHostPortNumber);
        
        if (message.getDestinationNode().equals(me)) {
            this.incrementReceivedCounter();
            this.addReceiveSummation(message.getPayload());
            return;
        }
        
        // Find the next node in the routing path
        ListIterator<NodeInformation> node = routePath.listIterator();
        NodeInformation nextNode = null;
        while (node.hasNext()) {
        	NodeInformation current = node.next();
            if (current.equals(me))
            {
                nextNode = node.next();
                System.out.println("Message received, routing to " + nextNode);
                break;
            }
        }
        if (nextNode != null) {
            this.incrementRelayCounter();
            this.sendMessageToMessagingNode(nextNode, message);
        }
        
     // DANIEL: while testing tomorrow, attempt using this format instead
		// end of the list, this node is the destination for the route path
        /**
		if (message.getDestinationNode().getNodeIPAddress().equals(me)) {
			this.incrementReceivedCounter();
			this.addReceiveSummation(message.getPayload());
		// this node is not the end, need to find the next node on the route path
		} else {
			NodeInformation nextNode = null;
			for (int i=0; i < routePath.size(); i++) {
				nextNode = routePath.get(i);
				if (DEBUG) { System.out.println("Checking to send the next message to: " + nextNode.getNodeIPAddress()); }
				if (nextNode.equals(me) {
					if ((i + 1) >= routePath.size()) {
						System.out.println("Out of bounds for Route Path.");
						nextNode = null;
						break;
					} else {
						nextNode = routePath.get(i + 1);
						break;
					}
				}
			}
			
			if (nextNode != null) {
				this.incrementRelayCounter();
				this.sendMessageToMessagingNode(nextNode, message);
			}
		}
		**/
		if (DEBUG) { System.out.println("end MessagingNode handleMessage"); }
	}
	
	private void printShortestPath() {
		System.out.println("Coming soon");
	}
	
	private void printNodes() {
		System.out.println("The total number of neighborNodes is a total of " + this.neighborNodes.size() + " nodes.");
		if (!this.neighborNodes.isEmpty()) {
			for (NodeInformation ni : this.neighborNodes) {
				System.out.println("The nodes in neighborNodes is: " + ni.getNodeIPAddress());
			}
		}
		
		System.out.println("The nodes that are in otherNodes contains " + this.nonNeighborNodes.size() + " nodes.");
		if (!this.nonNeighborNodes.isEmpty()) {
			for (NodeInformation ni : this.nonNeighborNodes) {
				System.out.println("The Nodes in nonNeighborNodes: " + ni.getNodeIPAddress());
			}
		}
		
		if (!this.edgesList.isEmpty()) {
			System.out.println("Edges associated with me are: ");
			for (Edge e : this.edgesList) {
				System.out.println("Edge Source: " + e.getSourceNode().getNodeIPAddress() + " Edge Destination: " + e.getDestationNode().getNodeIPAddress() + " with Weight: " + e.getWeight());
			}
		}
		
		System.out.println("The total number of nodes I'm Connected to is a total of " + this.connectedNeighborNodes.size() + " nodes.");
		if (!this.connectedNeighborNodes.isEmpty()) {
			for (NodeInformation ni : this.connectedNeighborNodes) {
				System.out.println("The nodes in connectedNeighborNodes is: " + ni.getNodeIPAddress());
			}
		}
	}
	
	/**
     * Create this as synchronized so that two threads can't update any counter simultaneously.
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
