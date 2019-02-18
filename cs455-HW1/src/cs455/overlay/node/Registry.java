package cs455.overlay.node;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Scanner;

import cs455.overlay.dijkstra.Edge;
import cs455.overlay.dijkstra.ShortestPath;
import cs455.overlay.transport.TCPSender;
import cs455.overlay.transport.TCPServerThread;
import cs455.overlay.util.OverlayCreator;
import cs455.overlay.util.StatisticsCollectorAndDisplay;
import cs455.overlay.wireformats.DeregisterRequest;
import cs455.overlay.wireformats.DeregisterResponse;
import cs455.overlay.wireformats.Event;
import cs455.overlay.wireformats.LinkWeights;
import cs455.overlay.wireformats.MessagingNodesList;
import cs455.overlay.wireformats.Protocol;
import cs455.overlay.wireformats.RegisterRequest;
import cs455.overlay.wireformats.RegisterResponse;
import cs455.overlay.wireformats.TaskInitiate;
import cs455.overlay.wireformats.TaskSummaryRequest;
import cs455.overlay.wireformats.TaskSummaryResponse;

/**
 * The registry maintains information about the registered messaging nodes in a registry; you can use any
 * data structure for managing this registry but make sure that your choice can support all the operations
 * that you will need.
 * The Registry can be run after being compiled using the following format:
 * java cs455.overlay.node.Registry portnum(integer)
 */

public class Registry implements Node {
	
	private static final boolean DEBUG = false;
	private int portNumber;
	private ArrayList<NodeInformation> nodesList;
	private OverlayCreator overlay;
	private int numberOfRounds;
	private StatisticsCollectorAndDisplay trafficSummary;
	private ArrayList<NodeInformation> unsentNodes;
	private HashMap<NodeInformation, TCPSender> messagingNodeSenders;
	
	public Registry(int portNumber) {
		this.portNumber = portNumber;
		this.nodesList = new ArrayList<>();
		this.messagingNodeSenders = new HashMap<NodeInformation, TCPSender>();
		
		try {
			TCPServerThread registryServerThread = new TCPServerThread(this.portNumber, this);
			registryServerThread.start();
			System.out.println("Registry TCPServerThread running.");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) {
		
		// requires 1 argument to initialize a registry
		if(args.length != 1) {
            System.out.println("Invalid Arguments. Must include a port number.");
            return;
        }
		
		int registryHostPortNumber = 0;
		
		try {
			registryHostPortNumber = Integer.parseInt(args[0]);
		} catch (NumberFormatException nfe) {
			System.out.println("Invalid argument. Argument must be a number.");
			nfe.printStackTrace();
		}
		
		Registry registry = new Registry(registryHostPortNumber);
		
		String registryIP = "";
        try{
            registryIP = InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            System.out.println(e.getMessage());
        }

        System.out.println(String.format("Registry Started and awaiting orders on port %d and IP address %s.", registry.portNumber, registryIP));
        handleUserInput(registry);
	}
	
	private static void handleUserInput(Registry registrynode) {
		Scanner scan = new Scanner(System.in);
		
		System.out.println("Registry ready for input.");
		
        while(true) {
        	System.out.println("Please enter your command: ");
            String response = scan.nextLine();
            if (DEBUG) { System.out.println("User entered: " + response); }
            
            if (response.equals("list-messaging-nodes")) {
            	System.out.println("Listing links of messaging nodes:");
            	registrynode.listMessagingNodes();
            } else if (response.equals("list-weights")) {
            	System.out.println("Starting List-Weights:");
            	registrynode.listWeights();
            // e.g. setup-overlay 4 (4 is the default connections this project will apply)
            } else if (response.startsWith("setup-overlay")) {
            	try {
            		int numConnections = Integer.parseInt(response.replaceAll("[^\\d.]", ""));
            		if (registrynode.nodesList.size() < 1) {
                		System.out.println("Unable to perform setup-overlay, not enough nodes have connected to the Registry yet.");
                	} else if (numConnections < 1) {
                		System.out.println("Unable to perform setup-overlay, number of connections must be greater than 1.");
                	} else {
                		System.out.println("Starting setup-overlay with: " + numConnections + " Number of Connections.");
                    	registrynode.setupOverlay(numConnections);
                	}
            	} catch (NumberFormatException nfe) {
            		System.out.println("Invalid argument. Argument must be a number.");
        			nfe.printStackTrace();
            	}
            } else if (response.equals("send-overlay-link-weights")) {
            	System.out.println("Sending link-weights to messaging nodes");
            	registrynode.sendOverlayLinkWeights();
            } else if (response.startsWith("start")) {
            	try {
            		System.out.println("Starting rounds");
            		int numRounds = Integer.parseInt(response.replaceAll("[^\\d.]", ""));
            		registrynode.startRounds(numRounds);
            	} catch (NumberFormatException nfe) {
            		System.out.println("Invalid argument. Argument must be a number.");
        			nfe.printStackTrace();
            	}
            } else {
            	System.out.println("Command unrecognized");
            }
        }
    }

	@Override
	public synchronized void onEvent(Event event) {
		int eventType = event.getType();
		System.out.println("Event Type " + eventType + " passed to Registry.");
		switch(eventType) {
			// REGISTER_REQUEST = 6000
			case Protocol.REGISTER_REQUEST:
				handleRegisterRequest(event);
				break;
			// DEREGISTER_REQUEST = 6002
			case Protocol.DEREGISTER_REQUEST:
				handleDeregisterRequest(event);
				break;
			// TASK_COMPLETE = 6007
			case Protocol.TASK_COMPLETE:
				handleTaskComplete(event);
				break;
			// TRAFFIC_SUMMARY = 6009
			case Protocol.TRAFFIC_SUMMARY:
				handleTaskSummaryResponse(event);
				break;
			default:
				System.out.println("Invalid Event to Node.");
				return;
			}
	}

	@Override
	public void setLocalHostPortNumber(int portNumber) {
		this.portNumber = portNumber;
	}
	
	// Registry allows messagingNodes to register themselves. This is performed when a messagingNode starts up for the first time.
	private void handleRegisterRequest(Event event) {
		if (DEBUG) { System.out.println("begin Registry handleRegisterRequest"); }
		RegisterRequest registerRequest = (RegisterRequest) event;
		String IP = registerRequest.getIPAddress();
		int port = registerRequest.getPortNumber();
		
		if (DEBUG) { System.out.println("Registry received a message type: " + registerRequest.getType()); }
		
		System.out.println("Registry received a registerRequest from IP: " + IP + " on Port: " + String.valueOf(port) + ".");
		
		NodeInformation ni = new NodeInformation(IP, port);
		
		try {
			Socket socket = new Socket(IP, port);
			TCPSender sender = new TCPSender(socket);
			
			byte status = 0;
			String message = "";
			
			// success, node is not currently registered so adding to the list of nodes
			if (!this.nodesList.contains(ni)) {
				this.nodesList.add(ni);
				this.messagingNodeSenders.put(ni, sender);
				System.out.println("Registration request successful. The number of messaging nodes currently constituting the Registry is (" + this.nodesList.size() + ")");
				status = (byte) 1;
				message = "Node Registered";
			} else {
				status = (byte) 0;
				message = "Node already registered. No action taken";
			}
			RegisterResponse registerResponse = new RegisterResponse(status, message);
			sender.sendData(registerResponse.getBytes());
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
		if (DEBUG) { System.out.println("end Registry handleRegisterRequest"); }
	}
	
	/**
	 * Used for when the Registry receives a request, it checks to see if the node had previously registered and ensures
	 * the IP address in the message matches the address where the request originated
	 * Message Type (int): REGISTER_RESPONSE (6001)
	 * Status Code (byte): SUCCESS or FAILURE
	 * Additional Info (String):
	 */
	
	// daniel this is no longer being used, you may be able to delete this method after testing
	/**
	private void sendRegistrationResponse(RegisterRequest registerRequest, byte status, String message, NodeInformation node) {
		try {
			Socket socket = new Socket(registerRequest.getIPAddress(), registerRequest.getPortNumber());
			TCPSender sender = new TCPSender(socket);
			
			RegisterResponse registerResponse = new RegisterResponse(status, message);
			
			
			sender.sendData(registerResponse.getBytes());
			//socket.close();
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
	}
	**/
	
	// Allows messaging nodes to deregister themselves. This is performed when a messaging node leaves the overlay.
	private synchronized void handleDeregisterRequest(Event event) {
		if (DEBUG) { System.out.println("begin Registry handleDeregisterRequest"); }
		DeregisterRequest deregisterRequest = (DeregisterRequest) event;
		
		if (DEBUG) { System.out.println("Registry received a message type: " + deregisterRequest.getType()); }
		
		String IP = deregisterRequest.getIPAddress();
		int port = deregisterRequest.getPortNumber();
		
		if (DEBUG) { System.out.println("Got Deregister Request from IP: " + IP + " on Port: " + String.valueOf(port) + "."); }
		
		NodeInformation ni = new NodeInformation(IP, port);
		
		// success, node is currently registered so will remove it from the list of nodes and the hashmap for nodes to message
		if (this.nodesList.contains(ni)) {
			this.nodesList.remove(ni);
			this.messagingNodeSenders.remove(ni);
			this.sendDeregistrationResponse(deregisterRequest, (byte) 1, "Node Deregistered");
		} else {
			this.sendDeregistrationResponse(deregisterRequest, (byte) 0, "Node not in Registry. No action taken");
		}
		if (DEBUG) { System.out.println("end Registry handleDeregisterRequest"); }
	}
	
	/**
	 * The Registry Node needs to respond when a messaging node exits and is trying to deregister itself
	 * Message Type (int): DEREGISTER_REQUEST (6003)
	 * Status Code (byte): SUCCESS or FAILURE
	 * Additional Info (String):
	 */
	private void sendDeregistrationResponse(DeregisterRequest deregisterRequest, byte status, String message) {
		if (DEBUG) { System.out.println("begin Registry sendDeregistrationResponse"); }
		try {
			Socket socket = new Socket(deregisterRequest.getIPAddress(), deregisterRequest.getPortNumber());
			TCPSender sender = new TCPSender(socket);
			
			if (DEBUG) { System.out.println("Sending to " + deregisterRequest.getIPAddress() + " on Port " + deregisterRequest.getPortNumber()); }
			
			DeregisterResponse deregisterResponse = new DeregisterResponse(status, message);
			
			if (DEBUG) { System.out.println("Registry about to send a message type: " + deregisterResponse.getType()); }
			
			sender.sendData(deregisterResponse.getBytes());
			socket.close();
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
		if (DEBUG) { System.out.println("end Registry sendDeregistrationResponse"); }
	}

	private void handleTaskComplete(Event event) {
		if (DEBUG) { System.out.println("begin Registry handleTaskComplete"); }
		
		if (DEBUG) { System.out.println("Registry received a message type: " + event.getType()); }
		
		if (this.unsentNodes.isEmpty()) {
			try {
				// After all MessagingNodes report task completion, wait ~15 seconds before sending request to collect statistics
				// Use Thread.sleep() in the registry (this is the ONLY place you should use this)
				System.out.println("After all MessagingNodes report task completion, wait ~15 seconds before sending request to collect statistics.");
				Thread.sleep(15000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			sendTaskSummaryRequest();
			return;
		}
		
		// still nodes to send the numbverOfRounds to, continue going through list
		NodeInformation node = this.unsentNodes.remove(0);
		if (DEBUG) { System.out.println("end Registry handleTaskComplete"); }
		sendTaskInitiate(node);
		
	}
	
	private void sendTaskSummaryRequest() {
		if (DEBUG) { System.out.println("begin Registry sendTaskSummaryRequest"); }
		
		for (NodeInformation ni : this.nodesList) {
			try {
				
				if (DEBUG) { System.out.println("Sending to " + ni.getNodeIPAddress() + " on Port " + ni.getNodePortNumber()); }
				
				TaskSummaryRequest taskSummaryRequest = new TaskSummaryRequest();
				
				if (DEBUG) { System.out.println("Registry about to send a message type: " + taskSummaryRequest.getType()); }
				
				this.messagingNodeSenders.get(ni).sendData(taskSummaryRequest.getBytes());

			} catch  (IOException ioe) {
				ioe.printStackTrace();
			}
		}
		
		if (DEBUG) { System.out.println("end Registry sendTaskSummaryRequest"); }
	}

	private synchronized void handleTaskSummaryResponse(Event event) {
		if (DEBUG) { System.out.println("begin Registry handleTaskSummaryResponse"); }
		
		TaskSummaryResponse taskSummaryResponse = (TaskSummaryResponse) event;
		
		if (DEBUG) { System.out.println("Registry received a message type: " + taskSummaryResponse.getType()); }
		
		this.trafficSummary.addTrafficSummary(taskSummaryResponse);
		
		if (DEBUG) { System.out.println("end Registry handleTaskSummaryResponse"); }
	}
	
	/**
	 * This should result in information about the messaging nodes (hostname, and port number) being listed. Information for each messaging node should be listed on a separate line.
	 */
	private void listMessagingNodes() {
		if (DEBUG) { System.out.println("begin Registry listMessagingNodes"); }
		
		if (!nodesList.isEmpty()) {
			for (NodeInformation ni : nodesList) {
				System.out.println("hostname: " + ni.getNodeIPAddress() + "/tport number: " + ni.getNodePortNumber());
			}
		} else {
			System.out.println("No Nodes are currently registered with the Registry.");
		}
		
		if (DEBUG) { System.out.println("begin Registry listMessagingNodes"); }
	}
	
	/**
	 * 	Enables the construction of the overlay by orchestrating connections that a messagingNode initiates with other messaging nodes in the system. 
	 *  Based on its knowledge of the messagingNodes (through a function) the registry informs messaging nodes about the other messaging nodes that they should connect to.
	 */
	private void setupOverlay(int numberOfConnections) {
		if (DEBUG) { System.out.println("begin Registry setupOverlay"); }
		
		if (this.nodesList.size() >= 10) {
			this.overlay = new OverlayCreator(this.nodesList);
			this.overlay.createOverlay(numberOfConnections);
			sendMessagingNodesList();
		} else {
			System.out.println("Minimum number of 10 Nodes must be registered to begin the overlay.");
		}
		
		if (DEBUG) { System.out.println("end Registry setupOverlay"); }
		
	}
	
	private void sendMessagingNodesList() {
		if (DEBUG) { System.out.println("begin Registry sendMessagingNodesList"); }
		if (this.nodesList.size() > 0) {
			// send a message to all nodes that the overlay has been created and tell them their connections
			for (NodeInformation node : nodesList) {
		        
				// set all the nodes in this overlay with a shortest path that can use said overlay in finding the path for each node to send its neighborNodes
				ShortestPath sp = new ShortestPath(this.overlay);
				sp.execute(node);
				ArrayList<NodeInformation> neighborNodes = new ArrayList<>(this.overlay.getNeighborNodes(node));
				
				try {		
					if (DEBUG) { System.out.println("Sending to " + node.getNodeIPAddress() + " on Port " + node.getNodePortNumber()); }
					
					MessagingNodesList messagingNodesList = new MessagingNodesList(neighborNodes);
					
					if (DEBUG) { System.out.println("Registry about to send a message type: " + messagingNodesList.getType()); }
					
					this.messagingNodeSenders.get(node).sendData(messagingNodesList.getBytes());
				} catch (IOException ioe) {
					ioe.printStackTrace();
				}
			}
		} else {
			System.out.println("No Nodes Registered to send MessagingNodesList to.");
		}
		if (DEBUG) { System.out.println("end Registry sendMessagingNodesList"); }
	}
	
	private void sendOverlayLinkWeights() {
		if (DEBUG) { System.out.println("begin Registry sendOverlayLinkWeights"); }
		
		if (!this.nodesList.isEmpty()) {
			if (this.overlay != null) {
				ArrayList<Edge> edgesList = this.overlay.getEdgesList();
				for (NodeInformation ni : this.nodesList) {
					try {
						if (DEBUG) { System.out.println("Sending to " + ni.getNodeIPAddress() + " on Port " + ni.getNodePortNumber()); }
						
						LinkWeights linkWeights = new LinkWeights(edgesList);
						
						if (DEBUG) { System.out.println("Registry about to send a message type: " + linkWeights.getType()); }
						
						this.messagingNodeSenders.get(ni).sendData(linkWeights.getBytes());
					} catch  (IOException ioe) {
						ioe.printStackTrace();
					}
				}
			} else {
				System.out.println("Overlay must be created before being able to send link weights.");
			}
		} else {
			System.out.println("No Nodes Registered to send MessagingNodesList to.");
		}
		
		if (DEBUG) { System.out.println("end Registry sendOverlayLinkWeights"); }
	}
	
	/**
	 * lists information about links comprising the overlay. Each link’s information should be on a separate line and include information about the nodes that it connects and the weight of that link.
	 */
	private void listWeights() {
		if (DEBUG) { System.out.println("begin Registry listWeights"); }
		
		if (this.overlay != null) {
			if (!this.overlay.getEdgesList().isEmpty()) {
				for (Edge e : this.overlay.getEdgesList()) {
					System.out.println(e.getSourceNode().getNodeIPAddress() + ":" + e.getSourceNode().getNodePortNumber() + " " + e.getDestationNode().getNodeIPAddress() + ":" + e.getDestationNode().getNodePortNumber() + " " + e.getWeight());
				}
			} else {
				System.out.println("Overlay does not have any edges created for it yet.");
			}
		} else {
			System.out.println("Overlay has not been created yet.");
		}
		
		if (DEBUG) { System.out.println("end Registry listWeights"); }
	}
	
	private void startRounds(int numberOfRounds) {
		if (DEBUG) { System.out.println("begin Registry startRounds"); }
		
		if (!this.nodesList.isEmpty()) {
			this.numberOfRounds = numberOfRounds;
			this.trafficSummary = new StatisticsCollectorAndDisplay(this.nodesList.size());
			this.unsentNodes = new ArrayList<>(this.nodesList);
			
			NodeInformation node = this.unsentNodes.remove(0);
			
			sendTaskInitiate(node);
		}
		if (DEBUG) { System.out.println("end Registry startRounds"); }
	}
	
	private void sendTaskInitiate(NodeInformation node) {
		if (DEBUG) { System.out.println("begin Registry sendTaskInitiate"); }
		
		try {
			if (DEBUG) { System.out.println("Sending TaskInitiate to " + node.getNodeIPAddress() + " at port number " + node.getNodePortNumber()); }
			
			TaskInitiate taskInitiate = new TaskInitiate(this.numberOfRounds);
			
			if (DEBUG) { System.out.println("Registry about to send a message type: " + taskInitiate.getType() + "with number " + taskInitiate.getNumberOfRounds()); }
			
			this.messagingNodeSenders.get(node).sendData(taskInitiate.getBytes());
		} catch  (IOException ioe) {
			ioe.printStackTrace();
		}
		
		if (DEBUG) { System.out.println("end sendTaskInitiate"); }
	}
}
