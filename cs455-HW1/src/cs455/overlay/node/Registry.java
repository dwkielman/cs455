package cs455.overlay.node;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Scanner;

import cs455.overlay.dijkstra.Edge;
import cs455.overlay.dijkstra.ShortestPath;
import cs455.overlay.transport.TCPSender;
import cs455.overlay.transport.TCPServerThread;
import cs455.overlay.util.OverlayCreator;
import cs455.overlay.wireformats.DeregisterRequest;
import cs455.overlay.wireformats.DeregisterResponse;
import cs455.overlay.wireformats.Event;
import cs455.overlay.wireformats.LinkWeights;
import cs455.overlay.wireformats.MessagingNodesList;
import cs455.overlay.wireformats.Protocol;
import cs455.overlay.wireformats.RegisterRequest;
import cs455.overlay.wireformats.RegisterResponse;

/**
 * The registry maintains information about the registered messaging nodes in a registry; you can use any
 * data structure for managing this registry but make sure that your choice can support all the operations
 * that you will need.
 */

//java cs455.overlay.node.Registry portnum

public class Registry implements Node {
	
	private static final boolean DEBUG = true;
	private int portNumber;
	private ArrayList<NodeInformation> nodesList;
	private OverlayCreator overlay;
	
	public Registry(int portNumber) {
		this.portNumber = portNumber;
		this.nodesList = new ArrayList<>();
		
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
		System.out.println("Registry main()");
		
		System.out.println("Registry accepting commands...");
        while(true) {
            System.out.println("Enter command: ");
            String response = scan.nextLine();
            System.out.println("You typed: " + response);
            
            if (response.equals("list-messaging-nodes")) {
            	System.out.println("Listing links of messaging nodes:");
            	registrynode.listMessagingNodes();
            } else if (response.equals("list-weights")) {
            	System.out.println("Starting List-Weights:");
            // e.g. setup-overlay 4 (4 is the default connections this project will apply)
            } else if (response.startsWith("setup-overlay")) {
            	try {
            		int numConnections = Integer.parseInt(response.replaceAll("[^\\d.]", ""));
            		if (registrynode.nodesList.size() < 1) {
                		System.out.println("Unable to perform setup-overlay, not enough nodes have connected to the Registry yet.");
                	} else if (numConnections < 1) {
                		System.out.println("Unable to perform setup-overlay, number of connectsion must be greater than 1.");
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
            } else if (response.equals("start")) {
            	System.out.println("Starting rounds");
            } else {
            	System.out.println("Command unrecognized");
            }
        }
    }

	@Override
	public void onEvent(Event event) {
		int eventType = event.getType();
		System.out.println("Event " + eventType + "Passed to Registry.");
		switch(eventType) {
			// REGISTER_REQUEST = 6000
			case Protocol.REGISTER_REQUEST:
				handleRegisterRequest(event);
				break;
			// DEREGISTER_REQUEST = 6002
			case Protocol.DEREGISTER_REQUEST:
				handleDeregisterRequest(event);
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
			// TASK_COMPLETE = 6007
			case Protocol.TASK_COMPLETE:
				handleTaskComplete(event);
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

	@Override
	public void setLocalHostPortNumber(int portNumber) {
		this.portNumber = portNumber;
	}
	
	private synchronized void handleRegisterRequest(Event event) {
		System.out.println("begin handleRegisterRequest");
		RegisterRequest registerRequest = (RegisterRequest) event;
		String IP = registerRequest.getIPAddress();
		int port = registerRequest.getPortNumber();
		
		System.out.println("Got register request from IP: " + IP + " Port: " + String.valueOf(port) + ".");
		
		NodeInformation ni = new NodeInformation(IP, port);
		
		// success, node is not currently registered so adding to the list of nodes
		if (!this.nodesList.contains(ni)) {
			this.nodesList.add(ni);
			System.out.println("Registration request successful. The number of messaging nodes currently constituting the overlay is (" + this.nodesList.size() + ")");
			this.sendRegistrationResponse(registerRequest, (byte) 1, "Node Registered");
		} else {
			this.sendRegistrationResponse(registerRequest, (byte) 0, "Node already registered. No action taken");
		}
		System.out.println("end handleRegisterRequest");
	}
	
	/**
	 * Used for when the Registry receives a request, it checks to see if the node had previously registered and ensures
	 * the IP address in the message matches the address where the request originated
	 * Message Type (int): REGISTER_RESPONSE (6001)
	 * Status Code (byte): SUCCESS or FAILURE
	 * Additional Info (String):
	 */
	private void sendRegistrationResponse(RegisterRequest registerRequest, byte status, String message) {
		System.out.println("begin sendRegistrationResponse");
		try {
			Socket socket = new Socket(registerRequest.getIPAddress(), registerRequest.getPortNumber());
			TCPSender sender = new TCPSender(socket);
			
			System.out.println("Sending to " + registerRequest.getIPAddress() + " on Port " + registerRequest.getPortNumber());
			
			RegisterResponse registerResponse = new RegisterResponse(status, message);
			sender.sendData(registerResponse.getBytes());
			socket.close();
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
		System.out.println("end sendRegistrationResponse");
	}
	
	private void handleDeregisterRequest(Event event) {
		System.out.println("begin handleDeregisterRequest");
		DeregisterRequest deregisterRequest = (DeregisterRequest) event;
		String IP = deregisterRequest.getIPAddress();
		int port = deregisterRequest.getPortNumber();
		
		System.out.println("Got register request from IP: " + IP + " Port: " + String.valueOf(port) + ".");
		
		NodeInformation ni = new NodeInformation(IP, port);
		
		// success, node is not currently registered so adding to the list of nodes
		if (this.nodesList.contains(ni)) {
			this.nodesList.remove(ni);
			this.sendDeregistrationResponse(deregisterRequest, (byte) 1, "Node Deregistered");
		} else {
			this.sendDeregistrationResponse(deregisterRequest, (byte) 0, "Node not in Registry. No action taken");
		}
		System.out.println("end handleDeregisterRequest");
	}
	
	/**
	 * The Registry Node needs to respond when a messaging node exits and is trying to deregister itself
	 * Message Type (int): DEREGISTER_REQUEST (6003)
	 * Status Code (byte): SUCCESS or FAILURE
	 * Additional Info (String):
	 */
	private void sendDeregistrationResponse(DeregisterRequest deregisterRequest, byte status, String message) {
		System.out.println("begin sendDeregistrationResponse");
		try {
			Socket socket = new Socket(deregisterRequest.getIPAddress(), deregisterRequest.getPortNumber());
			TCPSender sender = new TCPSender(socket);
			
			System.out.println("Sending to " + deregisterRequest.getIPAddress() + " on Port " + deregisterRequest.getPortNumber());
			
			DeregisterResponse deregisterResponse = new DeregisterResponse(status, message);
			sender.sendData(deregisterResponse.getBytes());
			socket.close();
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
		System.out.println("end sendDeregistrationResponse");
	}

	private void handleMessagingNodesList(Event event) {
		
	}

	private void handleLinkWeights(Event event) {
		
	}

	private void handleTaskInitiate(Event event) {
		
	}

	private void handleTaskComplete(Event event) {
		
	}

	private void handleTaskSummaryResponse(Event event) {
		
	}

	private void handleMessage(Event event) {
		
	}
	
	/**
	 * This should result in information about the messaging nodes (hostname, and port-number) being listed. Information for each messaging node should be listed on a separate line.
	 */
	private void listMessagingNodes() {
		for (NodeInformation ni : nodesList) {
			System.out.println("hostname: " + ni.getNodeIPAddress() + "/tport number: " + ni.getNodePortNumber());
		}
	}
	
	private void setupOverlay(int numberOfConnections) {
		this.overlay = new OverlayCreator(this.nodesList);
		this.overlay.createOverlay(numberOfConnections);
		
		sendMessagingNodesList();
	}
	
	private void sendMessagingNodesList() {
		System.out.println("begin sendMessagingNodesList");
		if (this.nodesList.size() > 0) {
			// send a message to all nodes that the overlay has been created and tell them their connections
			for (NodeInformation node : nodesList) {
				ShortestPath sp = new ShortestPath(this.overlay);
				sp.execute(node);
				ArrayList<NodeInformation> neighborNodes = new ArrayList<>(this.overlay.getNeighborNodes(node));
				
				try {		
					Socket socket = new Socket(node.getNodeIPAddress(), node.getNodePortNumber());
					TCPSender sender = new TCPSender(socket);
					
					if (DEBUG) {
						System.out.println("Sending to " + node.getNodeIPAddress() + " on Port " + node.getNodePortNumber());
					}
					
					MessagingNodesList messingNodesList = new MessagingNodesList(neighborNodes);
					sender.sendData(messingNodesList.getBytes());
					socket.close();
				} catch (IOException ioe) {
					ioe.printStackTrace();
				}
			}
		} else {
			System.out.println("No Nodes Registered to send MessagingNodesList to.");
		}
		System.out.println("end sendMessagingNodesList");
	}
	
	private void sendOverlayLinkWeights() {
		System.out.println("begin sendOverlayLinkWeights");
		ArrayList<Edge> edgesList = this.overlay.getEdgesList();
		
		for (NodeInformation ni : this.nodesList) {
			try {
				Socket socket = new Socket(ni.getNodeIPAddress(), ni.getNodePortNumber());
				TCPSender sender = new TCPSender(socket);
				
				if (DEBUG) {
					System.out.println("Sending to " + ni.getNodeIPAddress() + " on Port " + ni.getNodePortNumber());
				}
				
				LinkWeights linkWeights = new LinkWeights(edgesList);
				sender.sendData(linkWeights.getBytes());
				socket.close();
			} catch  (IOException ioe) {
				ioe.printStackTrace();
			}
		}
		System.out.println("end sendOverlayLinkWeights");
	}
	// Allows messaging nodes to register themselves. This is performed when a messaging node starts up for the first time.

	// Allows messaging nodes to deregister themselves. This is performed when a messaging node leaves the overlay.
	
	/**
	 * 	Enables the construction of the overlay by orchestrating connections that a messaging node initiates with other messaging nodes in the system. 
	 *  Based on its knowledge of the messagingnodes (through function A) the registry informs messaging nodes about the other messaging nodes that they should connect to.
	 */
	
	// Assign and publish weights to the links connecting any two messaging nodes in the overlay. The weights these links take will range from 1-10.
	
}
