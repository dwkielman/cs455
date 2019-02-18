package cs455.overlay.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import cs455.overlay.dijkstra.Edge;
import cs455.overlay.node.NodeInformation;

/**
 * Weighted overlay consisting of MessagingNodes and Edges.
 */

public class OverlayCreator {

	private ArrayList<NodeInformation> nodesList;
	private ArrayList<Edge> edgesList;
	
	public OverlayCreator(ArrayList<NodeInformation> nodesList) {
		this.nodesList = nodesList;
		this.edgesList = new ArrayList<>();
	}
	
	public void createOverlay(int numberOfConnections) {
		Random random = new Random();
		
		// connect adjacent nodes in a circular fashion, ensures no partitions in overlay
		// Assign and publish weights to the links connecting any two messaging nodes in the overlay. The weights these links take will range from 1-10.
		for (int i=0; i < this.nodesList.size(); i++) {
			this.edgesList.add(new Edge(this.nodesList.get(i % nodesList.size()), this.nodesList.get((i + 1) % nodesList.size()), random.nextInt(10) + 1));
			this.nodesList.get(i % nodesList.size()).addConnection();
			this.nodesList.get((i + 1) % nodesList.size()).addConnection();
		}
		
		// use some randomnization to make the overlay a little more dynamic
		Collections.shuffle(this.nodesList);
		
		// link the rest of the nodes to the number of connections they are required to have
		for (NodeInformation n : nodesList) {
			boolean addNode = false;
			
			// get another node to attempt a connection to
			for (int i=0; i < this.nodesList.size(); i++) {
				// node exceeded connections, move to next node
				if (n.getNumberOfConnections() >= (numberOfConnections)) {
					break;
				} else {
					NodeInformation nodeToConnect = this.nodesList.get(i);
					
					// ensure that we are not attempting to connect to the same node and that it isn't at max connections yet
					if ((!nodeToConnect.equals(n)) && (nodeToConnect.getNumberOfConnections() < (numberOfConnections))) {
						// ensure that these two nodes aren't already connected
						for (Edge e : edgesList) {
							if ((e.getSourceNode().equals(n) && e.getDestationNode().equals(nodeToConnect)) || ((e.getSourceNode().equals(nodeToConnect) && e.getDestationNode().equals(n)))) {
								addNode = false;
								break;
							} else {
								addNode = true;
							}
						}
						
						if (addNode) {
							// set a random weight between 1-10 to the Edge
							this.edgesList.add(new Edge(n, nodeToConnect, random.nextInt(10) + 1));
							n.addConnection();
							nodeToConnect.addConnection();
						}
					}
				}
			}
		}
	}
	
	// messagingNodes need to reconstruct the overlay using only a list of Edges
	public void createOverlayFromEdges(ArrayList<Edge> edgesList) {
		for (Edge e : edgesList) {
			this.edgesList.add(new Edge(e.getSourceNode(), e.getDestationNode(), e.getWeight()));
		}
	}
	
	public ArrayList<NodeInformation> getNodesList() {
		return this.nodesList;
	}
	
	public ArrayList<Edge> getEdgesList() {
		return this.edgesList;
	}
	
	public int getConnectionCount(NodeInformation n) {
		int count = 0;
		for (Edge e : edgesList) {
			if (edgesList.contains(e)) {
				count++;
			}
		}
		return count;
	}
	
	public ArrayList<NodeInformation> getNeighborNodes(NodeInformation node) {
		ArrayList<NodeInformation> neighborNodes = new ArrayList<>();
		
		for (Edge e : edgesList) {
			if (e.getSourceNode().equals(node)) {
				neighborNodes.add(e.getDestationNode());
			} else if (e.getDestationNode().equals(node)) {
				neighborNodes.add(e.getSourceNode());
			}
		}
		
		return neighborNodes;
	}
}
