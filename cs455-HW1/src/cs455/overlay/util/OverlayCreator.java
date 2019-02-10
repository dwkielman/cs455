package cs455.overlay.util;

import java.util.ArrayList;
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
		
		// connect adjacent nodes in a circular fashion, ensures no partitions
		for (int i=0; i < this.nodesList.size(); i++) {
			this.edgesList.add(new Edge(this.nodesList.get(i % nodesList.size()), this.nodesList.get((i + 1) % nodesList.size()), random.nextInt(10) + 1));
			this.nodesList.get(i % nodesList.size()).addConnection();
			this.nodesList.get((i + 1) % nodesList.size()).addConnection();
		}
		
		// link the rest of the nodes to the number of connections they are required to have
		for (NodeInformation n : nodesList) {
			if (n.getNumberOfConnections() < (numberOfConnections + 1)) {
				// get another node to attempt a connection to
				for (int i=0; i < this.nodesList.size(); i++) {
					NodeInformation nodeToConnect = this.nodesList.get(i);
					
					// ensure that we are not attempting to connect to the same node and that it isn't at max connections yet
					if ((!nodeToConnect.equals(n)) && (nodeToConnect.getNumberOfConnections() < (numberOfConnections + 1))) {
						this.edgesList.add(new Edge(n, nodeToConnect, random.nextInt(10) + 1));
						n.addConnection();
						nodeToConnect.addConnection();
					}
				}
			}
		}
	}
	
	public ArrayList<NodeInformation> getNodesList() {
		return this.nodesList;
	}
	
	public ArrayList<Edge> getEdgesList() {
		return this.edgesList;
	}
	
}
