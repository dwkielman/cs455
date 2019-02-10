package cs455.overlay.dijkstra;

import cs455.overlay.node.NodeInformation;

/**
 * Edge consists of two nodes and the weight between them.  Used in
 * Dijkstra's Algorithm, and also sent to MessagingNodes during the
 * startup process.  A list of edges can be used to reconstruct an Overlay.
 */

public class Edge {

	private NodeInformation sourceNode;
	private NodeInformation destationNode;
	private int weight;
	
	public Edge(NodeInformation sourceNode, NodeInformation destationNode, int weight) {
		this.sourceNode = sourceNode;
		this.destationNode = destationNode;
		this.weight = weight;
	}
	
	public NodeInformation getSourceNode() {
		return sourceNode;
	}
	
	public NodeInformation getDestationNode() {
		return destationNode;
	}
	
	public int getWeight() {
		return weight;
	}

	@Override
	public boolean equals(Object obj) {

		if (obj instanceof Edge ) {
			Edge otherEdge = (Edge) obj;
			return ((this.getSourceNode() == otherEdge.getSourceNode()) && (this.getDestationNode() == otherEdge.getDestationNode()));
		}
		return false;
	}
}
