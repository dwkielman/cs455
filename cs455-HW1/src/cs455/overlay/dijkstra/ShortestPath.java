package cs455.overlay.dijkstra;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import cs455.overlay.node.NodeInformation;
import cs455.overlay.util.OverlayCreator;

/**
 * Used to determine the shortest path through a weighted node graph using Dijkstra's Algorithm
 */

public class ShortestPath {

	private OverlayCreator overlay;
	private ArrayList<NodeInformation> nodesList;
	private ArrayList<Edge> edgeList;
	private Set<NodeInformation> settledNodes;
	private Set<NodeInformation> unsettledNodes;
	private Map<NodeInformation, NodeInformation> predecessors;
	private Map<NodeInformation, Integer> distance;
	
	public ShortestPath(OverlayCreator overlay) {
		this.overlay = overlay;
		this.nodesList = new ArrayList<NodeInformation>(overlay.getNodesList());
		this.edgeList = new ArrayList<Edge>(overlay.getEdgesList());
	}
	
	/**
	 * Calculate the shortest path
	 */
	public void execute(NodeInformation start) {
		settledNodes = new HashSet<>();
		unsettledNodes = new HashSet<>();
		distance = new HashMap<>();
		predecessors = new HashMap<>();
		distance.put(start, 0);
		unsettledNodes.add(start);
		
		while (unsettledNodes.size() >0) {
			NodeInformation node = getMin(unsettledNodes);
			settledNodes.add(node);
			unsettledNodes.remove(node);
			findMinDistance(node);
		}
	}
	
	private void findMinDistance(NodeInformation node) {
		List<NodeInformation> neighborNodes = getNeighborNodes(node);
		for (NodeInformation n : neighborNodes) {
			if (getShortestPath(n) > (getShortestPath(node) + getDistance(node, n))) {
				distance.put(n, getShortestPath(node) + getDistance(node, n));
				predecessors.put(n, node);
				unsettledNodes.add(n);
			}
		}
	}
	
	
	private int getDistance(NodeInformation startNode, NodeInformation destNode) {
		for (Edge e : this.edgeList) {
			if (e.getSourceNode().equals(startNode) && e.getDestationNode().equals(destNode)) {
				return e.getWeight();
			}
		} throw new RuntimeException();
	}
	
	private int getShortestPath(NodeInformation dest) {
		Integer pathNumber = distance.get(dest);
		if (pathNumber == null) {
			return Integer.MAX_VALUE;
		} else {
			return pathNumber;
		}
	}
	
	private NodeInformation getMin(Set<NodeInformation> nodes) {
		NodeInformation min = null;
		
		for (NodeInformation n : nodes) {
			if (min == null) {
				min = n;
			} else if ((getShortestPath(n)) < (getShortestPath(min))) {
				min = n;
			}
		}
		return min;
	}
	
	private List<NodeInformation> getNeighborNodes(NodeInformation node) {
		List<NodeInformation> neighborNodes = new ArrayList<>();
		for (Edge e : this.edgeList) {
			if (e.getSourceNode().equals(node) && (!this.settledNodes.contains(e.getDestationNode()))) {
				neighborNodes.add(e.getDestationNode());
			}
		}
		return neighborNodes;
	}
	
	public LinkedList<NodeInformation> getPath(NodeInformation dest) {
		LinkedList<NodeInformation> nodePath = new LinkedList<>();
		NodeInformation hop = dest;
		if (this.predecessors.get(hop) == null) {
			return null;
		}
		nodePath.add(hop);
		while (this.predecessors.get(hop) != null) {
			hop = predecessors.get(hop);
			nodePath.add(hop);
			
		}
		// bi-directional, but need to return the path TO the node
		Collections.reverse(nodePath);
		return nodePath;
	}
	
}
