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
 * Used to determine the shortest path through a weighted node graph using Dijkstra's Algorithm.
 * A source for how this was constructed can be found at:
 * https://medium.com/@ssaurel/calculate-shortest-paths-in-java-by-implementing-dijkstras-algorithm-5c1db06b6541
 */

public class ShortestPath {

	private OverlayCreator overlay;
	private ArrayList<NodeInformation> nodesList;
	private ArrayList<Edge> edgeList;
	private Set<NodeInformation> visitedNodes;
	private Set<NodeInformation> unvisitedNodes;
	private Map<NodeInformation, NodeInformation> predecessors;
	private Map<NodeInformation, Integer> distance;
	
	// ShortestPath needs an Overlay of the Graph layout to determine a given ShortestPath
	public ShortestPath(OverlayCreator overlay) {
		this.overlay = overlay;
		this.nodesList = new ArrayList<NodeInformation>(overlay.getNodesList());
		this.edgeList = new ArrayList<Edge>(overlay.getEdgesList());
	}
	
	// Calculate the shortest path for a given start MessagingNode
	public void execute(NodeInformation startNode) {
		visitedNodes = new HashSet<>();
		unvisitedNodes = new HashSet<>();
		distance = new HashMap<>();
		predecessors = new HashMap<>();
		distance.put(startNode, 0);
		unvisitedNodes.add(startNode);
		
		// start cycling through all the nodes that need to be visited to calculate a given distance
		while (unvisitedNodes.size() > 0) {
			NodeInformation node = getMin(unvisitedNodes);
			visitedNodes.add(node);
			unvisitedNodes.remove(node);
			findMinDistance(node);
		}
	}
	
	private void findMinDistance(NodeInformation node) {
		List<NodeInformation> neighborNodes = getNeighborNodes(node);
		for (NodeInformation target : neighborNodes) {
			if (getShortestPath(target) > getShortestPath(node) + getDistance(node, target)) {
				distance.put(target, getShortestPath(node) + getDistance(node, target));
				predecessors.put(target, node);
				unvisitedNodes.add(target);
			}
		}
	}
	
	// get the weighted distance between two MessagingNodes by finding them on the list of Edges
	private int getDistance(NodeInformation startNode, NodeInformation destNode) {
		for (Edge e : this.edgeList) {
			if (e.getSourceNode().equals(startNode) && e.getDestationNode().equals(destNode)) {
				return e.getWeight();
			}
		} throw new RuntimeException();
	}
	
	// get the given weight to a passed destination MessagingNode
	private int getShortestPath(NodeInformation destinationNode) {
		Integer pathNumber = distance.get(destinationNode);
		if (pathNumber == null) {
			return Integer.MAX_VALUE;
		} else {
			return pathNumber;
		}
	}
	
	// find the shortest-weighted path to the next node on the list
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
	
	// returns a List of all of the neighbors that are connected to a given MessagingNode
	private List<NodeInformation> getNeighborNodes(NodeInformation node) {
		List<NodeInformation> neighborNodes = new ArrayList<>();
		for (Edge e : this.edgeList) {
			if (e.getSourceNode().equals(node) && (!this.visitedNodes.contains(e.getDestationNode()))) {
				neighborNodes.add(e.getDestationNode());
			}
		}
		return neighborNodes;
	}
	
	// returns an ArrayList of the path to a given passed MessagingNode after the source node has been executed
	public ArrayList<NodeInformation> getPath(NodeInformation destinationNode) {
		LinkedList<NodeInformation> nodePath = new LinkedList<>();
		NodeInformation hop = destinationNode;
		if (this.predecessors.get(hop) == null) {
			return null;
		}
		nodePath.add(hop);
		while (this.predecessors.get(hop) != null) {
			hop = predecessors.get(hop);
			nodePath.add(hop);
			
		}
		// Nodes send messages bi-directional, but this is the source node going TO the destination so need to reverse the path that was just constructed
		Collections.reverse(nodePath);
		ArrayList<NodeInformation> returnPath = new ArrayList<>();
		for (NodeInformation ni : nodePath) {
			returnPath.add(ni);
		}
		return returnPath;
	}
	
	// used for printing the shortestPath weight for messagingNodes, shell for calling the private method here
	public int getWeightBetweenNodes(NodeInformation startNode, NodeInformation destinationNode) {
		return getDistance(startNode, destinationNode);
	}
	
}
