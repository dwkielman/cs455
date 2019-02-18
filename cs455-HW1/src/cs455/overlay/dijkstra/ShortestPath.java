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

	private final static boolean DEBUG = true;
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
		for (NodeInformation target : neighborNodes) {
			if (getShortestPath(target) > getShortestPath(node) + getDistance(node, target)) {
				distance.put(target, getShortestPath(node) + getDistance(node, target));
				predecessors.put(target, node);
				unsettledNodes.add(target);
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
	
	public ArrayList<NodeInformation> getPath(NodeInformation dest) {
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
		// bi-directional, but need to return the path TO the destination node
		Collections.reverse(nodePath);
		ArrayList<NodeInformation> returnPath = new ArrayList<>();
		for (NodeInformation ni : nodePath) {
			returnPath.add(ni);
		}
		return returnPath;
	}
	
	public static void main(String args[]) {
		if (DEBUG) {
			ArrayList<NodeInformation> nodeList = new ArrayList<>();
			nodeList.add(new NodeInformation("127.0.0.1", 9000));
	        nodeList.add(new NodeInformation("127.0.0.1", 9001));
	        nodeList.add(new NodeInformation("127.0.0.1", 9002));
	        nodeList.add(new NodeInformation("127.0.0.1", 9003));
	        nodeList.add(new NodeInformation("127.0.0.1", 9004));
	        nodeList.add(new NodeInformation("127.0.0.1", 9005));
	        nodeList.add(new NodeInformation("127.0.0.1", 9006));
	        nodeList.add(new NodeInformation("127.0.0.1", 9007));
	        nodeList.add(new NodeInformation("127.0.0.1", 9008));
	        nodeList.add(new NodeInformation("127.0.0.1", 9009));
	        
	        OverlayCreator myOC = new OverlayCreator(nodeList);
	        myOC.createOverlay(4);
	        
	        System.out.println("Testing Begin");
	        ArrayList<Edge> edgeListTesting = myOC.getEdgesList();
	        
	        
	        for (Edge e : edgeListTesting) {
	        	System.out.println("Source: " + e.getSourceNode());
	        	System.out.println("Destination: " + e.getDestationNode());
	        	System.out.println("Weight: " + e.getWeight());
	        	System.out.println();
	        	System.out.println("Source Num of C: " + e.getSourceNode().getNumberOfConnections());
	        }
	        
	        
	        ShortestPath sp = new ShortestPath(myOC);
	        sp.printConnections();
	        sp.execute(nodeList.get(0));
	        System.out.println("Starting at: " + nodeList.get(0).getNodePortNumber());
	        System.out.println("Going to: " + nodeList.get(5).getNodePortNumber());
	        System.out.println(sp.getPath(nodeList.get(5)));
	        //System.out.println(sp.getPath(nodeList.get(7)));
	        
	       // ArrayList<NodeInformation> neighborNodes = myOC.getNeighborNodes(nodeList.get(0));
	        
	        //for (NodeInformation ni : neighborNodes) {
	        	//System.out.println("Neighbor Node: " + ni.getNodePortNumber());
	        //}
	        
		}
	}

	private void printConnections( ) {
		for (NodeInformation n : this.nodesList) {
			System.out.println(n + "-" + this.overlay.getConnectionCount(n));
		}
	}
	
}
