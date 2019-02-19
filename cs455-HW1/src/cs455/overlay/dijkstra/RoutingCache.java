package cs455.overlay.dijkstra;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import cs455.overlay.node.NodeInformation;
import cs455.overlay.util.OverlayCreator;

/**
 * Once the ShortestPath algorithm has been created, the Routing Cache is used to cache the shortest routing path from one MessagingNode to another.
 */

public class RoutingCache {

	private Map<NodeInformation, ArrayList<NodeInformation>> routes;
	
	public RoutingCache() {
		routes = new HashMap<>();
	}
	
	public void addPath(NodeInformation node, ArrayList<NodeInformation> path) {
		if (!routes.containsKey(node)) {
			routes.put(node, path);
		}
	}
	
	public boolean isRoute(NodeInformation node) {
		return this.routes.containsKey(node);
	}
	
	public int getNumberOfRoutes() {
		return this.routes.size();
	}
	
	public ArrayList<NodeInformation> getPathFromRoutingCache(NodeInformation dest) {
		if (routes.containsKey(dest)) {
			return routes.get(dest);
		} else {
			return null;
		}
	}
}
