package cs455.overlay.node;

import cs455.overlay.wireformats.Event;

/**
 * The registry maintains information about the registered messaging nodes in a registry; you can use any
 * data structure for managing this registry but make sure that your choice can support all the operations
 * that you will need.
 */

public class Registry implements Node {

	@Override
	public void onEvent(Event event) {
		// TODO Auto-generated method stub
		
	}
	
	// Allows messaging nodes to register themselves. This is performed when a messaging node starts up for the first time.

	// Allows messaging nodes to deregister themselves. This is performed when a messaging node leaves the overlay.
	
	/**
	 * 	Enables the construction of the overlay by orchestrating connections that a messaging node initiates with other messaging nodes in the system. 
	 *  Based on its knowledge of the messagingnodes (through function A) the registry informs messaging nodes about the other messaging nodes that they should connect to.
	 */
	
	// Assign and publish weights to the links connecting any two messaging nodes in the overlay. The weights these links take will range from 1-10.
	
	// java cs455.overlay.node.Registry portnum
	
}
