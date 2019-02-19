package cs455.overlay.node;

import cs455.overlay.wireformats.Event;

/**
 * An interface for the Node's onEvent method
 */

public interface Node {
	
	public void onEvent(Event event);
	public void setLocalHostPortNumber(int portNumber);

}
