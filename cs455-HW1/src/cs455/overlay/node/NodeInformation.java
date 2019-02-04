package cs455.overlay.node;

/**
 * NodeInformation is a way to capture relevant information for keeping track of information regarding MessagingNodes in the Registry and a way to override the Equals() method
 * as doing so in the MessagingNode class itself would wreak havoc as it would create countless Threads in attempting to compare Nodes that are already in the Registry. Also allows an easy way
 * to keep track of the information that will be needed to pull reports on.
 */

public class NodeInformation {

	private String nodeIPAddress;
	private int nodePortNumber;
	
	public NodeInformation(String nodeIPAddress, int nodePortNumber) {
		this.nodeIPAddress = nodeIPAddress;
		this.nodePortNumber = nodePortNumber;
	}
	
	@Override
	public boolean equals(Object o) {
		if (o instanceof NodeInformation) {
			NodeInformation otherNodeInformation = (NodeInformation) o;
			if (otherNodeInformation.getNodeIPAddress().equals(this.nodeIPAddress) && (otherNodeInformation.getNodePortNumber() == this.nodePortNumber)) {
				return true;
			}
		}
		return false;
	}
	
	public String getNodeIPAddress() {
		return this.nodeIPAddress;
	}
	
	public int getNodePortNumber() {
		return this.nodePortNumber;
	}
	
	@Override
	public String toString() {
		return (this.nodeIPAddress + ":" + this.nodePortNumber);
	}
	
	@Override
	public int hashCode() {
		return this.toString().hashCode();
	}
}
