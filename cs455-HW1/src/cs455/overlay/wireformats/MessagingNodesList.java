package cs455.overlay.wireformats;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;

import cs455.overlay.node.NodeInformation;

/**
 * This Protocol represents a message sent from the Registry to the Messaging Nodes regarding the neighbors of a given MessagingNode of its neighboring MessagingNodes to connect to as constructed in the Overlay.
 * Message Type (int): MESSAGING_NODES_LIST (6004)
 * Number of peer messaging nodes (int): X
 * Messaging node1 Info (ArrayList<NodeInformation>)
 * Messaging node2 Info
 * ...
 * Messaging nodeX Info
 */

public class MessagingNodesList implements Event {

	private final int type = Protocol.MESSAGING_NODES_LIST;
	private int numberOfPeerMessagingNodes;
	private ArrayList<NodeInformation> messagingNodesInfoList;
	
	public MessagingNodesList(ArrayList<NodeInformation> nodesList) {
		this.numberOfPeerMessagingNodes = nodesList.size();
		this.messagingNodesInfoList = nodesList;
	}
	
	/**
	 * byte[] construction is as follows:
	 * type
	 * numberOfPeerMessagingNodes
	 * messagingNodesInfoList
	 * @throws IOException 
	 */
	public MessagingNodesList(byte[] marshalledBytes) throws IOException {
		ByteArrayInputStream baInputStream = new ByteArrayInputStream(marshalledBytes);
		DataInputStream din = new DataInputStream(new BufferedInputStream(baInputStream));
		
		int type = din.readInt();
		
		if (type != Protocol.MESSAGING_NODES_LIST) {
			System.out.println("Invalid Message Type for MessagingNodesList");
			return;
		}
		
		// numberOfPeerMessagingNodes
		int numberOfPeerNodes = din.readInt();
		this.numberOfPeerMessagingNodes = numberOfPeerNodes;
		
		// messagingNodesInfoList
		// declare as size of the number of messaging nodes that we are being passed
		this.messagingNodesInfoList = new ArrayList<>(this.numberOfPeerMessagingNodes);
		
		for (int i=0; i < this.numberOfPeerMessagingNodes; i++) {
			int mNILength = din.readInt();
			byte[] mNIBytes = new byte[mNILength];
			din.readFully(mNIBytes);
			this.messagingNodesInfoList.add(new NodeInformation(mNIBytes));
		}
		
		baInputStream.close();
		din.close();
	}

	@Override
	public int getType() {
		return this.type;
	}

	@Override
	public byte[] getBytes() throws IOException {
		byte[] marshalledBytes = null;
		ByteArrayOutputStream baOutputStream = new ByteArrayOutputStream();
		DataOutputStream dout = new DataOutputStream(new BufferedOutputStream(baOutputStream));
		dout.writeInt(this.type);
		
		// numberOfPeerMessagingNodes
		dout.writeInt(numberOfPeerMessagingNodes);
		
		// messagingNodesInfoList
		for (NodeInformation n : this.messagingNodesInfoList) {
			byte[] mNIBytes = n.getBytes();
			int mNILength = mNIBytes.length;
			dout.writeInt(mNILength);
			dout.write(mNIBytes);
		}

		dout.flush();
		marshalledBytes = baOutputStream.toByteArray();
		baOutputStream.close();
		dout.close();
		
		return marshalledBytes;
	}

	public int getNumberOfPeerMessagingNodes() {
		return this.numberOfPeerMessagingNodes;
	}

	public ArrayList<NodeInformation> getMessagingNodesInfoList() {
		return this.messagingNodesInfoList;
	}

}
