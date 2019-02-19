package cs455.overlay.wireformats;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import cs455.overlay.node.NodeInformation;

/**
 * MessagingNodes await instructions from the registry regarding the other messaging nodes that they must establish connections to.
 * This Protocol is used for when a MessagingNode send a request to connect to another MessagingNode
 * Message Type (int): NODE_CONNECTION_REQUEST (6011)
 * Requester (NodeInformation)
 */
public class NodeConnectionRequest implements Event {
	
	private final int type = Protocol.NODE_CONNECTION_REQUEST;
	private NodeInformation nodeRequester;
	
	public NodeConnectionRequest(NodeInformation ni) {
		this.nodeRequester = ni;
	}
	
	/**
	 * byte[] construction is as follows:
	 * type
	 * NodeInformation for Requester
	 * @throws IOException 
	 */
	public NodeConnectionRequest(byte[] marshalledBytes) throws IOException {
		ByteArrayInputStream baInputStream = new ByteArrayInputStream(marshalledBytes);
		DataInputStream din = new DataInputStream(new BufferedInputStream(baInputStream));
		
		int type = din.readInt();
		
		if (type != Protocol.NODE_CONNECTION_REQUEST) {
			System.out.println("Invalid Message Type for NodeConnectionRequest");
			return;
		}
		
		// NodeInformation
		int nodeInformationLength = din.readInt();
		byte[] nodeInformationBytes = new byte[nodeInformationLength];
		din.readFully(nodeInformationBytes);
		this.nodeRequester = new NodeInformation(nodeInformationBytes);
		
		baInputStream.close();
		din.close();
	}

	@Override
	public int getType() {
		return this.type;
	}

	public NodeInformation getNodeRequester() {
		return this.nodeRequester;
	}
	
	@Override
	public byte[] getBytes() throws IOException {
		byte[] marshalledBytes = null;
		ByteArrayOutputStream baOutputStream = new ByteArrayOutputStream();
		DataOutputStream dout = new DataOutputStream(new BufferedOutputStream(baOutputStream));
		dout.writeInt(this.type);
		
		// NodeInformation
		byte[] nodeInformationBytes = this.nodeRequester.getBytes();
		int nodeInformationLength = nodeInformationBytes.length;
		dout.writeInt(nodeInformationLength);
		dout.write(nodeInformationBytes);
		
		dout.flush();
		marshalledBytes = baOutputStream.toByteArray();
		baOutputStream.close();
		dout.close();
		
		return marshalledBytes;
	}
}
