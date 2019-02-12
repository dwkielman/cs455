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
 * Used for MessagingNodes requesting to connect to other MessagingNodes
 * Message Type (int): NODE_CONNECTION_REQUEST (6011)
 * IP address (String)
 * Port number (int)
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
		System.out.println("Begin NodeConnectionRequest Sending of Event");
		ByteArrayInputStream baInputStream = new ByteArrayInputStream(marshalledBytes);
		DataInputStream din = new DataInputStream(new BufferedInputStream(baInputStream));
		
		int type = din.readInt();
		
		if (type != Protocol.NODE_CONNECTION_REQUEST) {
			System.out.println("Invalid Message Type for NodeConnectionRequest");
			return;
		}
		
		int nodeInformationLength = din.readInt();
		byte[] nodeInformationBytes = new byte[nodeInformationLength];
		din.readFully(nodeInformationBytes);
		this.nodeRequester = new NodeInformation(nodeInformationBytes);
		
		baInputStream.close();
		din.close();
		System.out.println("End NodeConnectionRequest Sending of Event");
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
