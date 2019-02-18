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
 * Used for MessagingNodes to respond to connecting to other MessagingNodes
 * Message Type (int): NODE_CONNECTION_RESPONSE (6012)
 * Status Code (byte): SUCCESS or FAILURE
 * Additional Info (String):
 */

public class NodeConnectionResponse implements Event {
	
	private final int type = Protocol.NODE_CONNECTION_RESPONSE;
	private byte statusCode;
	private NodeInformation nodeResponder;
	
	public NodeConnectionResponse(byte statusCode, NodeInformation ni) {
		this.statusCode = statusCode;
		this.nodeResponder = ni;
	}
	
	/**
	 * byte[] construction is as follows:
	 * type
	 * statusCode
	 * NodeInformation for Requester
	 * @throws IOException 
	 */
	public NodeConnectionResponse(byte[] marshalledBytes) throws IOException {
		System.out.println("Begin NodeConnectionResponse Sending of Event");
		ByteArrayInputStream baInputStream = new ByteArrayInputStream(marshalledBytes);
		DataInputStream din = new DataInputStream(new BufferedInputStream(baInputStream));
		
		int type = din.readInt();
		
		if (type != Protocol.NODE_CONNECTION_RESPONSE) {
			System.out.println("Invalid Message Type for NodeConnectionResponse");
			return;
		}
		
		// statusCode
		this.statusCode = din.readByte();
		
		// NodeInformation
		int nodeInformationLength = din.readInt();
		byte[] nodeInformationBytes = new byte[nodeInformationLength];
		din.readFully(nodeInformationBytes);
		this.nodeResponder = new NodeInformation(nodeInformationBytes);
		
		baInputStream.close();
		din.close();
		System.out.println("End NodeConnectionResponse Sending of Event");
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
		
		// statusCode
		dout.writeByte(this.statusCode);
		
		// NodeInformation
		byte[] nodeInformationBytes = this.nodeResponder.getBytes();
		int nodeInformationLength = nodeInformationBytes.length;
		dout.writeInt(nodeInformationLength);
		dout.write(nodeInformationBytes);
		
		dout.flush();
		marshalledBytes = baOutputStream.toByteArray();
		baOutputStream.close();
		dout.close();
		
		return marshalledBytes;
	}

	public NodeInformation getNodeResponder() {
		return this.nodeResponder;
	}

	public byte getStatusCode() {
		return this.statusCode;
	}
}
