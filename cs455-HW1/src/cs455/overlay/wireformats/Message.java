package cs455.overlay.wireformats;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;

import cs455.overlay.dijkstra.RoutingCache;
import cs455.overlay.node.NodeInformation;

/**
 * Data can be fed into the network from any messaging node within the network. Packets are sent from a source to a sink.
 * A message includes a payload which is a random integer.
 * Message Type (int): MESSAGE (6010)
 * Source IP address and Port (NodeInformation)
 * Destination IP address and Port (NodeInformation)
 * Payload (int)
 * Route Path (ArrayList<NodeInformation>)
 */

public class Message implements Event {

	private final int type = Protocol.MESSAGE;
	private NodeInformation sourceNode;
	private NodeInformation destinationNode;
	private int payload;
	private ArrayList<NodeInformation> routePath;
	
	public Message(NodeInformation source, NodeInformation dest, int payload, ArrayList<NodeInformation> route) {
		this.sourceNode = source;
		this.destinationNode = dest;
		this.payload = payload;
		this.routePath = route;
	}

	/**
	 * byte[] construction is as follows:
	 * type
	 * source NodeInformation
	 * destiantion NodeInformation
	 * payload
	 * ArrayList<NodeInformation> routePath
	 * @throws IOException 
	 */
	public Message(byte[] marshalledBytes) throws IOException {
		ByteArrayInputStream baInputStream = new ByteArrayInputStream(marshalledBytes);
		DataInputStream din = new DataInputStream(new BufferedInputStream(baInputStream));
		
		int type = din.readInt();
		
		if (type != Protocol.MESSAGE) {
			System.out.println("Invalid Message Type for RegisterRequest");
			return;
		}
		
		int sourceNILength = din.readInt();
		byte[] sourceNIBytes = new byte[sourceNILength];
		din.readFully(sourceNIBytes);
		
		this.sourceNode = new NodeInformation(sourceNIBytes);
		
		int destNILength = din.readInt();
		byte[] destNIBytes = new byte[destNILength];
		din.readFully(destNIBytes);
		
		this.destinationNode = new NodeInformation(destNIBytes);
		
		int payload = din.readInt();

		this.payload = payload;
		
		int routePathLength = din.readInt();
		
		this.routePath = new ArrayList<>(routePathLength);
		
		for (int i = 0; i < routePathLength; i++) {
			int routeNILength = din.readInt();
			byte[] routeNIBytes = new byte[routeNILength];
			din.readFully(routeNIBytes);
			this.routePath.add(new NodeInformation(routeNIBytes));
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
		
		byte[] sourceNIBytes = this.sourceNode.getBytes();
		int sourceNILength = sourceNIBytes.length;
		dout.writeInt(sourceNILength);
		dout.write(sourceNIBytes);
		
		byte[] destNIBytes = this.destinationNode.getBytes();
		int destNILength = destNIBytes.length;
		dout.writeInt(destNILength);
		dout.write(destNIBytes);
		
		dout.writeInt(this.payload);
		
		for (NodeInformation ni : routePath) {
			byte[] routeNIBytes = ni.getBytes();
			int routeNILength = routeNIBytes.length;
			dout.writeInt(routeNILength);
			dout.write(routeNIBytes);
		}
		
		dout.flush();
		marshalledBytes = baOutputStream.toByteArray();
		baOutputStream.close();
		dout.close();
		
		return marshalledBytes;
	}

	public NodeInformation getSourceNode() {
		return sourceNode;
	}

	public NodeInformation getDestinationNode() {
		return destinationNode;
	}

	public int getPayload() {
		return payload;
	}

	public ArrayList<NodeInformation> getRoutePath() {
		return routePath;
	}
}
