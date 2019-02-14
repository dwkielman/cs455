package cs455.overlay.dijkstra;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import cs455.overlay.node.NodeInformation;

/**
 * Edge consists of two nodes and the weight between them. Used in
 * Dijkstra's Algorithm, and also sent to MessagingNodes during the
 * startup process. A list of edges can be used to reconstruct an Overlay.
 */

public class Edge {

	private NodeInformation sourceNode;
	private NodeInformation destationNode;
	private int weight;
	
	public Edge(NodeInformation sourceNode, NodeInformation destationNode, int weight) {
		this.sourceNode = sourceNode;
		this.destationNode = destationNode;
		this.weight = weight;
	}
	
	public Edge(byte[] marshalledBytes) throws IOException {
		ByteArrayInputStream baInputStream = new ByteArrayInputStream(marshalledBytes);
		DataInputStream din = new DataInputStream(new BufferedInputStream(baInputStream));
		
		// sourceNode
		int soucreNodeInfoLength = din.readInt();
		byte[] sourceNodeInfoBytes = new byte[soucreNodeInfoLength];
		din.readFully(sourceNodeInfoBytes);
		this.sourceNode = new NodeInformation(sourceNodeInfoBytes);
		
		// destinationNode
		int destNodeInfoLength = din.readInt();
		byte[] destNodeInfoBytes = new byte[destNodeInfoLength];
		din.readFully(destNodeInfoBytes);
		this.sourceNode = new NodeInformation(destNodeInfoBytes);
		
		//weight
		int edgeWeight = din.readInt();
		this.weight = edgeWeight;
		
		baInputStream.close();
		din.close();
	}
	
	public byte[] getBytes() throws IOException {
		byte[] marshalledBytes = null;
		ByteArrayOutputStream baOutputStream = new ByteArrayOutputStream();
		DataOutputStream dout = new DataOutputStream(new BufferedOutputStream(baOutputStream));
		
		// sourceNode
		byte[] sourceNodeInfoBytes = sourceNode.getBytes();
		int sourceNodeInfoLength = sourceNodeInfoBytes.length;
		dout.writeInt(sourceNodeInfoLength);
		dout.write(sourceNodeInfoBytes);
		
		// destinationeNode
		byte[] destinationNodeInfoBytes = sourceNode.getBytes();
		int destinationNodeInfoLength = destinationNodeInfoBytes.length;
		dout.writeInt(destinationNodeInfoLength);
		dout.write(destinationNodeInfoBytes);
		
		// weight
		dout.writeInt(this.weight);
		
		dout.flush();
		marshalledBytes = baOutputStream.toByteArray();
		baOutputStream.close();
		dout.close();
		
		return marshalledBytes;
	}
	
	public NodeInformation getSourceNode() {
		return sourceNode;
	}
	
	public NodeInformation getDestationNode() {
		return destationNode;
	}
	
	public int getWeight() {
		return weight;
	}

	@Override
	public boolean equals(Object obj) {

		if (obj instanceof Edge ) {
			Edge otherEdge = (Edge) obj;
			return ((this.getSourceNode() == otherEdge.getSourceNode()) && (this.getDestationNode() == otherEdge.getDestationNode()));
		}
		return false;
	}
}
