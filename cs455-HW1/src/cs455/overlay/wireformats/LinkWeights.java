package cs455.overlay.wireformats;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;

import cs455.overlay.dijkstra.Edge;

/**
 * This Protocol is constructed with all link weights for a given receiving MessagingNode and sent to all registered MessagingNodes.
 * A messaging node should process this message and store its information to generate routing paths for messages.
 * Message Type (int): LINK_WEIGHTS (6005)
 * Number of links (int)
 * Linkinfo1 (ArrayList<Edge>)
 * Linkinfo2
 * ...
 * LinkinfoL
 */

public class LinkWeights implements Event {

	private final int type = Protocol.LINK_WEIGHTS;
	private int numberOfLinks;
	private ArrayList<Edge> linkWeightsEdges;
	
	public LinkWeights(ArrayList<Edge> edgesList) {
		this.numberOfLinks = edgesList.size();
		this.linkWeightsEdges = edgesList;
	}
	
	/**
	 * byte[] construction is as follows:
	 * type
	 * numberOfLinks
	 * linkWeightsEdges
	 * @throws IOException 
	 */
	public LinkWeights(byte[] marshalledBytes) throws IOException {
		ByteArrayInputStream baInputStream = new ByteArrayInputStream(marshalledBytes);
		DataInputStream din = new DataInputStream(new BufferedInputStream(baInputStream));
		
		int type = din.readInt();
		
		if (type != Protocol.LINK_WEIGHTS) {
			System.out.println("Invalid Message Type for LinkWeights");
			return;
		}
		
		// numberOfLinks
		int numberOfLinks = din.readInt();
		this.numberOfLinks = numberOfLinks;
		
		// linkWeightsEdges
		// declare as a size of the numberOfLinks that we have been passed
		this.linkWeightsEdges = new ArrayList<>(this.numberOfLinks);
		
		for (int i=0; i < this.numberOfLinks; i++) {
			int edgeLength = din.readInt();
			byte[] edgeBytes = new byte[edgeLength];
			din.readFully(edgeBytes);
			this.linkWeightsEdges.add(new Edge(edgeBytes));
		}
		
		baInputStream.close();
		din.close();
	}

	@Override
	public int getType() {
		return this.type;
	}
	
	public ArrayList<Edge> getlinkWeightsEdges() {
		return this.linkWeightsEdges;
	}

	@Override
	public byte[] getBytes() throws IOException {
		byte[] marshalledBytes = null;
		ByteArrayOutputStream baOutputStream = new ByteArrayOutputStream();
		DataOutputStream dout = new DataOutputStream(new BufferedOutputStream(baOutputStream));
		dout.writeInt(this.type);
		
		// numberOfLinks
		dout.writeInt(this.numberOfLinks);
		
		// linkWeightsEdges
		for (Edge e : this.linkWeightsEdges) {
			byte[] edgeBytes = e.getBytes();
			int edgeLength = edgeBytes.length;
			dout.writeInt(edgeLength);
			dout.write(edgeBytes);
		}

		dout.flush();
		marshalledBytes = baOutputStream.toByteArray();
		baOutputStream.close();
		dout.close();
		
		return marshalledBytes;
	}

}
