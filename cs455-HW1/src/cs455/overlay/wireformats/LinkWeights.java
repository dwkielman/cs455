package cs455.overlay.wireformats;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;

/**
 * A single message should be constructed with all link weights and sent to all registered messaging nodes.
 * A messaging node should process this message and store its information to generate routing paths for messages.
 * Message Type (int): LINK_WEIGHTS (6005)
 * Number of links (int): L
 * Linkinfo1 (ArrayList<String>)
 * Linkinfo2
 * ...
 * LinkinfoL
 */


public class LinkWeights implements Event {

	private final int type = Protocol.LINK_WEIGHTS;
	private int numberOfLinks;
	private ArrayList<String> linkInfoList;
	
	public LinkWeights(ArrayList<String> linksList) {
		this.numberOfLinks = linksList.size();
		this.linkInfoList = linksList;
	}
	
	/**
	 * byte[] construction is as follows:
	 * type
	 * numberOfLinks
	 * linkInfoList
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
		
		int numberOfLinks = din.readInt();
		this.numberOfLinks = numberOfLinks;
		
		this.linkInfoList = new ArrayList<>(this.numberOfLinks);
		
		for (int i=0; i < this.numberOfLinks; i++) {
			int linkLength = din.readInt();
			byte[] linkBytes = new byte[linkLength];
			din.readFully(linkBytes);
			this.linkInfoList.add(new String(linkBytes));
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
		
		dout.writeInt(numberOfLinks);
		
		for (String s : this.linkInfoList) {
			byte[] linkBytes = s.getBytes();
			int linkLength = linkBytes.length;
			dout.writeInt(linkLength);
			dout.write(linkBytes);
		}

		dout.flush();
		marshalledBytes = baOutputStream.toByteArray();
		baOutputStream.close();
		dout.close();
		
		return marshalledBytes;
	}

}
