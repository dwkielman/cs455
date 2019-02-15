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
 * Once a node has completed its task of sending a certain number of messages in rounds, it informs the registry of its task completion.
 * Message Type: TASK_COMPLETE (6007)
 * Destination IP address and Port (NodeInformation)
 */

public class TaskComplete implements Event {

	private final int type = Protocol.TASK_COMPLETE;
	private NodeInformation nodeInformation;
	
	public TaskComplete(NodeInformation nodeInfo) {
		this.nodeInformation = nodeInfo;
	}
	
	/**
	 * byte[] construction is as follows:
	 * type
	 * IPAddress
	 * portNumber
	 * @throws IOException 
	 */
	public TaskComplete(byte[] marshalledBytes) throws IOException {
		ByteArrayInputStream baInputStream = new ByteArrayInputStream(marshalledBytes);
		DataInputStream din = new DataInputStream(new BufferedInputStream(baInputStream));
		
		int type = din.readInt();
		
		if (type != Protocol.TASK_COMPLETE) {
			System.out.println("Invalid Message Type for TaskComplete");
			return;
		}
		
		int nodeInfoLength = din.readInt();
		byte[] nodeInfoBytes = new byte[nodeInfoLength];
		din.readFully(nodeInfoBytes);
		
		this.nodeInformation = new NodeInformation(nodeInfoBytes);
		
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
		
		byte[] nodeInfoBytes = this.nodeInformation.getBytes();
		int nodeInfoLength = nodeInfoBytes.length;
		dout.writeInt(nodeInfoLength);
		dout.write(nodeInfoBytes);
		
		dout.flush();
		marshalledBytes = baOutputStream.toByteArray();
		baOutputStream.close();
		dout.close();
		
		return marshalledBytes;
	}

}
