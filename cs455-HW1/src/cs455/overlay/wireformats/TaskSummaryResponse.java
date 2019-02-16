package cs455.overlay.wireformats;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Upon receipt of the PULL_TRAFFIC_SUMMARY message from the registry, the node will create a response that includes summaries of the traffic that it has participated in. The summary will include information about messages that were sent and received.
 * Message Type: TRAFFIC_SUMMARY (6009)
 * Node IP address (String):
 * Node Port number (int):
 * Number of messages sent (int)
 * Summation of sent messages (long)
 * Number of messages received (int)
 * Summation of received messages (long)
 * Number of messages relayed (int)
 */

public class TaskSummaryResponse implements Event {

	private final int type = Protocol.TRAFFIC_SUMMARY;
	private String IPAddress;
	private int portNumber;
	private int numberOfMessagesSent;
	private long sumSentMessages;
	private int numberOfMessagesReceived;
	private long sumReceivedMessages;
	private int numberOfMessagesRelayed;
	
	
	
	public TaskSummaryResponse(String iPAddress, int portNumber, int numberOfMessagesSent, long sumSentMessages, int numberOfMessagesReceived, long sumReceivedMessages, int numberOfMessagesRelayed) {
		this.IPAddress = iPAddress;
		this.portNumber = portNumber;
		this.numberOfMessagesSent = numberOfMessagesSent;
		this.sumSentMessages = sumSentMessages;
		this.numberOfMessagesReceived = numberOfMessagesReceived;
		this.sumReceivedMessages = sumReceivedMessages;
		this.numberOfMessagesRelayed = numberOfMessagesRelayed;
	}

	/**
	 * byte[] construction is as follows:
	 * type
	 * IPAddress
	 * portNumber
	 * numberOfMessagesSent
	 * sumSentMessages
	 * numberOfMessagesReceived
	 * sumReceivedMessages
	 * numberOfMessagesRelayed
	 * @throws IOException 
	 */
	public TaskSummaryResponse(byte[] marshalledBytes) throws IOException {
		ByteArrayInputStream baInputStream = new ByteArrayInputStream(marshalledBytes);
		DataInputStream din = new DataInputStream(new BufferedInputStream(baInputStream));
		
		int type = din.readInt();
		
		if (type != Protocol.TRAFFIC_SUMMARY) {
			System.out.println("Invalid Message Type for TaskSummaryResponse");
			return;
		}
		
		int IPAddressLength = din.readInt();
		byte[] IPAddressBytes = new byte[IPAddressLength];
		din.readFully(IPAddressBytes);
		
		this.IPAddress = new String(IPAddressBytes);
		
		int portNumber = din.readInt();

		this.portNumber = portNumber;
		
		int numberOfMessagesSent = din.readInt();
		
		this.numberOfMessagesSent = numberOfMessagesSent;
		
		long sumSentMessages = din.readLong();
		
		this.sumSentMessages = sumSentMessages;
		
		int numberOfMessagesReceived = din.readInt();
		
		this.numberOfMessagesReceived = numberOfMessagesReceived;
		
		long sumReceivedMessages = din.readLong();
		
		this.sumReceivedMessages = sumReceivedMessages;
		
		int numberOfMessagesRelayed = din.readInt();
		
		this.numberOfMessagesRelayed = numberOfMessagesRelayed;
		
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
		
		byte[] IPAddressBytes = this.IPAddress.getBytes();
		int IPAddressLength = IPAddressBytes.length;
		dout.writeInt(IPAddressLength);
		dout.write(IPAddressBytes);
		
		dout.writeInt(this.portNumber);
		dout.writeInt(this.numberOfMessagesSent);
		dout.writeLong(this.sumSentMessages);
		dout.writeInt(this.numberOfMessagesReceived);
		dout.writeLong(this.sumReceivedMessages);
		dout.writeInt(this.numberOfMessagesRelayed);
		
		dout.flush();
		marshalledBytes = baOutputStream.toByteArray();
		baOutputStream.close();
		dout.close();
		
		return marshalledBytes;
	}

	public String getIPAddress() {
		return IPAddress;
	}

	public int getPortNumber() {
		return portNumber;
	}

	public int getNumberOfMessagesSent() {
		return numberOfMessagesSent;
	}

	public long getSumSentMessages() {
		return sumSentMessages;
	}

	public int getNumberOfMessagesReceived() {
		return numberOfMessagesReceived;
	}

	public long getSumReceivedMessages() {
		return sumReceivedMessages;
	}

	public int getNumberOfMessagesRelayed() {
		return numberOfMessagesRelayed;
	}

}
