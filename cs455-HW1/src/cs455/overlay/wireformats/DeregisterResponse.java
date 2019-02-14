package cs455.overlay.wireformats;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * The Registry Node needs to respond when a messaging node exits and is trying to deregister itself
 * Message Type (int): DEREGISTER_REQUEST (6003)
 * Status Code (byte): SUCCESS or FAILURE
 * Additional Info (String):
 */

public class DeregisterResponse implements Event {

	private final int type = Protocol.DEREGISTER_RESPONSE;
	private byte statusCode;
	private String additionalInfo;
	
	public DeregisterResponse(byte statusCode, String additionalInfo) {
		this.statusCode = statusCode;
		this.additionalInfo = additionalInfo;
	}
	
	/**
	 * byte[] construction is as follows:
	 * type
	 * statusCode
	 * additionalInfo
	 * @throws IOException 
	 */
	public DeregisterResponse(byte[] marshalledBytes) throws IOException {
		ByteArrayInputStream baInputStream = new ByteArrayInputStream(marshalledBytes);
		DataInputStream din = new DataInputStream(new BufferedInputStream(baInputStream));
		
		int type = din.readInt();
		
		if (type != Protocol.DEREGISTER_RESPONSE) {
			System.out.println("Invalid Message Type for RegisterResponse");
			return;
		}
		
		this.statusCode = din.readByte();
		
		int additionalInfoLength = din.readInt();
		byte[] additionalInfoBytes = new byte[additionalInfoLength];
		din.readFully(additionalInfoBytes);
		
		this.additionalInfo = new String(additionalInfoBytes);

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
		
		dout.writeByte(this.statusCode);
		
		byte[] additionalInfoBytes = this.additionalInfo.getBytes();
		int additionalInfoLength = additionalInfoBytes.length;
		dout.writeInt(additionalInfoLength);
		dout.write(additionalInfoBytes);
		
		dout.flush();
		marshalledBytes = baOutputStream.toByteArray();
		baOutputStream.close();
		dout.close();
		
		return marshalledBytes;
	}

	public byte getStatusCode() {
		return statusCode;
	}

	public String getAdditionalInfo() {
		return additionalInfo;
	}

}
