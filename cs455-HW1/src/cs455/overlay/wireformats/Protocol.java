package cs455.overlay.wireformats;

/**
 * Interactions between the registry and the messaging nodes, including prescribed wire-formats
 */

public class Protocol {
	public static final int REGISTER_REQUEST = 6000;
	public static final int REGISTER_RESPONSE = 6001;
	public static final int DEREGISTER_REQUEST = 6002;
	public static final int DEREGISTER_RESPONSE = 6003;
	public static final int MESSAGING_NODES_LIST = 6004;
	public static final int LINK_WEIGHTS = 6005;
	public static final int TASK_INITIATE = 6006;
	public static final int TASK_COMPLETE = 6007;
	public static final int PULL_TRAFFIC_SUMMARY = 6008;
	public static final int TRAFFIC_SUMMARY = 6009;
	public static final int MESSAGE = 6010;
}
