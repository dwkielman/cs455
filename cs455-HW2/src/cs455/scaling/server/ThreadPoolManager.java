package cs455.scaling.server;

import java.util.LinkedList;

/**
 * The server relies on the thread pool to perform all tasks. The threads within the thread pool should be, created just once.
 * Care must be taken to ensure that you are not inadvertently creating a new threadevery time a task needs to be performed
 *
 */

public class ThreadPoolManager {

	private LinkedList<WorkerThread> workToPerformList;
	
	// The thread pool needs methods that allow:
	
	// (1) a spare worker thread to be retrieved and
	
	
	// (2) a worker thread to return itself to the pool after it has finished it task.
	
	
	
	// Each unit of work is a list of data packets with a maximum length of batch-size
	
	
	
	// Work units are added to the tail of the work queue and when the work unit at the top of the queue has either:
	// (1) reached a length of batch-size or
	// (2) batch-time has expired since the previous unit was removed, an available worker is assigned to the work unit.
	
	
	
	
	
	
	
	
	

	
}
