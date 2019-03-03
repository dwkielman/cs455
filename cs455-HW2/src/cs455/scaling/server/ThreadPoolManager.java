package cs455.scaling.server;

import java.util.LinkedList;

/**
 * The server relies on the thread pool to perform all tasks. The threads within the thread pool should be, created just once.
 * Care must be taken to ensure that you are not inadvertently creating a new thread every time a task needs to be performed
 *
 */
	// Each unit of work is a list of data packets with a maximum length of batch-size
	
public class ThreadPoolManager {

	private final LinkedList<Task> tasksToPerformList;
	private final int threadPoolSize;
	private final int batchSize;
	private final int batchTime;
	private final ThreadPool threadPool;
	
	
	public ThreadPoolManager(int threadPoolSize, int batchSize, int batchTime) {
		this.threadPoolSize = threadPoolSize;
		this.batchSize = batchSize;
		this.batchTime = batchTime;
		this.tasksToPerformList = new LinkedList<Task>();
		this.threadPool = new ThreadPool(threadPoolSize);
		this.threadPool.startWorkerThreads();
	}
	
	public void addTask(Task task) {
		synchronized(tasksToPerformList) {
			this.tasksToPerformList.add(task);
		}
	}
	
	public void assignTaskToWorkerThread() {
		synchronized(tasksToPerformList) {
			// Work units are added to the tail of the work queue and when the work unit at the top of the queue has either:
			// (1) reached a length of batch-size or
			// (2) batch-time has expired since the previous unit was removed, an available worker is assigned to the work unit.
			WorkerThread worker = threadPool.getSpareWorkerThread();
			Task newTask = tasksToPerformList.poll();
			if (newTask == null) {
				System.out.println("No Tasks available to assign yet, waiting for a Task to be added to the ThreadPoolManager.");
				try {
					tasksToPerformList.wait();
					newTask = tasksToPerformList.removeFirst();
				} catch (InterruptedException ie) {
					ie.printStackTrace();
				}
			}
			worker.assignTaskToWorkerThread(newTask);
		}
	}
	
	public boolean checkForSpareTasks() {
		synchronized(tasksToPerformList) {
			return this.tasksToPerformList.isEmpty();
		}
	}
	
}
