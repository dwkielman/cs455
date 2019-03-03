package cs455.scaling.server;

import java.util.LinkedList;

	//The thread pool needs methods that allow:
	// (1) a spare worker thread to be retrieved and
	// (2) a worker thread to return itself to the pool after it has finished it task.

public class ThreadPool {

	private final LinkedList<WorkerThread> threadPool;
	
	public ThreadPool(int threadPoolSize) {
		this.threadPool = new LinkedList<WorkerThread>();
		
		for (int i = 0; i < threadPoolSize; i++) {
			WorkerThread workerThread = new WorkerThread(this);
			this.threadPool.add(workerThread);
		}
	}
	
	public void startWorkerThreads() {
		for (WorkerThread workerThread : threadPool) {
			new Thread(workerThread).start();
		}
	}
	
	public void addWorkerThreadBackToPool(WorkerThread workerThread) {
		synchronized(threadPool) {
			threadPool.add(workerThread);
			threadPool.notify();
		}
	}
	
	public WorkerThread getSpareWorkerThread() {
		WorkerThread spareWorkerThread = null;
		
		synchronized(threadPool) {
			spareWorkerThread = threadPool.poll();
			if (spareWorkerThread == null) {
				System.out.println("No worker threads available. Wait for one to be added back to the pool.");
				try {
					threadPool.wait();
					spareWorkerThread = threadPool.removeFirst();
				} catch (InterruptedException ie) {
					ie.printStackTrace();
				}
			}
		}
		
		return spareWorkerThread;
	}
	
}
