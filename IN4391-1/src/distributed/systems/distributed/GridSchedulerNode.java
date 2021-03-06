package distributed.systems.distributed;

import java.rmi.RemoteException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import distributed.systems.core.IMessageReceivedHandler;
import distributed.systems.core.Message;
import distributed.systems.core.Socket;
import distributed.systems.core.SynchronizedSocket;
import distributed.systems.example.LocalSocket;
import distributed.systems.gridscheduler.model.ControlMessage;
import distributed.systems.gridscheduler.model.ControlMessageType;
import distributed.systems.gridscheduler.model.Job;

public class GridSchedulerNode extends Client implements Runnable {
	private static final long serialVersionUID = 1L;

	
	private ConcurrentLinkedQueue<Job> jobQueue;//job queue
	private final String url;//local url
	private Socket socket;//communications socket
	private ConcurrentHashMap<String, Integer> resourceManagerLoad;//a hashmap linking each resource manager to an estimated load
	private long pollSleep = 1000;// polling frequency, 1hz
	private Thread pollingThread;// polling thread
	private boolean running;

	protected GridSchedulerNode(int clientID) throws RemoteException {
		super(clientID);
		super.init(Properties.PORT);
		// init members
		this.url = super.createURL(clientID);
		this.resourceManagerLoad = new ConcurrentHashMap<String, Integer>();
		this.jobQueue = new ConcurrentLinkedQueue<Job>();
		
		// create a messaging socket
		Socket lSocket = new LocalSocket();//TODO change this into a RMI version??
		socket = new SynchronizedSocket(lSocket);
		socket.addMessageReceivedHandler((IMessageReceivedHandler) this);
		
		// register the socket under the name of the gridscheduler.
		// In this way, messages can be sent between components by name.
		socket.register(url);

		// start the polling thread
		running = true;
		pollingThread = new Thread(this);
		pollingThread.start();
	}
	
	@Override
	public void messageReceived(Message msg) {
		/**
		 * same as GridScheduler
		 */
		if(msg instanceof ControlMessage){
			ControlMessage controlMessage = (ControlMessage)msg;
			
			// resource manager wants to join this grid scheduler 
			// when a new RM is added, its load is set to Integer.MAX_VALUE to make sure
			// no jobs are scheduled to it until we know the actual load
			if (controlMessage.getType() == ControlMessageType.ResourceManagerJoin){
				resourceManagerLoad.put(controlMessage.getUrl(), Integer.MAX_VALUE);
			}
			// resource manager wants to offload a job to us 
			if (controlMessage.getType() == ControlMessageType.AddJob){
				jobQueue.add(controlMessage.getJob());
			}
			// resource manager wants to offload a job to us 
			if (controlMessage.getType() == ControlMessageType.ReplyLoad){
				resourceManagerLoad.put(controlMessage.getUrl(),controlMessage.getILoad());
			}
		}
		/**
		 * 
		 */
		else{ //"normal message"
			//TODO 
		}
	}
	
	// finds the least loaded resource manager and returns its url
		private String getLeastLoadedRM() {
			String ret = null; 
			int minLoad = Integer.MAX_VALUE;
			
			// loop over all resource managers, and pick the one with the lowest load
			for (String key : resourceManagerLoad.keySet())
			{
				if (resourceManagerLoad.get(key) <= minLoad)
				{
					ret = key;
					minLoad = resourceManagerLoad.get(key);
				}
			}
			return ret;		
		}
		
		/**
		 * Polling thread runner. This thread polls each resource manager in turn to get its load,
		 * then offloads any job in the waiting queue to that resource manager
		 */
		public void run() {
			super.init(Properties.PORT);//initialize the RMI registry
			while (running) {
				// send a message to each resource manager, requesting its load
				for (String rmUrl : resourceManagerLoad.keySet())
				{
					/*
					 * TODO This part needs to be changed from localsocket to using RMI
					 */
					//ControlMessage cMessage = new ControlMessage(ControlMessageType.RequestLoad);
					//cMessage.setUrl(this.getUrl());
					//socket.sendMessage(cMessage, "localsocket://" + rmUrl);
				}
				
				// schedule waiting messages to the different clusters
				for (Job job : jobQueue)
				{
					String leastLoadedRM =  getLeastLoadedRM();
					
					if (leastLoadedRM!=null) {
					
						ControlMessage cMessage = new ControlMessage(ControlMessageType.AddJob);
						cMessage.setJob(job);
						socket.sendMessage(cMessage, "localsocket://" + leastLoadedRM);
						
						jobQueue.remove(job);
						
						// increase the estimated load of that RM by 1 (because we just added a job)
						int load = resourceManagerLoad.get(leastLoadedRM);
						resourceManagerLoad.put(leastLoadedRM, load+1);
					}
				}
				
				// sleep
				try
				{
					Thread.sleep(pollSleep);
				} catch (InterruptedException ex) {
					assert(false) : "Grid scheduler runtread was interrupted";
				}
			}
		}
}
