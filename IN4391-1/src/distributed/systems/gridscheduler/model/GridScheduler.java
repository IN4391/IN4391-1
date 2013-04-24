package distributed.systems.gridscheduler.model;

import java.net.MalformedURLException;
import java.rmi.AlreadyBoundException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import distributed.systems.core.IMessageReceivedHandler;
import distributed.systems.core.Message;
import distributed.systems.core.Socket;
import distributed.systems.core.SynchronizedSocket;
import distributed.systems.example.LocalSocket;

/**
 * 
 * The GridScheduler class represents a single-server implementation of the grid scheduler in the
 * virtual grid system.
 * 
 * @author Niels Brouwers
 *
 */
public class GridScheduler extends UnicastRemoteObject implements IMessageReceivedHandler, Runnable {
	
	// job queue
	private ConcurrentLinkedQueue<Job> jobQueue;
	
	// local url
	private final String url;

	// communications socket
	private SynchronizedSocket socket;
	
	// a hashmap linking each resource manager to an estimated load
	private ConcurrentHashMap<String, Integer> resourceManagerLoad;
	
	// other gridschedulers
	private ArrayList<String> gridschedulers;
	
	private String upstream_neighbour;
	private String downstream_neighbour;
	
	private long jobId;
	
	// random number generator
	private Random generator; 
	
	// timer
	private boolean timer;

	// polling frequency, 1hz
	private long pollSleep = 1000;
	
	// polling thread
	private Thread pollingThread;
	private boolean running;
	
	/**
	 * Constructs a new GridScheduler object at a given url.
	 * <p>
	 * <DL>
	 * <DT><B>Preconditions:</B>
	 * <DD>parameter <CODE>url</CODE> cannot be null
	 * </DL>
	 * @param url the gridscheduler's url to register at
	 * @throws RemoteException 
	 */
	public GridScheduler(String url, String downstream, String upstream) throws RemoteException {
		// preconditions
		assert(url != null) : "parameter 'url' cannot be null";
		
		// init members
		this.url = url;
		this.upstream_neighbour = upstream;
		this.downstream_neighbour = downstream;
		this.resourceManagerLoad = new ConcurrentHashMap<String, Integer>();
		this.jobQueue = new ConcurrentLinkedQueue<Job>();
		this.gridschedulers = new ArrayList<String>();
		this.jobId = 0;
		System.out.println("Like wtf is going on?");
		// create a messaging socket
		/*LocalSocket lSocket = new LocalSocket();
		socket = new SynchronizedSocket(lSocket);
		socket.addMessageReceivedHandler(this);*/
		
		long seed = System.currentTimeMillis();
		generator = new Random(seed);
		
		timer = false;
		// register the socket under the name of the gridscheduler.
		// In this way, messages can be sent between components by name.
		//socket.register(url);
		
		// Bind the node to the RMI registry.
		try {
			java.rmi.Naming.bind(url, this);
		} catch (MalformedURLException | AlreadyBoundException e) {
			e.printStackTrace();
		}
		
		final String name = url;
		
		// Let the node unregister from RMI registry on shut down.
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				System.out.println("Shutting down " + name + ".");
				try {
					java.rmi.Naming.unbind(name);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		});
	
		// start the polling thread
		running = true;
		pollingThread = new Thread(this);
		pollingThread.start();
	}
	
	public void addScheduler(String gs)
	{
		this.gridschedulers.add(gs);
	}
	
	/**
	 * The gridscheduler's name also doubles as its URL in the local messaging system.
	 * It is passed to the constructor and cannot be changed afterwards.
	 * @return the name of the gridscheduler
	 */
	public String getUrl() {
		return url;
	}

	/**
	 * Gets the number of jobs that are waiting for completion.
	 * @return
	 */
	public int getWaitingJobs() {
		int ret = 0;
		ret = jobQueue.size();
		return ret;
	}
	
	public void sendMessage(Message m, String url)
	{
		try {
			IMessageReceivedHandler stub = (IMessageReceivedHandler) java.rmi.Naming.lookup(url);
			stub.onMessageReceived(m);
		} catch (MalformedURLException | RemoteException
				| NotBoundException e) {
			e.printStackTrace();
		}
	}
	
	public void nodeLeft(String gs)
	{
		if (gs.equals(downstream_neighbour) || gs.equals(upstream_neighbour))
		{
			if (gs.equals(downstream_neighbour))
			{
				ControlMessage cMessage = new ControlMessage(ControlMessageType.NeighborRequest);
				cMessage.setUrl(this.getUrl());
				cMessage.setSLoad(gs);
				sendMessage(cMessage, upstream_neighbour);
				//socket.sendMessage(cMessage, "localsocket://" + upstream_neighbour);
			}
			else
			{
				ControlMessage cMessage = new ControlMessage(ControlMessageType.NeighborRequest);
				cMessage.setUrl(this.getUrl());
				cMessage.setSLoad(gs);
				sendMessage(cMessage, downstream_neighbour);
				//socket.sendMessage(cMessage, "localsocket://" + downstream_neighbour);
			}
		}
		else
		{
			ControlMessage cMessage = new ControlMessage(ControlMessageType.CrashedGS);
			cMessage.setUrl(this.getUrl());
			sendMessage(cMessage, upstream_neighbour);
			//socket.sendMessage(cMessage, "localsocket://" + upstream_neighbour);
			
			ControlMessage cMessage2 = new ControlMessage(ControlMessageType.CrashedGS);
			cMessage2.setUrl(this.getUrl());
			sendMessage(cMessage2, downstream_neighbour);
			//socket.sendMessage(cMessage2, "localsocket://" + downstream_neighbour);
		}
		
	}

	/**
	 * Receives a message from another component.
	 * <p>
	 * <DL>
	 * <DT><B>Preconditions:</B>
	 * <DD>parameter <CODE>message</CODE> should be of type ControlMessage 
	 * <DD>parameter <CODE>message</CODE> should not be null
	 * </DL> 
	 * @param message a message
	 */
	public void onMessageReceived(Message message) {
		// preconditions
		assert(message instanceof ControlMessage) : "parameter 'message' should be of type ControlMessage";
		assert(message != null) : "parameter 'message' cannot be null";
		
		ControlMessage controlMessage = (ControlMessage)message;
		
		// resource manager wants to join this grid scheduler 
		// when a new RM is added, its load is set to Integer.MAX_VALUE to make sure
		// no jobs are scheduled to it until we know the actual load
		if (controlMessage.getType() == ControlMessageType.ResourceManagerJoin)
			resourceManagerLoad.put(controlMessage.getUrl(), Integer.MAX_VALUE);
		
		if (controlMessage.getType() == ControlMessageType.ForwardRM)
		{	
			String origin = (String)controlMessage.getLoad();
			
			if (!origin.equals(this.getUrl()))
			{
				resourceManagerLoad.put(controlMessage.getUrl(), Integer.MAX_VALUE);
				ControlMessage cMessage = new ControlMessage(ControlMessageType.ReplyGS);
				cMessage.setUrl(this.getUrl());
				sendMessage(cMessage, controlMessage.getSLoad());
				//socket.sendMessage(cMessage, "localsocket://" + controlMessage.getSLoad());
				
				ControlMessage fMessage = new ControlMessage(ControlMessageType.ForwardRM);
				fMessage.setUrl(this.getUrl());
				fMessage.setLoad(origin);
				fMessage.setSLoad(controlMessage.getSLoad());
				sendMessage(fMessage, downstream_neighbour);
				//socket.sendMessage(fMessage, "localsocket://" + downstream_neighbour);
			}
		}
		
		if (controlMessage.getType() == ControlMessageType.RequestGSes)
		{
			resourceManagerLoad.put(controlMessage.getUrl(), Integer.MAX_VALUE);
			
			ControlMessage fMessage = new ControlMessage(ControlMessageType.ForwardRM);
			fMessage.setUrl(this.getUrl());
			fMessage.setLoad(this.getUrl());
			fMessage.setSLoad(controlMessage.getUrl());
			sendMessage(fMessage, downstream_neighbour);
			//socket.sendMessage(fMessage, "localsocket://" + downstream_neighbour);
		}
		
		// resource manager wants to offload a job to us 
		if (controlMessage.getType() == ControlMessageType.AddJob)
		{
			jobQueue.add(controlMessage.getJob());
			ControlMessage cMessage = new ControlMessage(ControlMessageType.Roger);
			cMessage.setUrl(this.getUrl());
			sendMessage(cMessage, controlMessage.getUrl());
			//socket.sendMessage(cMessage, "localsocket://" + controlMessage.getUrl());
		}
			
		// resource manager told us his load 
		if (controlMessage.getType() == ControlMessageType.ReplyLoad)
			resourceManagerLoad.put(controlMessage.getUrl(),controlMessage.getILoad());
		
		if (controlMessage.getType() == ControlMessageType.UpdateView)
		{
			String origin = controlMessage.getSLoad();
			if (!origin.equals(this.getUrl()))
			{
				resourceManagerLoad.put(controlMessage.getUrl(),controlMessage.getILoad());
				ControlMessage uMessage = new ControlMessage(ControlMessageType.UpdateView);
				uMessage.setUrl(controlMessage.getUrl());
				uMessage.setILoad(controlMessage.getILoad());
				uMessage.setSLoad(controlMessage.getSLoad());
				sendMessage(uMessage, downstream_neighbour);
				//socket.sendMessage(uMessage, "localsocket://" + downstream_neighbour);
			}
		}
		
		if (controlMessage.getType() == ControlMessageType.BulkJob)
		{
			System.out.println(url + "|: " + jobQueue.size());
			ArrayList<Job> temp = controlMessage.getJobs();
			for (Job j : temp)
				jobQueue.add(j);
			
			System.out.println(url + ": " + jobQueue.size());
		}
		
		// resource manager wants to offload a job to us 
		if (controlMessage.getType() == ControlMessageType.Retry)
		{
			jobQueue.add(controlMessage.getJob());
			ControlMessage cMessage = new ControlMessage(ControlMessageType.Roger);
			cMessage.setUrl(this.getUrl());
			sendMessage(cMessage, controlMessage.getUrl());
			//socket.sendMessage(cMessage, "localsocket://" + controlMessage.getUrl());
			nodeLeft(controlMessage.getSLoad());
		}
		
		// node crash recovery 
		if (controlMessage.getType() == ControlMessageType.CrashedGS)
		{
			if (controlMessage.getSLoad().equals(downstream_neighbour) || controlMessage.getSLoad().equals(upstream_neighbour))
			{
				if (controlMessage.getSLoad().equals(downstream_neighbour))
				{
					ControlMessage cMessage = new ControlMessage(ControlMessageType.NeighborRequest);
					cMessage.setUrl(this.getUrl());
					cMessage.setSLoad(controlMessage.getSLoad());
					sendMessage(cMessage, upstream_neighbour);
					//socket.sendMessage(cMessage, "localsocket://" + upstream_neighbour);
				}
				else
				{
					ControlMessage cMessage = new ControlMessage(ControlMessageType.NeighborRequest);
					cMessage.setUrl(this.getUrl());
					cMessage.setSLoad(controlMessage.getSLoad());
					sendMessage(cMessage, downstream_neighbour);
					//socket.sendMessage(cMessage, "localsocket://" + downstream_neighbour);
				}
			}
			else
			{
				if (controlMessage.getUrl().equals(downstream_neighbour))
				{
					ControlMessage cMessage = new ControlMessage(ControlMessageType.CrashedGS);
					cMessage.setUrl(this.getUrl());
					cMessage.setSLoad(controlMessage.getSLoad());
					sendMessage(cMessage, upstream_neighbour);
					//socket.sendMessage(cMessage, "localsocket://" + upstream_neighbour);
				}
				else
				{
					ControlMessage cMessage = new ControlMessage(ControlMessageType.CrashedGS);
					cMessage.setUrl(this.getUrl());
					cMessage.setSLoad(controlMessage.getSLoad());
					sendMessage(cMessage, downstream_neighbour);
					//socket.sendMessage(cMessage, "localsocket://" + downstream_neighbour);
				}
			}
		}
		
		if (controlMessage.getType() == ControlMessageType.ShutDown)
		{
			stopPollThread();
		}
		
		// node crash recovery 
		if (controlMessage.getType() == ControlMessageType.NeighborRequest)
		{
			if (controlMessage.getSLoad().equals(downstream_neighbour) || controlMessage.getSLoad().equals(upstream_neighbour))
			{
				if (controlMessage.getSLoad().equals(downstream_neighbour))
				{
					downstream_neighbour = controlMessage.getUrl();
				}
				else
				{
					upstream_neighbour = controlMessage.getUrl();
				}
			}
			else	
			{
				if (controlMessage.getUrl().equals(downstream_neighbour))
				{
					ControlMessage cMessage = new ControlMessage(ControlMessageType.NeighborRequest);
					cMessage.setUrl(this.getUrl());
					cMessage.setSLoad(controlMessage.getSLoad());
					sendMessage(cMessage, upstream_neighbour);
					//socket.sendMessage(cMessage, "localsocket://" + upstream_neighbour);
				}
				else
				{
					ControlMessage cMessage = new ControlMessage(ControlMessageType.NeighborRequest);
					cMessage.setUrl(this.getUrl());
					cMessage.setSLoad(controlMessage.getSLoad());
					sendMessage(cMessage, downstream_neighbour);
					//socket.sendMessage(cMessage, "localsocket://" + downstream_neighbour);
				}
			}
		}
		
		
		if (controlMessage.getType() == ControlMessageType.InformQueue)
		{
			int size = jobQueue.size();
			if (size >= controlMessage.getILoad() * 10 || (controlMessage.getILoad() == 0 && size > 10))
			{
				ControlMessage hMessage = new ControlMessage(ControlMessageType.BulkJob);
				hMessage.setUrl(this.getUrl());
				System.out.println("Bulking from: " + url + "-->" + controlMessage.getUrl());
				System.out.println(url + "|: " + size);
				int nrjobs;
				if (size >= controlMessage.getILoad() * 10)
					nrjobs = controlMessage.getILoad() * 5;
				else
					nrjobs = size / 2;
				
				ArrayList<Job> bulk = new ArrayList<Job>();
				for (int i = 0; i < nrjobs; i++)
				{
					Job j = jobQueue.poll();
					if (j != null)
					{
						bulk.add(j);
					}
				}
				hMessage.setJobs(new ArrayList<Job>(bulk));
				sendMessage(hMessage, controlMessage.getUrl());
				//socket.sendMessage(hMessage, "localsocket://" + controlMessage.getUrl());
				System.out.println(url + ": " + jobQueue.size());
			}
			
		}		
		
	}
	
	public void informOthers(String leastLoadedRM, int newLoad)
	{
		ControlMessage uMessage = new ControlMessage(ControlMessageType.UpdateView);
		uMessage.setUrl(leastLoadedRM);
		uMessage.setILoad(newLoad);
		uMessage.setSLoad(this.getUrl());
		sendMessage(uMessage, downstream_neighbour);
		//socket.sendMessage(uMessage, "localsocket://" + downstream_neighbour);
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
		System.out.println(this.getUrl() + " started!");
		while (running) {
			// send a message to each resource manager, requesting its load
			for (String rmUrl : resourceManagerLoad.keySet())
			{
				ControlMessage cMessage = new ControlMessage(ControlMessageType.RequestLoad);
				cMessage.setUrl(this.getUrl());
				sendMessage(cMessage, rmUrl);
				//socket.sendMessage(cMessage, "localsocket://" + rmUrl);
			}
			
			/*int r = generator.nextInt(gridschedulers.size());
			ControlMessage hMessage = new ControlMessage(ControlMessageType.InformQueue);
			hMessage.setUrl(this.getUrl());
			hMessage.setILoad(jobQueue.size());
			socket.sendMessage(hMessage, "localsocket://" + gridschedulers.get(r));*/
			
			// schedule waiting messages to the different clusters
			for (Job job : jobQueue)
			{
				String leastLoadedRM =  getLeastLoadedRM();
				
				if (leastLoadedRM!=null) {
				
					ControlMessage cMessage = new ControlMessage(ControlMessageType.AddJob);
					cMessage.setJob(job);
					sendMessage(cMessage, leastLoadedRM);
					//socket.sendMessage(cMessage, "localsocket://" + leastLoadedRM);
					
					jobQueue.remove(job);
					
					// increase the estimated load of that RM by 1 (because we just added a job)
					int load = resourceManagerLoad.get(leastLoadedRM);
					resourceManagerLoad.put(leastLoadedRM, load+1);
					
					informOthers(leastLoadedRM, load + 1);
					
				}
				
			}
			
			System.out.println(getUrl() + ": " + jobQueue.size());
			
			// sleep
			try
			{
				Thread.sleep(pollSleep);
			} catch (InterruptedException ex) {
				assert(false) : "Grid scheduler runtread was interrupted";
			}
			
		}
		
	}
	
	/**
	 * Stop the polling thread. This has to be called explicitly to make sure the program 
	 * terminates cleanly.
	 *
	 */
	public void stopPollThread() {
		running = false;
		try {
			pollingThread.join();
		} catch (InterruptedException ex) {
			assert(false) : "Grid scheduler stopPollThread was interrupted";
		}
		
	}
	
}
