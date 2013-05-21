package distributed.systems.gridscheduler.model;

import java.io.IOException;
import java.net.MalformedURLException;
import java.rmi.AlreadyBoundException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

import distributed.systems.core.IMessageReceivedHandler;
import distributed.systems.core.Message;
import distributed.systems.core.SynchronizedSocket;
import distributed.systems.example.LocalSocket;

/**
 * This class represents a resource manager in the VGS. It is a component of a cluster, 
 * and schedulers jobs to nodes on behalf of that cluster. It will offload jobs to the grid
 * scheduler if it has more jobs waiting in the queue than a certain amount.
 * 
 * The <i>jobQueueSize</i> is a variable that indicates the cutoff point. If there are more
 * jobs waiting for completion (including the ones that are running at one of the nodes)
 * than this variable, jobs are sent to the grid scheduler instead. This variable is currently
 * defaulted to [number of nodes] + MAX_QUEUE_SIZE. This means there can be at most MAX_QUEUE_SIZE jobs waiting 
 * locally for completion. 
 * 
 * Of course, this scheme is totally open to revision.
 * 
 * @author Niels Brouwers, Boaz Pat-El
 *
 */
public class ResourceManager extends UnicastRemoteObject implements INodeEventHandler, IMessageReceivedHandler {
	private Cluster cluster;
	private Queue<Job> jobQueue;
	private String socketURL;
	private ArrayList<String> gridschedulers;
	private ArrayList<String> rr;
	private Random generator; 
	private int jobQueueSize;
	private boolean timer;
	private boolean maintenance;
	private HashMap<Integer, Boolean> timers;
	public static final int MAX_QUEUE_SIZE = 32; 
	
	// logger
	Logger logger;  
	FileHandler fh;  

	// Scheduler url
	private String gridSchedulerURL = null;

	private SynchronizedSocket socket;

	/**
	 * Constructs a new ResourceManager object.
	 * <P> 
	 * <DL>
	 * <DT><B>Preconditions:</B>
	 * <DD>the parameter <CODE>cluster</CODE> cannot be null
	 * </DL>
	 * @param cluster the cluster to wich this resource manager belongs.
	 * @throws RemoteException 
	 */
	public ResourceManager(Cluster cluster, String gridscheduler) throws RemoteException	{
		// preconditions
		assert(cluster != null);
		//logger.info("RM in da house");
		this.jobQueue = new ConcurrentLinkedQueue<Job>();

		this.cluster = cluster;
		this.socketURL = cluster.getName();
		
		this.gridschedulers = new ArrayList<String>();
		this.rr = new ArrayList<String>();
		
		this.gridschedulers.add(gridscheduler);
		
		// Number of jobs in the queue must be larger than the number of nodes, because
		// jobs are kept in queue until finished. The queue is a bit larger than the 
		// number of nodes for efficiency reasons - when there are only a few more jobs than
		// nodes we can assume a node will become available soon to handle that job.
		jobQueueSize = cluster.getNodeCount() + MAX_QUEUE_SIZE;
		
		long seed = System.currentTimeMillis();
		generator = new Random(seed);
		
		this.timer   = true;
		this.timers  = new HashMap<Integer, Boolean>();
		
		logger = Logger.getLogger("MyLog");
		
		try {  
            
            // This block configure the logger with handler and formatter  
            fh = new FileHandler(socketURL + "LogFile.log");  
            logger.addHandler(fh);  
            //logger.setLevel(Level.ALL);  
            SimpleFormatter formatter = new SimpleFormatter();  
            fh.setFormatter(formatter);  
              
            // the following statement is used to log any messages  
            logger.info("Opening log for " + socketURL);  
              
        } catch (SecurityException e) {  
            e.printStackTrace();  
        } catch (IOException e) {  
            e.printStackTrace();  
        }  
		
		/*LocalSocket lSocket = new LocalSocket();
		socket = new SynchronizedSocket(lSocket);
		socket.register(socketURL);

		socket.addMessageReceivedHandler(this);*/
		
		// Bind the node to the RMI registry.
		try {
			java.rmi.Naming.bind(socketURL, this);
		} catch (MalformedURLException e) {
			e.printStackTrace();
		} catch (AlreadyBoundException e) {
			e.printStackTrace();
		}
		
		/*catch (MalformedURLException | AlreadyBoundException e) {
			e.printStackTrace();
		}*/
				
		final String name = socketURL;
				
		// Let the node unregister from RMI registry on shut down.
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				logger.info("Shutting down " + name + ".");
				try {
					java.rmi.Naming.unbind(name);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		});
		
		this.gridSchedulerURL = gridscheduler;
		initGridSchedulers(gridscheduler);
	}
	
	public void initGridSchedulers(String random_gs)
	{
		ControlMessage message = new ControlMessage(ControlMessageType.RequestGSes);
		message.setUrl(socketURL);
		message.setLoad(random_gs);
		logger.info("this is it: " + random_gs);
		sendMessage(message, random_gs);
		//socket.sendMessage(message, "localsocket://" + random_gs);
	}
	
	public void roundRobin()
	{
		for (int i = 0; i < gridschedulers.size(); i++)
		{
			rr.add(gridschedulers.get(i));
		}
	}
	
	public void sendMessage(Message m, String url)
	{
		try {
			IMessageReceivedHandler stub = (IMessageReceivedHandler) java.rmi.Naming.lookup(url);
			stub.onMessageReceived(m);
		} catch (MalformedURLException e) {
			e.printStackTrace();
		} catch (RemoteException e) {
			e.printStackTrace();
		} catch (NotBoundException e) {
			e.printStackTrace();
		}
		
		/*catch (MalformedURLException | RemoteException | NotBoundException e) {
			e.printStackTrace();
		}*/
	}

	/**
	 * Add a job to the resource manager. If there is a free node in the cluster the job will be
	 * scheduled onto that Node immediately. If all nodes are busy the job will be put into a local
	 * queue. If the local queue is full, the job will be offloaded to the grid scheduler.
	 * <DL>
	 * <DT><B>Preconditions:</B>
	 * <DD>the parameter <CODE>job</CODE> cannot be null
	 * <DD>a grid scheduler url has to be set for this rm before calling this function (the RM has to be
	 * connected to a grid scheduler)
	 * </DL>
	 * @param job the Job to run
	 */
	public void addJob(Job job) {
		// check preconditions
		assert(job != null) : "the parameter 'job' cannot be null";
		assert(gridSchedulerURL != null) : "No grid scheduler URL has been set for this resource manager";
		
		// if the jobqueue is full, offload the job to the grid scheduler
		if (jobQueue.size() >= jobQueueSize) { //cluster.getNodeCount()){

			ControlMessage controlMessage = new ControlMessage(ControlMessageType.AddJob);
			controlMessage.setUrl(cluster.getName());
			controlMessage.setJob(job);
			
			int r = generator.nextInt(gridschedulers.size());
			final String chosen = gridschedulers.get(r);
			
			/*if (rr.size() == 0)
				roundRobin();
			int r = generator.nextInt(rr.size());
			final String chosen = rr.remove(r);*/
			//socket.sendMessage(controlMessage, "localsocket://" + chosen);
			
			final Job fjob = job;
			timers.put(new Integer((int) fjob.getId()), true);
			sendMessage(controlMessage, chosen);
			//socket.sendMessage(controlMessage, "localsocket://" + chosen);
			
			startTimer(chosen, fjob);
			// Start a client.
			/*Thread t = new Thread() {
				public void run() {
					timers.put(new Integer((int) fjob.getId()), true);
					startTimer(chosen, fjob);
				}
			};
			t.start();*/
			
			// otherwise store it in the local queue
		} else {
			jobQueue.add(job);
			scheduleJobs();
		}

	}
	
	public void startTimer(String gs, Job j)
	{
		ExecutorService service = Executors.newSingleThreadExecutor();
		final Integer jobId = new Integer((int) j.getId());
		try {
		    Runnable r = new Runnable() {
		        @Override
		        public void run() {
		            while (timers.get(jobId)) {}
		        }
		    };

		    Future<?> f = service.submit(r);
		    
		    f.get(1500, TimeUnit.MILLISECONDS);     // attempt the task for 30 sec
		}
		catch (final InterruptedException e) {
		    // The thread was interrupted during sleep, wait or join
		}
		catch (final TimeoutException e) {
			System.out.println("============ TIMEOUT!!! in " + this.socketURL + "========= for " + gs);
			timers.remove(jobId);
			logger.info("Before" + gridschedulers.toString());
			gridschedulers.remove(gs);
			logger.info(gridschedulers.toString() + "");
		    retry(gs, j);
		}
		catch (final ExecutionException e) {
		    // An exception from within the Runnable task
		}
		finally {
			timers.remove(jobId);
		    service.shutdown();
		}
	}
	
	public void stopTimer(Integer jobId)
	{
		timers.put(jobId, false);
	}
	
	public void retry(String gs, Job job)
	{
		logger.info("Retrying for " + gs);
		ControlMessage controlMessage = new ControlMessage(ControlMessageType.Retry);
		controlMessage.setJob(job);
		controlMessage.setUrl(cluster.getName());
		controlMessage.setSLoad(gs);
		
		int r = generator.nextInt(gridschedulers.size());
		String chosen = gridschedulers.get(r);
		sendMessage(controlMessage, chosen);
		//socket.sendMessage(controlMessage, "localsocket://" + chosen);
		
		startTimer(gs, job);
	}

	/**
	 * Tries to find a waiting job in the jobqueue.
	 * @return
	 */
	public Job getWaitingJob() {
		// find a waiting job
		for (Job job : jobQueue) 
			if (job.getStatus() == JobStatus.Waiting) 
				return job;

		// no waiting jobs found, return null
		return null;
	}
	
	public int queueSize() {
		return jobQueue.size();
	}
	/**
	 * Tries to schedule jobs in the jobqueue to free nodes. 
	 */
	public void scheduleJobs() {
		// while there are jobs to do and we have nodes available, assign the jobs to the 
		// free nodes
		Node freeNode;
		Job waitingJob;
		//logger.info(jobQueue.size());
		while ( ((waitingJob = getWaitingJob()) != null) && ((freeNode = cluster.getFreeNode()) != null) ) {
			freeNode.startJob(waitingJob);
		}

	}

	/**
	 * Called when a job is finished
	 * <p>
	 * pre: parameter 'job' cannot be null
	 */
	public void jobDone(Job job) {
		// preconditions
		assert(job != null) : "parameter 'job' cannot be null";

		// job finished, remove it from our pool
		jobQueue.remove(job);
	}

	/**
	 * @return the url of the grid scheduler this RM is connected to 
	 */
	public String getGridSchedulerURL() {
		return gridSchedulerURL;
	}

	/**
	 * Connect to a grid scheduler
	 * <p>
	 * pre: the parameter 'gridSchedulerURL' must not be null
	 * @param gridSchedulerURL
	 */
	public void connectToGridScheduler(String gridSchedulerURL) {

		// preconditions
		assert(gridSchedulerURL != null) : "the parameter 'gridSchedulerURL' cannot be null"; 

		this.gridSchedulerURL = gridSchedulerURL;

		ControlMessage message = new ControlMessage(ControlMessageType.ResourceManagerJoin);
		message.setUrl(socketURL);
		sendMessage(message, gridSchedulerURL);
		//socket.sendMessage(message, "localsocket://" + gridSchedulerURL);

	}
	
	public void exit() {
		int r = generator.nextInt(gridschedulers.size());
		final String chosen = gridschedulers.get(r);
		ControlMessage dMessage = new ControlMessage(ControlMessageType.RMLeave);
		dMessage.setUrl(cluster.getName());
		sendMessage(dMessage, chosen);
		//socket.sendMessage(dMessage, "localsocket://" + chosen);
	}

	/**
	 * Message received handler
	 * <p>
	 * pre: parameter 'message' should be of type ControlMessage 
	 * pre: parameter 'message' should not be null 
	 * @param message a message
	 */
	public void onMessageReceived(Message message) {
		// preconditions
		assert(message instanceof ControlMessage) : "parameter 'message' should be of type ControlMessage";
		assert(message != null) : "parameter 'message' cannot be null";

		ControlMessage controlMessage = (ControlMessage)message;
		
		// resource manager wants to offload a job to us 
		if (controlMessage.getType() == ControlMessageType.AddJob)
		{
			jobQueue.add(controlMessage.getJob());
			scheduleJobs();
		}
		
		// resource manager wants to offload a job to us 
		if (controlMessage.getType() == ControlMessageType.SpawnJob)
		{
			addJob(controlMessage.getJob());
		}

		if (controlMessage.getType() == ControlMessageType.GSDown)
		{
			if (gridschedulers.contains(controlMessage.getSLoad()))
				gridschedulers.remove(controlMessage.getSLoad());
			
			System.out.println(this.socketURL + ": Scheduler down acknowledged.");
			logger.info(gridschedulers.toString() + "");
		}

		// resource manager wants to offload a job to us 
		if (controlMessage.getType() == ControlMessageType.RequestLoad)
		{
			ControlMessage replyMessage = new ControlMessage(ControlMessageType.ReplyLoad);
			replyMessage.setUrl(cluster.getName());
			replyMessage.setILoad(jobQueue.size());
			sendMessage(replyMessage, controlMessage.getUrl());
			//socket.sendMessage(replyMessage, "localsocket://" + controlMessage.getUrl());				
		}
		
		// connect to new GS node 
		if (controlMessage.getType() == ControlMessageType.ReplyGS)
		{
			String gs = controlMessage.getUrl();
			logger.info("RM of: " + socketURL + "registered with" + gs);
			gridschedulers.add(gs);
		}
		
		// connect to new GS node 
		if (controlMessage.getType() == ControlMessageType.JoiningGS)
		{
			connectToGridScheduler("scheduler" + controlMessage.getILoad());
		}
		
		// reply from GS node who is alive
		if (controlMessage.getType() == ControlMessageType.Roger)
		{
			int jobId = controlMessage.getILoad();
			stopTimer(new Integer(jobId));
		}
		
		if (controlMessage.getType() == ControlMessageType.Status)
		{
			List <Node> nodes = cluster.getNodes();
		    
		    int nrBusyNodes = 0;
		    int nrDownNodes = 0;
		    for (Node node : nodes) {
		    	if (node.getStatus() == NodeStatus.Busy) nrBusyNodes++;
		    	if (node.getStatus() == NodeStatus.Down) nrDownNodes++;
		    }
		    
		    int load = (int)Math.round( (nrBusyNodes * 100) / (double)nodes.size() );
		    int availability = (int)Math.round( ( (nodes.size() - nrDownNodes) * 100) / (double)nodes.size() );
		    String reply = "Cluster name: " + cluster.getName() + "\n";
		    reply += "Nr. of nodes: " + cluster.getNodeCount() + "\n";
		    reply += "Load: " + load + "% \n";
		    reply += "Available: " + availability + "% \n";
		    
		    ControlMessage replyMessage = new ControlMessage(ControlMessageType.ReplyLoad);
			replyMessage.setUrl(cluster.getName());
			replyMessage.setSLoad(reply);
			sendMessage(replyMessage, controlMessage.getUrl());
			//socket.sendMessage(replyMessage, "localsocket://" + controlMessage.getUrl());				
		}
		
		if (controlMessage.getType() == ControlMessageType.ShutDown)
		{
			exit();
			try {
				java.rmi.Naming.unbind(this.socketURL);
			} catch (Exception e) {
				e.printStackTrace();
			}
			this.cluster.stopPollThread();
		}

	}

}
