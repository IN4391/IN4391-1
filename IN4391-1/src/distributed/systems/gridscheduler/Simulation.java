package distributed.systems.gridscheduler;

import java.net.MalformedURLException;
import java.rmi.AlreadyBoundException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.Random;

import javax.swing.JFrame;

import distributed.systems.core.IMessageReceivedHandler;
import distributed.systems.core.Message;
import distributed.systems.gridscheduler.gui.ClusterStatusPanel;
import distributed.systems.gridscheduler.gui.GridSchedulerPanel;
import distributed.systems.gridscheduler.model.Cluster;
import distributed.systems.gridscheduler.model.ControlMessage;
import distributed.systems.gridscheduler.model.ControlMessageType;
import distributed.systems.gridscheduler.model.GridScheduler;
import distributed.systems.gridscheduler.model.Job;

/**
 *
 * The Simulation class is an example of a grid computation scenario. Every 100 milliseconds 
 * a new job is added to first cluster. As this cluster is swarmed with jobs, it offloads
 * some of them to the grid scheduler, wich in turn passes them to the other clusters.
 * 
 * @author Niels Brouwers, Boaz Pat-El
 */
public class Simulation extends UnicastRemoteObject implements Runnable {
	// Number of clusters in the simulation
	private final static int nrClusters = 5;

	// Number of nodes per cluster in the simulation
	private final static int nrNodes = 50;
	
	// Simulation components
	Cluster clusters[];
	
	GridScheduler gs;
	
	GridSchedulerPanel gridSchedulerPanel;
	
	/**
	 * Constructs a new simulation object. Study this code to see how to set up your own
	 * simulation.
	 * @throws RemoteException 
	 */
	public Simulation() throws RemoteException {
		
		// Bind the node to the RMI registry.
				try {
					java.rmi.Naming.bind("JobCreator", this);
				} catch (MalformedURLException | AlreadyBoundException e) {
					e.printStackTrace();
				}
						
				final String name = "JobCreator";
						
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
				
		GridScheduler scheduler;
		
		ArrayList<GridScheduler> schedulers = new ArrayList<GridScheduler>();
		for (int i = 0; i < 5; i++)
			schedulers.add(new GridScheduler("scheduler" + i, "scheduler" + ((i + 1) % 5), "scheduler" + ((i - 1 + 5) % 5)));
		
		gs = schedulers.get(0);
		
		// Setup the model. Create a grid scheduler and a set of clusters.
		//scheduler = new GridScheduler("scheduler1");
		scheduler = schedulers.get(0);
		
		ArrayList<String> gridschedulers = new ArrayList<String>();
		for (GridScheduler gs : schedulers)
		{
			gridschedulers.add(gs.getUrl());
		}
		//String[] gs_urls = new String[gridschedulers.size()];
		//gs_urls = gridschedulers.toArray(gs_urls);
		//String[] gs_urls = {scheduler.getUrl()};
	
		// Create a new gridscheduler panel so we can monitor our components
		gridSchedulerPanel = new GridSchedulerPanel(schedulers);
		gridSchedulerPanel.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		
		long seed = System.currentTimeMillis();
		Random generator = new Random(seed);
		
		// Startup delay to make sure all GS nodes have started
		try {
			Thread.sleep(5000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		// Create the clusters and nods
		clusters = new Cluster[nrClusters];
		for (int i = 0; i < nrClusters; i++) {
			int r = generator.nextInt(gridschedulers.size());
			clusters[i] = new Cluster("cluster" + i, gridschedulers.get(r), nrNodes); 
			
			// Now create a cluster status panel for each cluster inside this gridscheduler
			ClusterStatusPanel clusterReporter = new ClusterStatusPanel(clusters[i]);
			gridSchedulerPanel.addStatusPanel(clusterReporter);
		}
		
		// Open the gridscheduler panel
		gridSchedulerPanel.start();
		
		// Run the simulation
		Thread runThread = new Thread(this);
		runThread.run(); // This method only returns after the simulation has ended
		
		// Now perform the cleanup
		
		// Stop clusters
		for (Cluster cluster : clusters)
			cluster.stopPollThread();
		
		// Stop grid scheduler
		scheduler.stopPollThread();
	}

	/**
	 * The main run thread of the simulation. You can tweak or change this code to produce
	 * different simulation scenarios. 
	 */
	public void run() {
		long jobId = 0;
		// Do not stop the simulation as long as the gridscheduler panel remains open
		while (gridSchedulerPanel.isVisible()) {
			// Add a new job to the system that take up random time
			Job job = new Job(8000 + (int)(Math.random() * 5000), jobId++);
			ControlMessage cMessage = new ControlMessage(ControlMessageType.SpawnJob);
			cMessage.setJob(job);
			sendMessage(cMessage, clusters[0].getName());
			
			if (jobId == 250)
			{
				System.out.println("sending shutdown message");
				ControlMessage sMessage = new ControlMessage(ControlMessageType.ShutDown);
				sendMessage(sMessage, "scheduler2");
			}
			//clusters[0].getResourceManager().addJob(job);
			//gs.spawnJob();
			
			try {
				// Sleep a while before creating a new job
				Thread.sleep(100L);
			} catch (InterruptedException e) {
				assert(false) : "Simulation runtread was interrupted";
			}
			
		}

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

	/**
	 * Application entry point.
	 * @param args application parameters
	 * @throws RemoteException 
	 */
	public static void main(String[] args) throws RemoteException {
		try {
			java.rmi.registry.LocateRegistry.createRegistry(1099);
		} catch (RemoteException e) {
			e.printStackTrace();
		}
		
		// Create and run the simulation
		new Simulation();
	}

}
