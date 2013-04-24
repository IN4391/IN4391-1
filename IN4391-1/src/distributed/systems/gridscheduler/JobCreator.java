package distributed.systems.gridscheduler;

import java.net.MalformedURLException;
import java.rmi.AlreadyBoundException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.Scanner;

import distributed.systems.core.IMessageReceivedHandler;
import distributed.systems.core.Message;
import distributed.systems.core.SynchronizedSocket;
import distributed.systems.example.LocalSocket;
import distributed.systems.gridscheduler.model.ControlMessage;
import distributed.systems.gridscheduler.model.ControlMessageType;
import distributed.systems.gridscheduler.model.Job;

public class JobCreator extends UnicastRemoteObject implements IMessageReceivedHandler, Runnable{
	
	private int nrClusters;
	static private SynchronizedSocket socket;
	
	// polling thread
	private Thread pollingThread;
	private boolean running;
	
	public JobCreator(int nrClusters) throws RemoteException 
	{
		this.nrClusters = nrClusters;
		
		/*LocalSocket lSocket = new LocalSocket();
		socket = new SynchronizedSocket(lSocket);
		socket.register("JobCreator");

		socket.addMessageReceivedHandler(this);*/
		
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
		
		// start the polling thread
		running = true;
		pollingThread = new Thread(this);
		//pollingThread.start();
	}

	@Override
	public void onMessageReceived(Message message) {
		ControlMessage controlMessage = (ControlMessage)message;
		System.out.println(controlMessage.getSLoad());
		
	}
	
	public static void sendMessage(Message m, String url)
	{
		try {
			IMessageReceivedHandler stub = (IMessageReceivedHandler) java.rmi.Naming.lookup(url);
			stub.onMessageReceived(m);
		} catch (MalformedURLException | RemoteException
				| NotBoundException e) {
			e.printStackTrace();
		}
	}
	
	@Override
	public void run() {
		long jobId = 0;
		while(running)
		{
			// Add a new job to the system that take up random time
			Job job = new Job(8000 + (int)(Math.random() * 5000), jobId++);
			
			ControlMessage cMessage = new ControlMessage(ControlMessageType.SpawnJob);
			cMessage.setJob(job);
			sendMessage(cMessage, "cluster0");
			//socket.sendMessage(cMessage, "localsocket://" + "cluster0");
			
			try {
				// Sleep a while before creating a new job
				Thread.sleep(100L);
			} catch (InterruptedException e) {
				assert(false) : "JobCreator runtread was interrupted";
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
			assert(false) : "JobCreator stopPollThread was interrupted";
		}
		
	}
	
	public void startPollThread() {
		pollingThread.start();
	}
	
	public void stopEveryone() {
		for (int i = 0; i < nrClusters; i++)
		{
			ControlMessage cMessage = new ControlMessage(ControlMessageType.ShutDown);
			sendMessage(cMessage, "cluster" + i);
			//socket.sendMessage(cMessage, "localsocket://" + "cluster" + i);
		}
	}
	
	/**
	 * @param args
	 * @throws RemoteException 
	 */
	public static void main(String[] args) throws RemoteException {
		
		if (args.length < 1) {
			System.err.println("Please specify the nr of clusters");
			return;
		}
		
		final int nrClusters = Integer.parseInt(args[0]);
		
		JobCreator jc = new JobCreator(nrClusters);
		
		jc.startPollThread();
		
		Thread t2 = new Thread() {
      		 public void run() {
         		Scanner sc = new Scanner(System.in);
         		int a = 2;
         		while (a > 1) {
         			a = sc.nextInt();
         			for (int i = 0; i < nrClusters; i++)
         			{
         				ControlMessage replyMessage = new ControlMessage(ControlMessageType.Status);
         				replyMessage.setUrl("JobCreator");
         				sendMessage(replyMessage, "cluster" + i);
         				//socket.sendMessage(replyMessage, "localsocket://" + "cluster" + i);
         			}
         		}
       	 }
     	};
     	t2.start();

     	try {
			t2.join();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
     	jc.stopEveryone();
     	jc.stopPollThread();
	}

}
