package distributed.systems.gridscheduler;

import java.net.MalformedURLException;
import java.rmi.AlreadyBoundException;
import java.rmi.NotBoundException;
import java.rmi.RMISecurityManager;
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
		} catch (MalformedURLException e) {
			e.printStackTrace();
		} catch (AlreadyBoundException e) {
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
		} catch (MalformedURLException e) {
			e.printStackTrace();
		} catch (RemoteException e) {
			e.printStackTrace();
		} catch (NotBoundException e) {
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
			
			if (jobId == 250)
			{
				System.out.println("sending shutdown message");
				ControlMessage sMessage = new ControlMessage(ControlMessageType.ShutDown);
				sendMessage(sMessage, "scheduler2");
				//socket.sendMessage(sMessage, "localsocket://" + "scheduler2");
			}
			
			try {
				// Sleep a while before creating a new job
				Thread.sleep(100L);
			} catch (InterruptedException e) {
				assert(false) : "JobCreator runtread was interrupted";
			}
			
		}
		System.out.println("Run method jobcreator stopped.");
		
	}
	
	/**
	 * Stop the polling thread. This has to be called explicitly to make sure the program 
	 * terminates cleanly.
	 *
	 */
	public void stopPollThread() {
		running = false;
		System.out.println("So what's going on?");
		try {
			if (running)
				System.out.println("WTF============");
			else
				pollingThread.join();
		} catch (InterruptedException ex) {
			assert(false) : "JobCreator stopPollThread was interrupted";
		}
		
	}
	
	public void startPollThread() {
		pollingThread.start();
	}
	
	public void stopEveryone() {
		System.out.println("Stopping clusters..");
		for (int i = 0; i < nrClusters; i++)
		{
			ControlMessage cMessage = new ControlMessage(ControlMessageType.ShutDown);
			sendMessage(cMessage, "cluster" + i);
			//socket.sendMessage(cMessage, "localsocket://" + "cluster" + i);
		}
		System.out.println("Stopping GS nodes..");
		for (int j = 0; j < 5; j++)
		{
			if (j != 2)
			{
				System.out.println("Sending to " + j);
				ControlMessage cMessage = new ControlMessage(ControlMessageType.ShutDown);
				sendMessage(cMessage, "scheduler" + j);
				//socket.sendMessage(cMessage, "localsocket://" + "scheduler" + j);
			}
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
		
		System.setProperty("java.security.policy", "file:./my.policy");
		if (System.getSecurityManager() == null) {
			System.setSecurityManager(new RMISecurityManager());
		}
		
		JobCreator jc = new JobCreator(nrClusters);
		
		jc.startPollThread();
		
		Scanner sc = new Scanner(System.in);
		System.out.println("Enter a number to close application..");
		sc.nextInt();
		
		/*Thread t2 = new Thread() {
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
		}*/
		System.out.println("Stopping myself..");
     	jc.stopPollThread();
		System.out.println("Stopping everyone..");
     	jc.stopEveryone();
     	System.out.println("This is the end see ya");
     	return;
	}

}
