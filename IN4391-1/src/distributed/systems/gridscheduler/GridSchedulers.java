package distributed.systems.gridscheduler;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Scanner;

public class GridSchedulers {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws RemoteException {
		/*
		try {
			java.rmi.registry.LocateRegistry.createRegistry(1099);
		} catch (RemoteException e) {
			e.printStackTrace();
		}
		*/
		if (args.length < 1) {
			System.err.println("Please specify the registry of this Scheduler!");
			return;
		}
		
		// Create GridScheduler processes
		final ArrayList<Process> processes = new ArrayList<Process>();
				
				Runtime.getRuntime().addShutdownHook(new Thread() {
					@Override
					public void run() {
						System.out.println("Shutting down GridSchedulers process.");
						for (Process p : processes)
							p.destroy();
					}
				});
		
		System.out.println("Creating GS nodes..");
		
		
		ProcessBuilder pb;
		for (int i = 0; i < 5; i++) {
			pb = new ProcessBuilder("java", "-jar", "LaunchGridScheduler.jar", i + "", ((i + 1) % 5) + "", ((i - 1 + 5) % 5) + "", args[0]);
			pb.redirectErrorStream();
			try {
				processes.add(pb.start());
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		//Process p = processes.get(0);
		for (Process p : processes)
		{
			InputStream inputstream = p.getInputStream();
			InputStreamReader inputstreamreader = new InputStreamReader(inputstream);
			final BufferedReader bufferedreader = new BufferedReader(inputstreamreader);
			// Start a client.
			Thread t = new Thread() {
			BufferedReader bf = bufferedreader;
			public void run() {
				String line;
					try {
						while((line = bf.readLine()) != null) {
							System.out.println(line);
						System.out.println("We done here.");
						}
					} catch (IOException e) {}
				}
			};
			t.start();
		}
		
		System.out.println("Waiting for process to end");
		Process p = processes.get(0);
		try {
			p.waitFor();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		System.out.println("End of gridschedulers process");
		return;
		//	Scanner scan = new Scanner(System.in);
		/*	scan.nextInt();
			pb = new ProcessBuilder("java", "-jar", "LaunchGridScheduler.jar", 1 + "", ((1 + 1) % 3) + "", ((1 - 1 + 3) % 3) + "");
			pb.redirectErrorStream();
			try {
				processes.add(pb.start());
			} catch (IOException e) {
				e.printStackTrace();
			}
			
			Process p1 = processes.get(1);*/
			//for (Process p : processes)
			//{
			/*	InputStream inputstream1 = p1.getInputStream();
				InputStreamReader inputstreamreader1 = new InputStreamReader(inputstream1);
				final BufferedReader bufferedreader1 = new BufferedReader(inputstreamreader1);
				// Start a client.
				Thread t1 = new Thread() {
				BufferedReader bf = bufferedreader1;
				public void run() {
					String line;
						try {
							while((line = bf.readLine()) != null) {
								System.out.println(line);
							}
						} catch (IOException e) {}
					}
				};
				t1.start();
				
			scan.nextInt();
			pb = new ProcessBuilder("java", "-jar", "LaunchGridScheduler.jar", 2 + "", ((2 + 1) % 3) + "", ((2 - 1 + 3) % 3) + "");
			pb.redirectErrorStream();
			try {
				processes.add(pb.start());
			} catch (IOException e) {
				e.printStackTrace();
			}
			
			Process p2 = processes.get(2);
			//for (Process p : processes)
			//{
				InputStream inputstream2 = p2.getInputStream();
				InputStreamReader inputstreamreader2 = new InputStreamReader(inputstream2);
				final BufferedReader bufferedreader2 = new BufferedReader(inputstreamreader2);
				// Start a client.
				Thread t2 = new Thread() {
				BufferedReader bf = bufferedreader2;
				public void run() {
					String line;
						try {
							while((line = bf.readLine()) != null) {
								System.out.println(line);
							}
						} catch (IOException e) {}
					}
				};
				t2.start();
			*/
			//scan.nextInt();
			
			//Process pizza = null;
			/*int a = 2;
			while (a > 1)
			{*/
			/*	pb = new ProcessBuilder("java", "-jar", "LaunchCluster.jar", 0 + "", 1 + "", 50 + "");
				pb.redirectErrorStream();
				try {
					pizza = pb.start();
				} catch (IOException e) {
					e.printStackTrace();
				}
				
				InputStream inputstream = pizza.getInputStream();
				InputStreamReader inputstreamreader = new InputStreamReader(inputstream);
				final BufferedReader bufferedreader = new BufferedReader(inputstreamreader);
				// Start a client.
				Thread t = new Thread() {
				BufferedReader bf = bufferedreader;
				public void run() {
					String line;
						try {
							while((line = bf.readLine()) != null) {
								System.out.println(line);
							}
						} catch (IOException e) {}
					}
				};
				t.start();*/
			/*	a = scan.nextInt();
				pizza.destroy();
			}*/
			
			/*scan.nextInt();
			pizza.destroy();*/
			
	}

}
