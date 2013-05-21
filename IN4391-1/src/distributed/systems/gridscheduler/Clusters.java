package distributed.systems.gridscheduler;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Random;
import java.util.Scanner;

public class Clusters {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		if (args.length < 1) {
			System.err.println("Please specify the nr of clusters");
			return;
		}
		long seed = System.currentTimeMillis();
		Random generator = new Random(seed);
		int r;
		Scanner scan = new Scanner(System.in);
		final ArrayList<Process> pclusters = new ArrayList<Process>();
		String registry = args[0];
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				System.out.println("Shutting down Clusters process.");
				for (Process p : pclusters)
					p.destroy();
			}
		});
		
		
		ProcessBuilder pb;
		// Create Cluster processes
		for (int i = 0; i < 5; i++) {
			r = generator.nextInt(5);
			pb = new ProcessBuilder("java", "-jar", "LaunchCluster.jar", i + "", r + "", 50 + "", registry);
			pb.redirectErrorStream();
			try {
				pclusters.add(pb.start());
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		for (Process p : pclusters)
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
						}
					} catch (IOException e) {}
					System.out.println("I think we're done here.");
				}
			};
			t.start();
		}
		
		Process p = pclusters.get(0);
		try {
			p.waitFor();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		System.out.println("End of clusters process.");
	}

}
