package distributed.systems.gridscheduler;

import java.rmi.RemoteException;

import distributed.systems.gridscheduler.model.Cluster;

public class LaunchCluster {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws RemoteException {
		
		if (args.length < 1) {
			System.err.println("Please specify the ID of this Cluster!");
			return;
		}
		
		final int cid 			    = Integer.parseInt(args[0]);
		final String grid_scheduler = args[1];
		final int nrNodes 	 	    = Integer.parseInt(args[2]);
		
		System.out.println("We launching something here");
		
		try {
			Cluster c = new Cluster("cluster" + cid, "scheduler" + grid_scheduler, nrNodes, args[3]);
		} catch (RemoteException e) {
			e.printStackTrace();
		}

	}

}
