package distributed.systems.gridscheduler;

import java.rmi.RemoteException;

import distributed.systems.gridscheduler.model.GridScheduler;

public class LaunchGridScheduler {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws RemoteException {
		
		if (args.length < 4) {
			System.err.println("Please specify the ID of this GridScheduler!");
			return;
		}
		
		final int gid 			 = Integer.parseInt(args[0]);
		final int downstream_gid = Integer.parseInt(args[1]);
		final int upstream_gid 	 = Integer.parseInt(args[2]);
		
		try {
			//System.out.println("Launch1");
			GridScheduler gs = new GridScheduler("scheduler" + gid, "scheduler" + downstream_gid, "scheduler" + upstream_gid, args[3]);
		} catch (RemoteException e) {
			e.printStackTrace();
		}
		
	}

}
