package distributed.systems.gridscheduler;

import java.rmi.RMISecurityManager;
import java.rmi.RemoteException;
import java.util.ArrayList;

import distributed.systems.gridscheduler.model.GridScheduler;

public class LaunchGridScheduler {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws RemoteException {
		
		if (args.length < 1) {
			System.err.println("Please specify the ID of this GridScheduler!");
			return;
		}
		
		final int gid 			 = Integer.parseInt(args[0]);
		final int downstream_gid = Integer.parseInt(args[1]);
		final int upstream_gid 	 = Integer.parseInt(args[2]);
		
		// Create and install a security manager
		/*if (System.getSecurityManager() == null) {
		System.setSecurityManager(new RMISecurityManager());
		}*/
		
		try {
			GridScheduler gs = new GridScheduler("scheduler" + gid, "scheduler" + downstream_gid, "scheduler" + upstream_gid);
		} catch (RemoteException e) {
			e.printStackTrace();
		}
		
	}

}
