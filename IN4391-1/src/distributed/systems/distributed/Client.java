package distributed.systems.distributed;

import java.rmi.server.UnicastRemoteObject;

import distributed.systems.core.IMessageReceivedHandler;
import distributed.systems.core.Message;

public abstract class Client extends UnicastRemoteObject implements  IMessageReceivedHandler{
	private static final long serialVersionUID = 1L;
	private int clientID;
	private final String clientURL;
	
	/**
	 * Constructor
	 * @param clientID
	 * @throws RemoteException
	 */
	protected Client(int clientID) throws java.rmi.RemoteException {
		this.clientID = clientID;
		this.clientURL = createURL(this.clientID);
		try {
			java.rmi.Naming.bind(this.clientURL, this);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}	
	
	
	/**
	 * TODO how to work with unknown/nonlocal hosts???
	 * @param clientID
	 * @return the url of the clientID
	 */
	public static String createURL(int clientID) {
		return ("rmi://localhost/gridscheduler_" + clientID);
	}
	
	
	public void sendMessage(Message msg, int receiverID) throws java.rmi.RemoteException{
		try {
			IMessageReceivedHandler receiver = (IMessageReceivedHandler) java.rmi.Naming.lookup(createURL(receiverID));
			receiver.onMessageReceived(msg);
		} catch (java.rmi.RemoteException e) {
			e.printStackTrace();
		} catch (java.net.MalformedURLException e) {
			e.printStackTrace();
		} catch (java.rmi.NotBoundException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Initializes the rmiregistry
	 * @param port
	 */
	public static void init(int port) {
		try {
			java.rmi.registry.LocateRegistry.createRegistry(port);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * The remote callable method
	 */
	public void onMessageReceived(Message msg) {
		this.messageReceived(msg);
	}
	
	public abstract void messageReceived(Message msg);

}
