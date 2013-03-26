/**
 * Software written for the Distributed Systems Lab course.
 * 
 * @author H. Pijper
 * @author P.A.M. Anemaet
 * @author N. Brouwers
 */
package distributed.systems.example;

import java.rmi.RemoteException;
import java.util.HashMap;
import java.util.Map;

import distributed.systems.core.Message;
import distributed.systems.core.Socket;
import distributed.systems.core.exception.IDNotAssignedException;

public class LocalSocket extends Socket {

	/**
	 * 
	 */
	private static final long serialVersionUID = 5696990532894945513L;
	/* ID of the current socket */
	private String url;
	/* Hashmap where ids are stored */
	private static Map<String, Socket> localMap = new HashMap<String, Socket>();

	/**
	 * Create a new local socket.
	 */
	public LocalSocket() throws RemoteException
	{
	}

	/**
	 * Register the socket with the hashmap.
	 */
	@Override
	public void register(String ID) {
		// Use a new 'protocol' for LocalSockets
		url = "localsocket://"+ID;
		localMap.put(url, this);
	}

	/**
	 * Notify handlers that a message has arrived.
	 */
	public void receiveMessage(Message message) {
		notifyReceivedHandlers(message, null);
	}

	/**
	 * Unregister the socket 
	 */
	@Override
	public void unRegister() {
		localMap.remove(url);
	}

	/**
	 * @return this URL.
	 */
	public String getURL() {
		return url;
	}

	/**
	 * Send a message to the specified URL.
	 * 
	 * @param message is the message to send.
	 * @param URL is the target address to send it to.
	 */
	@Override
	public void sendMessage(Message message, String URL ) {
		if (localMap.containsKey(URL))
		{
			message.put("origin", getURL());
			localMap.get(URL).receiveMessage(message);
		}
		else
			throw new IDNotAssignedException();
	}
}