package distributed.systems.core;
/**
 * Software written for the Distributed Systems Lab course.
 * 
 * @author H. Pijper
 * @author P.A.M. Anemaet
 * @author N. Brouwers
 */

import java.util.ArrayList;

/**
 * Abstract socket class. Should be extended to implement
 * different transport types (TCP/IP, RMI, etc).  
 */
public abstract class Socket {
	private ArrayList<IMessageReceivedHandler> receivedHandlers = new ArrayList<IMessageReceivedHandler>();
	private ArrayList<IStatusListener> statusListeners = new ArrayList<IStatusListener>();

	/// The internal status as managed by setInternalStatus( ) and getInternalStatus( ).
	protected String internalStatus = "";

	/**
	 * Send the actual message to the remote socket
	 * with the given ID.
	 * 
	 * @param message to send.
	 * @param ID to deliver the message to.
	 */
	public abstract void sendMessage(Message message, String ID);

	/**
	 * Receive a message. 
	 *  
	 * @param message is the actual message being received.
	 * 
	 * @see Message
	 * @see Message.send
	 */
	public abstract void receiveMessage(Message message);	

	/**
	 * Register the socket, so other sockets can find it
	 * by its ID.
	 * 
	 * @param ID 
	 *   is the ID with which the socket is to 
	 *   be registered.
	 */
	public abstract void register(String ID);

	/**
	 * Unregister the socket. 
	 */
	public abstract void unRegister();

	/**
	 * Set the handler, which receives the message. 
	 * Usually, these messages are passed onto a
	 * SynchronizedSocket instance.
	 * 
	 * @param imrh 
	 *   is the instance which implements the
	 *   IMessageReceivedHandler interface.
	 */
	public void addMessageReceivedHandler(IMessageReceivedHandler imrh)
	{
		receivedHandlers.add(imrh);
	}

	/**
	 * @return the URL of the socket.
	 */
	public abstract String getURL();

	/**
	 * The internal status of the socket.
	 * This status is for human eyes only, so it can be anything you think will be useful.
	 *
	 * This method will at all times be completely unsynchronized and fully reentrant,
	 * meaning that it can be called by several threads at the same time without this causing
	 * problems, and without this being solved by synchronizing.
	 *
	 * @return      The internal status of the socket.
	 */
	public String getInternalStatus( ) {
		return internalStatus;
	}

	/**
	 * Set the internal status of the socket.
	 * This status is for human eyes only, so it can be anything you think will be useful.
	 *
	 * When overriding this method, be sure to either call the original, or call notifyStatusListeners.
	 *
	 * @arg status  The new internal status.
	 */
	public void setInternalStatus( String status ) {
		internalStatus = status;
		notifyStatusListeners( );
	}

	/**
	 * Called when the internal status changed, propagating the change.
	 */
	public void notifyStatusListeners( ) {
		if( statusListeners.size( ) > 0 ) {
			for( IStatusListener sl : statusListeners ) {
				sl.statusChanged( this );
			}
		}
	}

	/**
	 * Adds a StatusListener to the list of listeners to be notified upon internal status changes.
	 *
	 * @arg listener    The StatusListener to add.
	 */
	public void addStatusListener( IStatusListener listener ) {
		statusListeners.add( listener );
	}

	/**
	 * Called when a message is received, to actually
	 * deliver the message to the handler.
	 * 
	 * @param message is the message, which is to be delivered.
	 * @param caller is the object that is calling, if it implements
	 * 		IMessageReceivedHandler. This prevents the caller from
	 * 		being notified about its own message.
	 */
	protected void notifyReceivedHandlers(Message message, IMessageReceivedHandler caller)
	{
		if (receivedHandlers.size() > 0)
			for(IMessageReceivedHandler imrh: receivedHandlers)
				if (imrh != caller)
					imrh.onMessageReceived(message);		
	}	
}