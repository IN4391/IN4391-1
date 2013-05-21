package distributed.systems.core;
/**
 * Software written for the Distributed Systems Lab course.
 * 
 * @author H. Pijper
 * @author P.A.M. Anemaet
 * @author N. Brouwers
 */


import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import distributed.systems.core.magic.SynchronizedSocketInterface;

/**
 * SynchronizedSocket is used to synchronize the messages
 * received from an asynchronous Socket implementation.
 * 
 * Use this class as a wrapper around another Socket
 * to have the messages from that Socket delivered
 * one by one and in an orderly fashion.
 */
public class SynchronizedSocket extends Socket implements Runnable, IMessageReceivedHandler, IStatusListener {
	private boolean running;
	private ArrayList<Message> waitQueue;
	private ArrayList<Message> sendQueue;
	private Thread runningThread;
	private Socket socket;
	private double messageDelay;
	private int iMessageDelay;
	private SynchronizedSocketInterface ssi;
	private long pauseTime;
	private boolean paused;
	private Map< String, Integer > messageCounts;

	/**
	 * Create a new synchronized socket. Base
	 * this on the given socket, listening
	 * for messages received on the given 
	 * socket and making sure they appear 
	 * in order.
	 * 
	 * @param socket is the actual socket
	 * to which data is being sent.
	 */
	public SynchronizedSocket(Socket socket) {
		this.waitQueue = new ArrayList<Message>();
		this.sendQueue = new ArrayList<Message>();
		this.socket = socket;
		this.paused = false;
		this.messageCounts = new HashMap< String, Integer >( );
		setMessageDelay(Math.random() / 100.0);
		runningThread = new Thread(this);
		runningThread.start();

		socket.addMessageReceivedHandler(this);

		/* create our magic interface */
		ssi = new SynchronizedSocketInterface( this );

		// Add the status listener only after ssi has been made, since
		// the callback uses ssi.
		socket.addStatusListener( this );
	}

	/**
	 * Set the delay it takes a message
	 * to be received. This is mainly used
	 * for testing.
	 * 
	 * @param newDelay is the time in seconds.
	 */
	public synchronized void setMessageDelay(double newDelay) {
		messageDelay = newDelay;
		iMessageDelay = (int)(messageDelay*1000.0);
	}

	/**
	 * Useful check to see whether there are any
	 * messages waiting in the buffer.
	 * 
	 * @return 
	 *   whether there are any messages left in 
	 *   the message queue.
	 */
	public boolean hasMessagesWaiting() {
		return !waitQueue.isEmpty();
	}

	/**
	 * @return the URL from the underlying
	 * socket class.
	 */
	public String getURL() {
		return socket.getURL();
	}

	/**
	 * Part of the run cycle which
	 * needs to be thread-safe.
	 */
	public void doRun() {
		/* Check if any messages are waiting
		 * in the queue.
		 */
		boolean doLoop;
		synchronized( this ) {
			doLoop = waitQueue.size() > 0;
		}
		if( doLoop ) {
			while( true ) {
				Message m;
				synchronized( this ) {
					m = getMostRecentMessage();

					// See if m exists and should be delivered by now
					if( m == null || m.getReceivedTime() > ((System.currentTimeMillis( ) / 1000.0) - messageDelay) )
						break;

					// Pop the message from the queue
					waitQueue.remove(m);
				}
				notifyReceivedHandlers(m, this);
			}
		}
		synchronized( this ) {
			doLoop = sendQueue.size() > 0;
		}
		if( doLoop || true ) {
			while( true ) {
				Message m;
				synchronized( this ) { 
					m = getMostRecentSendMessage();

					// See if m exists and should be delivered by now
					if( m == null )//|| m.getSendTime() >= System.currentTimeMillis( ) / 1000.0 )
						break;

					// Pop the message from the queue
					sendQueue.remove(m);
				}
				dispatchMessage(m);
			}
		}
	}

	/**
	 * The main process in the SynchronizedSocket. This
	 * is being activated when an interrupt is made on the
	 * running thread. It will then deliver any messages
	 * in the buffer in order of time received.
	 */
	public void run() {
		running = true;

		while(running) {
			try {
				Thread.sleep(iMessageDelay);
			} catch (InterruptedException e) {
				/* Wake up on interrupt */
			}

			/* Don't run if we're not running or if we're paused */
			if( running && !paused )
				doRun();
		}
	}

	/**
	 * Register the synchronized socket.
	 * 
	 * @param ID 
	 *   is the ID with which the socket is to 
	 *   be registered.
	 */
	public void register(String ID) {
		socket.register(ID);		
		ssi.register( );
	}

	/**
	 * Unregister the synchronized socket.
	 */
	public void unRegister() {
		running = false;
		socket.unRegister();
	}

	/**
	 * Send a message to the specified URL.
	 * 
	 * @param m is the message to send.
	 * @param URL is the url to which the message is to be delivered.
	 */
	public void sendMessage(Message m, String URL) {
		/* Make sure the sendtime is correct */
		//String theURL = URL;
		String theURL = "";
		try {
			// Parse the URL and make it canonical (sort of)
			java.net.URI uri = new java.net.URI( URL );
			if( uri.getScheme( ) != null && !( "".equals( uri.getScheme( ) ) ) )
				theURL = uri.getScheme( ) + "://";
			else
				theURL = "rmi://";
			if( uri.getAuthority( ) != null && !( "".equals( uri.getAuthority( ) ) ) )
				theURL += uri.getAuthority( );
			else
				theURL += "localhost:1099";
			if( uri.getPath( ) != null )
				theURL += uri.getPath( );
			if( uri.getQuery( ) != null && !( "".equals( uri.getQuery( ) ) ) )
				theURL += "?" + uri.getQuery( );
			if( uri.getFragment( ) != null && !( "".equals( uri.getFragment( ) ) ) )
				theURL += "#" + uri.getFragment( );
		}
		catch( java.net.URISyntaxException e ) {
			throw new RuntimeException( e );
		}
		m.send(URL);
		m.put("origin", getURL());
		m.put("destination", theURL );
		synchronized( this ) {
			if( !running )
				return;
			if( paused || sendQueue.size( ) > 0 ) {
				sendQueue.add( m );
				return;
			}
		}
		dispatchMessage( m );
	}

	/**
	 * Actually sends a message.
	 *
	 * @param m is the message to send.
	 */
	private void dispatchMessage(Message m) {
		String URL = (String)m.get("destination");
		synchronized(this) {
			if( !running )
				return;
			if( messageCounts.containsKey( URL ) )
				messageCounts.put( URL, messageCounts.get( URL ) +  1 );
			else
				messageCounts.put( URL, 1 );
		}

		socket.sendMessage(m, URL);
	}

	/**
	 * Get the message, which was sent first.
	 *  
	 * @return the message which has been sent first.
	 */
	public synchronized Message getMostRecentMessage() {
		Message res = null;

		for(Message it : waitQueue)	{
			if (res == null || it.getSendTime() < res.getSendTime())
				res = it;
		}

		return res;
	}

	/**
	 * Get the message, which was sent from this message first.
	 *  
	 * @return the message which has been sent first.
	 */
	public Message getMostRecentSendMessage() {
		Message res = null;

		for(Message it : sendQueue)	{
			if (res == null || it.getSendTime() < res.getSendTime())
				res = it;
		}

		return res;
	}

	/**
	 * Queue the message for delivery in the current socket. 
	 * 
	 * @param message is the message to queue. 
	 */
	public synchronized void onMessageReceived(Message message) {
		message.receive( );
		waitQueue.add(message);
	}

	@Override
	public synchronized void receiveMessage(Message message) {}

	/**
	 * Pauses this Synchronized Socket, if it wasn't already paused.
	 * 
	 * While a Synchronized Socket is paused, no messages are sent on.
	 */
	public void pause( ) {
		synchronized( this ) {
			if( paused )
				return;

			pauseTime = System.currentTimeMillis( );
			paused = true;
		}

		ssi.statusChanged( );
	}

	/**
	 * Resumes this Synchronized Socket, if it was paused.
	 */
	public void play( ) {
		synchronized( this ) {
			if( !paused )
				return;

			double offset = ( System.currentTimeMillis( ) - pauseTime ) / 1000.0;
			for( Message m : waitQueue )
				m.bumpReceiveTime( offset );
			for( Message m : sendQueue )
				m.bumpSendTime( offset );
			paused = false;
		}

		ssi.statusChanged( );
	}

	/**
	 * Sees if the Synchronized Socket is paused.
	 * 
	 * @return True iff the Synchronized Socket is paused.
	 */
	public synchronized boolean isPaused( ) {
		return paused;
	}

	/**
	 * Kills the Synchronized Socket.
	 * 
	 * After killing a Synchronized Socket, it will never
	 * sent messages on, ever.
	 */
	public void kill( ) {
		synchronized( this ) {
			running = false;
		}

		ssi.statusChanged( );
	}

	/**
	 * Sees if the Synchronized Socket is dead after a call to kill( ).
	 * In general a Synchronized Socket is considered dead if it's not running anymore.
	 *
	 * @return      True iff the Synchronized Socket is dead.
	 */
	public synchronized boolean isDead( ) {
		return !running;
	}

	/**
	 * The message counts: the number of messages that have been sent to each destination.
	 * 
	 * Message counts are kept for all outgoing messages.
	 * The map returned by this method maps from destination URL
	 * to number of messages sent since the last call to this
	 * method.
	 * 
	 * Note that this method is used by the Magic package.
	 * 
	 * @return	The map from URL to message count.
	 */
	public synchronized Map< String, Integer > messageCounts( ) {
		Map< String, Integer > res = new HashMap< String, Integer >( messageCounts );
		messageCounts.clear( );
		return res;
	}

	/**
	 * The internal status of the socket.
	 * This status is for human eyes only, so it can be anything you think will be useful.
	 *
	 * This method is forwarded to the socket.
	 *
	 * Note that this method is used by the Magic package.
	 *
	 * This method will at all times be completely unsynchronized and fully reentrant.
	 *
	 * @return      The internal status of the Synchronized Socket.
	 */
	public String getInternalStatus( )  {
		return socket.getInternalStatus( );
	}

	/**
	 * Called when the internal status of the socket changes.
	 *
	 * @arg socket  The socket who's status changed.
	 */
	public void statusChanged( Socket socket ) {
		ssi.statusChanged( );
	}
}