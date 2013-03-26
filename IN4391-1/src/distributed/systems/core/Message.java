package distributed.systems.core;

/**
 * Software written for the Distributed Systems Lab course.
 * 
 * @author H. Pijper
 * @author P.A.M. Anemaet
 * @author N. Brouwers
 */

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import distributed.systems.core.exception.MessageHasAlreadyBeenReceivedException;
import distributed.systems.core.exception.MessageHasAlreadyBeenSendException;
import distributed.systems.core.exception.MessageHasNotBeenReceivedException;
import distributed.systems.core.exception.MessageHasNotBeenSentException;

/**
 * A message is the actual data unit, which is transmitted over the transport
 * layer. This contains both the actual data and relevant metadata (time this
 * was send, etc).
 */
@SuppressWarnings("serial")
public class Message implements Serializable {
	/* Current global message id in the current JVM */
	private static int messageIDs = 0;
	/* Data elements */
	public Map<String, Serializable> dataElements;
	/* IDs of internal data strings */
	private static final String ID_MESSAGEID = "*MessageID";
	private static final String ID_SENDTIME = "*SendTime";
	private static final String ID_SENDERID = "*SenderID";
	private static final String ID_RECEIVETIME = "*ReceiveTime";

	/**
	 * Construct a message, assign a unique message ID (according to the current
	 * JVM) and assign the time at which this message was sent.
	 * 
	 * @pre none
	 * @post message hasBeenSend state is false
	 */
	public Message() {
		dataElements = new HashMap<String, Serializable>();
		dataElements.put(ID_MESSAGEID, Message.getNewMessageID());
		dataElements.put(ID_SENDTIME, -1.0);
		dataElements.put(ID_RECEIVETIME, -1.0);
	}

	/**
	 * @return a unique threadsafe identifier, identifying the message.
	 */
	private static synchronized int getNewMessageID() {
		return ++messageIDs;
	}

	/**
	 * Call this when the message is about to be send.
	 * 
	 * @pre message has not been sent
	 * 
	 * @post message has been sent, sendTime has been updated.
	 * 
	 * @throws MessageHasAlreadyBeenSendException
	 *             when the message has already been sent.
	 */
	public void send(String ID) {
		if ((Double) dataElements.get(ID_SENDTIME) != -1)
			throw new MessageHasAlreadyBeenSendException();

		dataElements.put(ID_SENDTIME, System.currentTimeMillis() / 1000.0);
		dataElements.put(ID_SENDERID, ID);
	}

	/**
	 * Call this when the message has just been received.
	 * 
	 * @pre message has been sent, but not received
	 * 
	 * @post message has been received, receivedTime has been updated.
	 * 
	 * @throws MessageHasAlreadyBeenReceivedException
	 *             when the message has already been received.
	 * 
	 * @throws MessageHasNotBeenSentException
	 *             when the message has not been sent.
	 */
	public void receive() {
		if ((Double) dataElements.get(ID_RECEIVETIME) != -1)
			throw new MessageHasAlreadyBeenReceivedException();
		if ((Double) dataElements.get(ID_SENDTIME) == -1)
			throw new MessageHasNotBeenSentException();

		dataElements.put(ID_RECEIVETIME, System.currentTimeMillis() / 1000.0);
	}

	/**
	 * Adds an amount of time to the time this message was sent.
	 * 
	 * @pre Message has been sent
	 * 
	 * @post The time the message was sent has been increased by the offset.
	 * 
	 * @arg offset The amount of time, in seconds, to add to the time the
	 *      message was received.
	 * 
	 * @throws MessageHasNotBeenSentException
	 *             when the message has not been sent yet.
	 */
	public void bumpSendTime(double offset) {
		if ((Double) dataElements.get(ID_SENDTIME) == -1)
			throw new MessageHasNotBeenSentException();

		dataElements.put(ID_SENDTIME, (Double) dataElements.get(ID_SENDTIME)
				+ offset);
	}

	/**
	 * Adds an amount of time to the time this message was received.
	 * 
	 * @pre Message has been received
	 * 
	 * @post The time the message was received has been increased by the offset.
	 * 
	 * @arg offset The amount of time, in seconds, to add to the time the
	 *      message was received.
	 * 
	 * @throws MessageHasNotBeenReceivedException
	 *             when the message has not been received yet.
	 */
	public void bumpReceiveTime(double offset) {
		if ((Double) dataElements.get(ID_RECEIVETIME) == -1)
			throw new MessageHasNotBeenReceivedException();

		dataElements.put(ID_RECEIVETIME, (Double) dataElements
				.get(ID_RECEIVETIME)
				+ offset);
	}

	/**
	 * Return the time at which the message has been sent.
	 * 
	 * @return the time at which the message has been sent.
	 * @pre Message has been send.
	 * @throws MessageHasNotBeenSentException
	 *             when the message has not been sent yet (so there is no actual
	 *             sendtime).
	 */
	public double getSendTime() {
		if ((Double) dataElements.get(ID_SENDTIME) == -1)
			throw new MessageHasNotBeenSentException();

		return (Double) dataElements.get(ID_SENDTIME);
	}

	/**
	 * Return the time at which the message has been received.
	 * 
	 * @return the time at which the message has been received.
	 * @pre Message has been received.
	 * @throws MessageHasNotBeenReceivedException
	 *             when the message has not been received yet.
	 */
	public double getReceivedTime() {
		if ((Double) dataElements.get(ID_RECEIVETIME) == -1)
			throw new MessageHasNotBeenReceivedException();

		return (Double) dataElements.get(ID_RECEIVETIME);
	}

	/**
	 * @return true iff the message has been sent.
	 */
	public boolean hasBeenSent() {
		return (Double) dataElements.get(ID_SENDTIME) != -1;
	}

	/**
	 * @return the unique message id.
	 */
	public int getMessageID() {
		return (Integer) dataElements.get(ID_MESSAGEID);
	}

	/**
	 * @return target ID.
	 */
	public String getID() {
		return (String) dataElements.get(ID_SENDERID);
	}

	/**
	 * Insert an element in the dataset.
	 * 
	 * @param key
	 *            identifier of the hashmap.
	 * @param value
	 *            replacing the old value of the key.
	 */
	public void put(String key, Serializable value) {
		dataElements.put("_" + key, value);
	}

	/**
	 * Retrieve an element from the dataset.
	 * 
	 * @param key
	 *            identifier.
	 * 
	 * @return the value at the key position.
	 */
	public Serializable get(String key) {
		return dataElements.get("_" + key);
	}
}