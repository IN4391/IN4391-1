package distributed.systems.gridscheduler.model;

import java.util.ArrayList;

import distributed.systems.core.Message;

/**
 * 
 * Class that represents the messages being exchanged in the VGS. It has some members to
 * facilitate the passing of common arguments. Feel free to expand it and adapt it to your 
 * needs. 
 * 
 * @author Niels Brouwers
 *
 */
public class ControlMessage extends Message {

	/**
	 * Generated serial version UID
	 */
	private static final long serialVersionUID = -1453428681740343634L;

	private final ControlMessageType type;
	private String url;
	private Job job;
	private ArrayList<Job> jobs;
	private Object load;
	private String sload;
	private int iload;

	/**
	 * Constructs a new ControlMessage object
	 * @param type the type of control message
	 */
	public ControlMessage(ControlMessageType type) {
		this.type = type;
	}

	/**
	 * @return the job
	 */
	public Job getJob() {
		return job;
	}
	
	/**
	 * @return the jobs
	 */
	public ArrayList<Job> getJobs() {
		return jobs;
	}

	/**
	 * <DL>
	 * <DT><B>Preconditions:</B>
	 * <DD>parameter <CODE>job</CODE> cannot be null
	 * </DL>
	 * @param job the job to set
	 */
	public void setJob(Job job) {
		assert(job != null) : "parameter 'job' cannot be null";
		this.job = job;
	}
	
	/**
	 * <DL>
	 * <DT><B>Preconditions:</B>
	 * <DD>parameter <CODE>job</CODE> cannot be null
	 * </DL>
	 * @param job the job to set
	 */
	public void setJobs(ArrayList<Job> jobs) {
		assert(job != null) : "parameter 'job' cannot be null";
		this.jobs = jobs;
	}
	
	/**
	 * @return the load
	 */
	public Object getLoad() {
		return load;
	}

	/**
	 * @param load the load to set
	 */
	public void setLoad(Object load) {
		this.load = load;
	}
	
	/**
	 * @return the load
	 */
	public String getSLoad() {
		return sload;
	}

	/**
	 * @param iload the load to set
	 */
	public void setSLoad(String sload) {
		this.sload = sload;
	}

	/**
	 * @return the load
	 */
	public int getILoad() {
		return iload;
	}

	/**
	 * @param load the load to set
	 */
	public void setILoad(int load) {
		this.iload = load;
	}

	/**
	 * @return the url
	 */
	public String getUrl() {
		return url;
	}

	/**
	 * @param url the url to set
	 */
	public void setUrl(String url) {
		this.url = url;
	}

	/**
	 * @return the type
	 */
	public ControlMessageType getType() {
		return type;
	}


}
