package distributed.systems.gridscheduler.model;

/**
 * 
 * Different types of control messages. Feel free to add new message types if you need any. 
 * 
 * @author Niels Brouwers
 *
 */
public enum ControlMessageType {

	// from RM to GS
	ResourceManagerJoin,
	ReplyLoad,
	Retry,

	// from GS to RM
	RequestLoad,
	
	JoiningGS,
	
	Roger,

	// both ways
	AddJob,
	
	// from GS to GS
	UpdateView,
	
	// from GS to GS
	InformQueue,
	
	//from GS to GS
	BulkJob,
	
	NeighborRequest,
	
	CrashedGS

}
