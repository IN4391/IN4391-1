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
	RequestGSes,
	ReplyLoad,
	Retry,

	// from GS to RM
	RequestLoad,
	
	ReplyGS,
	
	JoiningGS,
	
	GSDown,
	
	Roger,

	// both ways
	AddJob,
	SpawnJob,
	
	// from GS to GS
	UpdateView,
	
	// from GS to GS
	InformQueue,
	
	//from GS to GS
	BulkJob,
	
	NeighborRequest,
	
	MaintenanceCheck,
	
	MaintenanceDone,
	
	ForwardRM,
	
	CrashedGS,
	
	Status,
	
	ShutDown

}
