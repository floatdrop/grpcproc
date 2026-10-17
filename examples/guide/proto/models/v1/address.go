package modelsv1

// The scheduler's address, SchedulerAddr, and its handler are generated
// from the Scheduler service in models.proto (models_grpcproc.pb.go); what
// the contract says beyond its messages is here.
const (
	// Service is the models service's name. Its nodes are not placed: they
	// are found in Group.
	Service = "models"
	// Group is the process group every GPU node's scheduler is in: where the
	// conversations find the nodes that run the model.
	Group = "models"
	// ToolFailed begins the text of a tool's turn when the tool failed: what
	// follows is why.
	ToolFailed = "failed: "
)
