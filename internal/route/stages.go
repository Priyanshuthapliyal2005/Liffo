package route

// Stage is the request lifecycle position for streaming recovery decisions.
// Stages are ordered; higher stages mean more progress on a single worker attempt.
type Stage string

const (
	StageQueued            Stage = "queued"
	StageAssigned          Stage = "assigned"
	StageConnected         Stage = "connected"
	StageMetadataReceived  Stage = "metadata_received"
	StageFirstVisibleToken Stage = "first_visible_token"
	StageStreaming         Stage = "streaming"
	StageCompleted         Stage = "completed"
)

// StageOrder for comparisons (higher = further along).
func StageOrder(s Stage) int {
	switch s {
	case StageQueued:
		return 0
	case StageAssigned:
		return 1
	case StageConnected:
		return 2
	case StageMetadataReceived:
		return 3
	case StageFirstVisibleToken:
		return 4
	case StageStreaming:
		return 5
	case StageCompleted:
		return 6
	default:
		return -1
	}
}

// HasVisibleOutput is true once any non-empty completion text was observed.
// Retrying after this point would risk duplicating visible output to the client.
func HasVisibleOutput(s Stage) bool {
	return StageOrder(s) >= StageOrder(StageFirstVisibleToken)
}

// SafeToRetryStage returns whether a failure at this stage may be retried on
// another worker without replicating visible tokens.
//
// Policy (evidence-driven contract):
//
//	SAFE:   queued | assigned | connected | metadata_received
//	UNSAFE: first_visible_token | streaming | completed
func SafeToRetryStage(s Stage) bool {
	switch s {
	case StageQueued, StageAssigned, StageConnected, StageMetadataReceived:
		return true
	default:
		return false
	}
}
