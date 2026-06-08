package reactor

// Event represents an I/O readiness notification.
type Event struct {
	FD   int
	Type EventType
}
