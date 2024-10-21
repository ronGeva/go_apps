package go_db

func connectionScore(connection ProvenanceConnection) ProvenanceScore {
	// Connection score is the value of the most significant byte.
	// This is an arbitrary value chosen to represent some heuristic through which we rank
	// different network endpoints based on their address.

	// The smaller the first byte of the IPV4 is, the better we consider it.
	return ProvenanceScore(connection.Ipv4 >> 24)
}
