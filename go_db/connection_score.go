package go_db

func connectionScore(connection ProvenanceConnection) ProvenanceScore {
	return ProvenanceScore(connection.Ipv4)
}
