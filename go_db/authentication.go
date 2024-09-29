package go_db

func authenticationScore(authentication ProvenanceAuthentication) ProvenanceScore {
	// TODO: improve this
	return ProvenanceScore(len(authentication.password))
}
