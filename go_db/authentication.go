package go_db

func authenticationScore(authentication ProvenanceAuthentication) ProvenanceScore {
	// Rank password as better the longer it is, capped at rank 0 (which is the best rank).
	// Return a score between 0 and 256.
	const optimalLength int = 32
	const maximalScore int = 256
	const scorePerCharacterShort int = maximalScore / optimalLength

	length := len(authentication.Password)
	if length >= optimalLength {
		return 0
	}

	// Each character short of 64 characters will increase the score in 4, up to 256.
	return ProvenanceScore((optimalLength - length) * scorePerCharacterShort)
}
