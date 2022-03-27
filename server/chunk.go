package server

type Chunk struct {
	Name     string `json:"name"`
	Complete bool   `json:"complete"`
}
