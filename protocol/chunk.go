package protocol

type Chunk struct {
	Name     string `json:"name"`
	Complete bool   `json:"complete"`
	Size     uint64 `json:"size"`
}
