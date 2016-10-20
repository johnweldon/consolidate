package storage

// Repository is the overall storage unit
type Repository interface {
	AddFile(file string, root string) error
	AllNames() []string
	AllTags() []string
}
