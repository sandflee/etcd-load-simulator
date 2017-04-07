package cmd


func NewStore() store {
	return store{}
}

type store struct {
	values map[string]string
}

func (s *store)Update() {

}

