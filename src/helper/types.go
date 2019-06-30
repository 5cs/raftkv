package helper

type Applier interface {
	Apply(index int, cmd interface{})
}
