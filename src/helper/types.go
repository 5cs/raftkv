package helper

type Applier interface {
	//Apply(index int, cmd interface{}, isLeader bool, reqId int64)
	Apply(applyMsg interface{})
}
