package shardmaster

import "fmt"

func (sm *ShardMaster) DumpMasterServer() {
	fmt.Printf("%#v\n%#v\n%#v\n%#v\n\n", sm.me, sm.configs, sm.configNum, sm.clientSeqs)
}
