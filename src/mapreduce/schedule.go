package mapreduce

import (
	// "os"
	"fmt"
	"sync"
	"time"
)

//
// schedule() starts and waits for all tasks in the given phase (mapPhase
// or reducePhase). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.
	//
	// Your code here (Part III, Part IV).
	//

	// var wg sync.WaitGroup
	var done chan bool = make(chan bool)

	ids := make([]int, 0)
	for i := 0; i < ntasks; i++ {
		ids = append(ids, i)
	}
	var mu sync.Mutex

	for n := ntasks; n > 0; {
		select {
		case wk := <-registerChan:
			// wg.Add(1)
			go func(wk string) {
				// defer wg.Done()
				for {
					mu.Lock()
					if len(ids) == 0 {
						mu.Unlock()
						break
					}
					id := ids[0]
					ids = ids[1:]
					mu.Unlock()
					args := new(DoTaskArgs)
					args.JobName = jobName
					if phase == mapPhase {
						args.File = mapFiles[id]
					}
					args.Phase = phase
					args.TaskNumber = id
					args.NumOtherPhase = n_other
					ok := call(wk, "Worker.DoTask", args, new(struct{}))
					if ok == false {
						// fmt.Printf("DoTask: RPC %s do task error\n", wk)
						mu.Lock()
						ids = append(ids, id)
						mu.Unlock()
						// Sleep 1 second, let other goroutines acquire the lock
						time.Sleep(time.Second)
					} else {
						// mu.Lock()
						done <- true
						// mu.Unlock()
					}
				}
			}(wk)
		case <-done:
			n--
		}
	}

	// wg.Wait()

	fmt.Printf("Schedule: %v done\n", phase)
}
