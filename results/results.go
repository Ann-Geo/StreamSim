package results

import "time"

type Throughput struct {
	FirstTS         int64
	LastTS          int64
	ThroughputValue float64
}

func (t *Throughput) CalculateThroughPut(totalMsgCount int64) {
	duration := float64(t.LastTS-t.FirstTS) / 1e9
	t.ThroughputValue = float64(totalMsgCount) / duration
}

type Rtt struct {
	RttMap  map[string][]time.Duration
	AvgRtt  time.Duration
	RttVals []time.Duration
}

func NewRtt() *Rtt {
	return &Rtt{
		RttMap: make(map[string][]time.Duration),
	}
}

func (r *Rtt) CalculateAverageRtt() {

	var sum time.Duration
	var count int

	for _, durations := range r.RttMap {
		for _, value := range durations {
			//fmt.Printf("rtt value: %v\n", value)
			sum += value
			r.RttVals = append(r.RttVals, value)
			count++
		}
	}
	//fmt.Println("rtt sum: ", sum)
	if count > 0 {
		r.AvgRtt = sum / time.Duration(count)
	} else {
		r.AvgRtt = 0
	}
}
