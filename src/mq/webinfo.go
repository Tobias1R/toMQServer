package mq

import (
	"runtime"
	"strconv"
)

type serverInfo struct {
	InstanceName string `json:"instance_name"`
	ServerAddr   string `json:"server_addr"`
	MemoryAlloc  uint64 `json:"memory_alloc"`
	MemoryTotal  uint64 `json:"memory_total"`
	MemorySys    uint64 `json:"memory_sys"`
	MemoryGc     uint64 `json:"memory_gc"`
}

type consumerInfo struct {
	Ip     string `json:"ip"`
	Status string `json:"status"`
}

type QueueInfo struct {
	Name             string         `json:"name"`
	TotalMessages    int            `json:"total_messages"`
	AckMessages      int            `json:"ack_messages"`
	UnAckMessages    int            `json:"un_ack_messages"`
	RejectedMessages int            `json:"rejected_messages"`
	Consumers        []consumerInfo `json:"consumers"`
	MemorySize       uintptr        `json:"memory_size"`
}

type webInfo struct {
	serverInfo `json:"serverInfo"`
	Queues     map[string]QueueInfo `json:"queues"`
}

func bToMb(b uint64) uint64 {
	return b / 1024 / 1024
}

func (qc *queuesControl) ServerInfo() webInfo {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	si := serverInfo{
		InstanceName: "instance",
		ServerAddr:   qc.tcpServer.Listener.Addr().Network() + "://" + qc.tcpServer.Listener.Addr().String(),
		MemoryAlloc:  bToMb(m.Alloc),
		MemoryTotal:  bToMb(m.HeapAlloc),
		MemorySys:    m.Sys,
		MemoryGc:     uint64(m.NumGC),
	}
	qq := make(map[string]QueueInfo)
	for _, q := range qc.queues {
		consumersInfo := make([]consumerInfo, 0)
		for _, con := range q.consumers {
			ci := consumerInfo{
				Ip:     con.session.Conn().RemoteAddr().String(),
				Status: strconv.Itoa(con.status),
			}
			if ci.Ip != "" {
				consumersInfo = append(consumersInfo, ci)
			}

		}
		qq[q.name] = QueueInfo{
			Name:             q.name,
			TotalMessages:    q.TotalMesssages(),
			AckMessages:      0,
			UnAckMessages:    0,
			RejectedMessages: 0,
			Consumers:        consumersInfo,
			MemorySize:       uintptr(q.GetStorageByteSize()),
		}
	}
	wi := webInfo{
		serverInfo: si,
		Queues:     qq,
	}
	return wi
}
