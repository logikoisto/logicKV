package rpcraft

import (
	"fmt"
	"log"
	"net/rpc"
)

type ClientEnd struct {
	IP     string
	Port   int
	client *rpc.Client
}

func (e *ClientEnd) Call(svcMeth string, args interface{}, reply interface{}) bool {
	var err error

	if e.client == nil {
		e.client, err = rpc.DialHTTP("tcp", fmt.Sprintf("%s:%d", e.IP, e.Port))
		if err != nil {
			log.Printf("ERROR: %+v", err)
			return false
		}
	}

	err = e.client.Call(svcMeth, args, reply)
	if err != nil {
		log.Printf("ERROR: %+v", err)
	}
	return err == nil
}

func MakeClients(ip string, ports []int) []*ClientEnd {
	var clients []*ClientEnd

	for _, port := range ports {
		clients = append(clients, &ClientEnd{ip, port, nil})
	}

	return clients
}
