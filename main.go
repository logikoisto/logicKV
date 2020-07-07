package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	raftkv "logicKV/kvraft"
	"logicKV/labrpc"
	"logicKV/raft"
	"logicKV/rpcraft"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
)

func MakeAndServe(clients []*rpcraft.ClientEnd, me int) {
	persister := raft.MakePersister()
	cl := make([]labrpc.Client, len(clients))
	for i, c := range clients {
		cl[i] = c
	}
	raftkv.StartKVServer(cl, me, persister, -1)
	rpc.HandleHTTP()

	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", clients[me].Port))
	if err != nil {
		log.Fatalf("could not listen: %v", err)
	}
	log.Printf("serving kvraft server on port %d", clients[me].Port)

	err = http.Serve(listener, nil)
	if err != nil {
		log.Fatalf("could not listen: %v", err)
	}
}

type API struct {
	clerk *raftkv.Clerk
}

type APIGetResponse struct {
	Value string `json:"value"`
}

type APIPostRequest struct {
	Value string `json:"value"`
}

type APIPostResponse struct {
	Success bool `json:"success"`
}

func logRequest(handler http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		log.Printf("%s %s %s\n", r.RemoteAddr, r.Method, r.URL)
		handler.ServeHTTP(w, r)
	})
}

func (a *API) Serve() {
	http.HandleFunc("/kvraft/", func(w http.ResponseWriter, r *http.Request) {
		key := strings.TrimPrefix(r.URL.Path, "/kvraft/")
		switch r.Method {
		case "GET":
			val := a.clerk.Get(key)
			json, _ := json.Marshal(&APIGetResponse{val})
			w.Write(json)
		case "POST":
			var req APIPostRequest
			json.NewDecoder(r.Body).Decode(&req)
			a.clerk.Put(key, req.Value)
			json, _ := json.Marshal(&APIPostResponse{true})
			w.Write(json)
		}
	})

	http.ListenAndServe(":3000", logRequest(http.DefaultServeMux))
}

type Client struct {
	clerk *raftkv.Clerk
}

func (c *Client) Prompt() {
	scanner := bufio.NewScanner(os.Stdin)
	for {
		scanner.Scan()
		input := scanner.Text()
		tokens := strings.Split(input, " ")
		switch tokens[0] {
		case "get":
			key := tokens[1]
			fmt.Println(c.clerk.Get(key))
		case "put":
			key := tokens[1]
			value := tokens[2]
			c.clerk.Put(key, value)
		case "append":
			key := tokens[1]
			value := tokens[2]
			c.clerk.Append(key, value)
		case "exit":
			return
		}
	}
}

func main() {
	if len(os.Args) == 1 {
		fmt.Printf("usage: go run main.go server  [ports] [me]\n")
		fmt.Printf("usage: go run main.go client  [ports] [ip]\n")
		return
	}

	var ports []int
	for _, port := range strings.Split(os.Args[2], ",") {
		nport, err := strconv.Atoi(port)
		if err != nil {
			log.Fatalf("bad port: %s\n", port)
		}

		ports = append(ports, nport)
	}

	switch os.Args[1] {
	case "server":
		ip := "localhost"
		clients := rpcraft.MakeClients(ip, ports)
		me, err := strconv.Atoi(os.Args[3])
		if err != nil {
			log.Fatalf("bad index: %s\n", os.Args[2])
		}

		MakeAndServe(clients, me)
	case "api":
		ip := os.Args[3]
		cl := rpcraft.MakeClients(ip, ports)
		clients := make([]labrpc.Client, len(cl))
		for i, c := range cl {
			clients[i] = c
		}
		clerk := raftkv.MakeClerk(clients)
		api := API{clerk}
		api.Serve()
	case "client":
		ip := os.Args[3]
		cl := rpcraft.MakeClients(ip, ports)
		clients := make([]labrpc.Client, len(cl))
		for i, c := range cl {
			clients[i] = c
		}
		clerk := raftkv.MakeClerk(clients)
		c := Client{clerk}
		c.Prompt()
	}
}
