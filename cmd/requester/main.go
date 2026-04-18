package main

import (
	"context"
	"flag"
	"fmt"
	"log"

	raftpb "raft-maekawa-sync/api/raft"
	"raft-maekawa-sync/internal/rpc"
)

func main() {
	raftAddr := flag.String("raft", "", "address of a raft node, e.g. 127.0.0.1:5001")
	data := flag.String("data", "", "task payload")
	flag.Parse()

	if *raftAddr == "" || *data == "" {
		log.Fatal("--raft and --data are required")
	}

	conn, err := rpc.Dial(*raftAddr)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	client := raftpb.NewRaftClient(conn)
	resp, err := client.SubmitTask(context.Background(), &raftpb.SubmitTaskRequest{Data: *data})
	if err != nil {
		log.Fatal(err)
	}

	if !resp.Success {
		fmt.Printf("Not the leader. Try node %d\n", resp.LeaderId)
		return
	}
	fmt.Printf("Task submitted: %s\n", resp.TaskId)
}