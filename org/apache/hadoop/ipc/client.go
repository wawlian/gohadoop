package ipc 

import "fmt"

type Client struct {
}

func call(c Client) {
  fmt.Printf("Client.call")
}

