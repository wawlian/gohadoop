package ipc 

import (
  "fmt" 
  "bytes"
  "net"
  "github.com/nu7hatch/gouuid"
)

type Client struct {
  ClientId *uuid.UUID
  Server string
  Port int
}

func (c *Client) String() string {
  buf := bytes.NewBufferString("Client: <")
  fmt.Fprint(buf, " ", c.ClientId)
  fmt.Fprint(buf, " ", c.Server)
  fmt.Fprint(buf, ":")
  fmt.Fprint(buf, c.Port)
  fmt.Fprint(buf, " >");
  return buf.String()

}

func Call(c *Client) {
  fmt.Println(c)
  fmt.Println("Connecting...")
  _, err := getConnection(c)
  if err != nil {
    fmt.Println("error")
  } else {
    fmt.Println("success")
  }
}

func getConnection (c *Client) (net.Conn, error) {
  buf := bytes.NewBufferString("")
  fmt.Fprint(buf, c.Server)
  fmt.Fprint(buf, ":")
  fmt.Fprint(buf, c.Port)
  
  return net.Dial("tcp", buf.String())
}
