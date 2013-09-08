package ipc 

import (
  "fmt" 
  "bytes"
  "log"
  "net"
  "strconv"
  "github.com/nu7hatch/gouuid"
  "github.com/gohadooprpc"
)

type Client struct {
  ClientId *uuid.UUID
  Server string
  Port int
  TCPNoDelay bool
}

type connection struct {
  con *net.TCPConn 
}

func (c *Client) String() (string) {
  buf := bytes.NewBufferString("")
  fmt.Fprint(buf, "<clientId:", c.ClientId)
  fmt.Fprint(buf, ", server:", getServerAddr(c)) 
  fmt.Fprint(buf, ">");
  return buf.String()

}

func (c *Client) Call () (error) {
  log.Println("Connecting...", c)
  _, err := getConnection(c)
  return err
}

func getServerAddr (c *Client) (string) {
  return net.JoinHostPort(c.Server, strconv.Itoa(c.Port))
}

func getConnection (c *Client) (connection, error) {
  con, err := setupConnection(c)
  writeConnectionHeader(con)
  writeConnectionContext(con)
  return con, err
}

func setupConnection (c *Client) (connection, error) {
  addr, _ := net.ResolveTCPAddr("tcp", getServerAddr(c))
  tcpConn, err := net.DialTCP("tcp", nil, addr) 
  if err != nil {
    log.Println("error: ", err)
    return connection{}, err
  } else {
    log.Println("Successfully connected ", c)
  }

  // Set tcp no-delay
  tcpConn.SetNoDelay(c.TCPNoDelay)

  return connection{tcpConn}, nil 
}

func writeConnectionHeader (conn connection) (error) {
  // RPC_HEADER
  log.Println("conn.Write: ", gohadooprpc.RPC_HEADER)
  if _, err := conn.con.Write(gohadooprpc.RPC_HEADER); err != nil {
    log.Fatal("conn.Write", err)
    return err
  }
  

  // RPC_VERSION
  log.Println("conn.Write: ", gohadooprpc.RPC_HEADER)
  if _, err := conn.con.Write(gohadooprpc.VERSION); err != nil {
    log.Fatal("conn.Write", err)
    return err
  }

  // RPC_SERVICE_CLASS
  if serviceClass, err := gohadooprpc.ConvertFixedToBytes(gohadooprpc.RPC_SERVICE_CLASS); err != nil {
    log.Fatal("WTF binary.Write", err)
    return err
  } else if _, err := conn.con.Write(serviceClass); err != nil {
    log.Fatal("conn.Write RPC_SERVICE_CLASS", err)
    return err
  }

  // AuthProtocol
  if authProtocol, err := gohadooprpc.ConvertFixedToBytes(gohadooprpc.AUTH_PROTOCOL_NONE); err != nil {
    log.Fatal("WTF AUTH_PROTOCOL_NONE", err)
    return err
  } else if _, err := conn.con.Write(authProtocol); err != nil {
    log.Fatal("conn.Write RPC_SERVICE_CLASS", err)
    return err
  }

  return nil 
}

func writeConnectionContext (conn connection) (error) {
  return nil
}
