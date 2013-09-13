package ipc 

import (
  "fmt" 
  "bytes"
  "log"
  "net"
  "strconv"
  "os/user"
  "code.google.com/p/goprotobuf/proto"
  "github.com/nu7hatch/gouuid"
  "github.com/gohadooprpc"
  "github.com/gohadooprpc/hadoop_common"
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
  writeConnectionContext(c, con)
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

  // TODO: Ping thread

  // Set tcp no-delay
  tcpConn.SetNoDelay(c.TCPNoDelay)

  return connection{tcpConn}, nil 
}

func writeConnectionHeader (conn connection) (error) {
  // RPC_HEADER
  if _, err := conn.con.Write(gohadooprpc.RPC_HEADER); err != nil {
    log.Fatal("conn.Write gohadooprpc.RPC_HEADER", err)
    return err
  } 

  // RPC_VERSION
  if _, err := conn.con.Write(gohadooprpc.VERSION); err != nil {
    log.Fatal("conn.Write gohadooprpc.VERSION", err)
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
    log.Fatal("conn.Write gohadooprpc.AUTH_PROTOCOL_NONE", err)
    return err
  }

  return nil 
}

func writeConnectionContext (c *Client, conn connection) (error) {
  // Figure the current user-name
  var username string
  if user, err := user.Current(); err != nil {
    log.Fatal("user.Current", err)
    return err
  } else {
    username = user.Username
  }
  log.Println("username = " + username)
  userProto := hadoop_common.UserInformationProto{EffectiveUser: nil, RealUser: &username}

  // Create hadoop_common.IpcConnectionContextProto
  protocolName := "org.apache.hadoop.yarn.api.ApplicationClientProtocolPB"
  ipcCtxProto := hadoop_common.IpcConnectionContextProto{UserInfo: &userProto, Protocol: &protocolName}
  
  // Create RpcRequestHeaderProto
  var callId uint32 = 4294967293 // TODO: HADOOP-9944
  var rpcKind hadoop_common.RpcKindProto = hadoop_common.RpcKindProto_RPC_PROTOCOL_BUFFER
  var rpcOperation hadoop_common.RpcRequestHeaderProto_OperationProto = hadoop_common.RpcRequestHeaderProto_RPC_FINAL_PACKET
  var retryCount int32 = hadoop_common.Default_RpcRequestHeaderProto_RetryCount;
  var clientId [16]byte = [16]byte(*c.ClientId)
  rpcReqHeaderProto := hadoop_common.RpcRequestHeaderProto {RpcKind: &rpcKind, RpcOp: &rpcOperation, CallId: &callId, ClientId: clientId[0:16], RetryCount: &retryCount}

  rpcReqHeaderProtoBytes, err := proto.Marshal(&rpcReqHeaderProto)
  if err != nil {
    log.Fatal("proto.Marshal(&rpcReqHeaderProto)", err)
    return err
  }
  log.Println("XXX rpcReqHeaderProtoBytes: ", len(rpcReqHeaderProtoBytes))

  ipcCtxProtoBytes, _ := proto.Marshal(&ipcCtxProto)
  if err != nil {
    log.Fatal("proto.Marshal(&ipcCtxProto)", err)
    return err
  }
  log.Println("XXX ipcCtxProtoBytes: ", len(ipcCtxProtoBytes))

/*
  // Now create IpcRpcRequestHeaderProto
  ipcRpcHeaderProto := hadoop_common.IpcRpcRequestHeaderProto{IpcConnectionContext: &ipcCtxProto, RpcRequestHeader: &rpcReqHeaderProto} 
  ipcRpcHeaderProtoBytes, err := proto.Marshal(&ipcRpcHeaderProto); 
  if err != nil {
    log.Fatal("proto.Marshal(ipcRpcHeaderProto)", err)
    return err
  } else if _, err := conn.con.Write(ipcRpcHeaderProtoBytes); err != nil {
    log.Fatal("conn.Write ipcRpcHeaderProtoBytes", err)
    return err
  }
    log.Println("Sent #bytes: ", len(ipcRpcHeaderProtoBytes))

  log.Println("Success... sent IpcRpcRequestHeaderProto")
*/

  totalLength := len(rpcReqHeaderProtoBytes) + sizeVarint(len(rpcReqHeaderProtoBytes)) + len(ipcCtxProtoBytes) + sizeVarint(len(ipcCtxProtoBytes))
  var tLen int32 = int32(totalLength)
  if totalLengthBytes, err := gohadooprpc.ConvertFixedToBytes(tLen); err != nil {
    log.Fatal("ConvertFixedToBytes(totalLength)", err)
    return err
  } else {
    if wbytes, err := conn.con.Write(totalLengthBytes); err != nil {
      log.Fatal("conn.con.Write(totalLengthBytes)", err)
      return err
    } else {
      log.Println("conn.con.Write(totalLengthBytes) = ", totalLengthBytes)
      log.Println("conn.con.Write(totalLengthBytes) = ", wbytes)
      log.Println("conn.con.Write() totalLength = ", totalLength)
      //log.Fatal("conn.con.Write(totalLengthBytes) = ", wbytes)
    }
  }

  log.Println("About to write rpcReqHeaderProtoBytes: ", len(rpcReqHeaderProtoBytes))
  conn.con.Write(proto.EncodeVarint(uint64(len(rpcReqHeaderProtoBytes))))
  conn.con.Write(rpcReqHeaderProtoBytes)
  log.Println("Wrote rpcReqHeaderProtoBytes: ", len(rpcReqHeaderProtoBytes))
  log.Println("About to write ipcCtxProtoBytes: ", len(ipcCtxProtoBytes))
  conn.con.Write(proto.EncodeVarint(uint64(len(ipcCtxProtoBytes))))
  conn.con.Write(ipcCtxProtoBytes)
  log.Println("Wrote ipcCtxProtoBytes: ", len(ipcCtxProtoBytes))

  return nil
}

func sizeVarint(x int) (n int) {
  for {
    n++
    x >>= 7
    if x == 0 {
      break
    }
  }
  return n
}

