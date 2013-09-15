package gohadooprpc 

import (
  "bytes"
  "encoding/binary"
  "github.com/gohadooprpc/hadoop_common"
)

var RPC_HEADER []byte = []byte("hrpc") 
var VERSION []byte = []byte {0x09} 
var RPC_SERVICE_CLASS byte = 0x00 

var RPC_PROTOCOL_BUFFFER hadoop_common.RpcKindProto = hadoop_common.RpcKindProto_RPC_PROTOCOL_BUFFER
var RPC_FINAL_PACKET hadoop_common.RpcRequestHeaderProto_OperationProto = hadoop_common.RpcRequestHeaderProto_RPC_FINAL_PACKET
var RPC_DEFAULT_RETRY_COUNT int32 = hadoop_common.Default_RpcRequestHeaderProto_RetryCount

type AuthMethod byte
const (
  AUTH_SIMPLE AuthMethod = 0x50
  AUTH_KERBEROS AuthMethod = 0x51
  AUTH_TOKEN AuthMethod = 0x52
  AUTH_PLAIN AuthMethod = 0x53
)
func (authmethod AuthMethod) String() string {
  switch {
    case authmethod == AUTH_SIMPLE:
      return "SIMPLE"
    case authmethod == AUTH_KERBEROS:
      return "GSSAPI"
    case authmethod == AUTH_TOKEN:
      return "DIGEST-MD5"
    case authmethod == AUTH_PLAIN:
      return "PLAIN"
  }
  return "ERROR-UNKNOWN"
}

type AuthProtocol byte 
const (
  AUTH_PROTOCOL_NONE AuthProtocol = 0x00
  AUTH_PROTOCOL_SASL AuthProtocol = 0xDF 
)
func (authprotocol AuthProtocol) String() string {
  switch {
    case authprotocol == AUTH_PROTOCOL_NONE:
      return "NONE"
    case authprotocol == AUTH_PROTOCOL_SASL:
      return "SASL"
  }
  return "ERROR-UNKNOWN"
}

func ConvertFixedToBytes (data interface{}) ([]byte, error) {
  buf := new(bytes.Buffer)
  err := binary.Write(buf, binary.BigEndian, data)
  return buf.Bytes(), err
}

