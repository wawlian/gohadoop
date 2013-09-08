package gohadooprpc 

import (
  "bytes"
  "encoding/binary"
)

var RPC_HEADER []byte = []byte("hrpc") 
var VERSION []byte = []byte {9} 
var RPC_SERVICE_CLASS int32 = 1

type AuthMethod byte
const (
  AUTH_SIMPLE AuthMethod = 80
  AUTH_KERBEROS AuthMethod = 81
  AUTH_TOKEN AuthMethod = 82
  AUTH_PLAIN AuthMethod = 83
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

type AuthProtocol int32 
const (
  AUTH_PROTOCOL_NONE AuthProtocol = 0
  AUTH_PROTOCOL_SASL AuthProtocol = -33
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
