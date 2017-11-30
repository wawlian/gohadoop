// Code generated by protoc-gen-go.
// source: containermanagement_protocol.proto
// DO NOT EDIT!

package hadoop_yarn

import proto "github.com/golang/protobuf/proto"
import json "encoding/json"
import math "math"

import (
	"github.com/hortonworks/gohadoop"
	hadoop_ipc_client "github.com/hortonworks/gohadoop/hadoop_common/ipc/client"
	"github.com/nu7hatch/gouuid"
	"net"
	"strconv"
)

// Reference proto, json, and math imports to suppress error if they are not otherwise used.
var _ = proto.Marshal
var _ = &json.SyntaxError{}
var _ = math.Inf

var protocolName = "org.apache.hadoop.yarn.api.ContainerManagementProtocolPB"

func init() {
}

type ContainerManagementProtocolService interface {
	StartContainers(in *StartContainersRequestProto, out *StartContainersResponseProto) error
	StopContainers(in *StopContainersRequestProto, out *StopContainersResponseProto) error
	GetContainerStatuses(in *GetContainerStatusesRequestProto, out *GetContainerStatusesResponseProto) error
}

type ContainerManagementProtocolServiceClient struct {
	*hadoop_ipc_client.Client
}

func (c *ContainerManagementProtocolServiceClient) StartContainers(in *StartContainersRequestProto, out *StartContainersResponseProto) error {
	return c.Call(gohadoop.GetCalleeRPCRequestHeaderProto(&protocolName), in, out)
}
func (c *ContainerManagementProtocolServiceClient) StopContainers(in *StopContainersRequestProto, out *StopContainersResponseProto) error {
	return c.Call(gohadoop.GetCalleeRPCRequestHeaderProto(&protocolName), in, out)
}
func (c *ContainerManagementProtocolServiceClient) GetContainerStatuses(in *GetContainerStatusesRequestProto, out *GetContainerStatusesResponseProto) error {
	return c.Call(gohadoop.GetCalleeRPCRequestHeaderProto(&protocolName), in, out)
}

// DialContainerManagementProtocolService connects to an ContainerManagementProtocolService at the specified network address.
func DialContainerManagementProtocolService(host string, port int) (ContainerManagementProtocolService, error) {
	clientId, _ := uuid.NewV4()
	ugi, _ := gohadoop.CreateSimpleUGIProto()
	c := &hadoop_ipc_client.Client{ClientId: clientId, Ugi: ugi, ServerAddress: net.JoinHostPort(host, strconv.Itoa(port))}
	return &ContainerManagementProtocolServiceClient{c}, nil
}

/*
// DialContainerManagementProtocolServiceTimeout connects to an ContainerManagementProtocolService at the specified network address.
func DialContainerManagementProtocolServiceTimeout(network, addr string,
	timeout time.Duration) (*ContainerManagementProtocolServiceClient, *rpc.Client, error) {
	c, err := protorpc.DialTimeout(network, addr, timeout)
	if err != nil {
		return nil, nil, err
	}
	return &ContainerManagementProtocolServiceClient{c}, c, nil
}
*/
