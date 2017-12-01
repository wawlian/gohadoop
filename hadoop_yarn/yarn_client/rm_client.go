package yarn_client

import (
	"github.com/hortonworks/gohadoop/hadoop_yarn"
	yarn_conf "github.com/hortonworks/gohadoop/hadoop_yarn/conf"
)

type RMClient struct {
	client hadoop_yarn.ResourceManagerAdministrationProtocolService
	conf   yarn_conf.YarnConfiguration
}

func CreateRMClient(conf yarn_conf.YarnConfiguration) (*RMClient, error) {
	c, err := hadoop_yarn.DialResourceManagerAdministrationProtocolService(conf)
	return &RMClient{client: c, conf: conf}, err
}

func (c *RMClient) UpdateNodeResource(nodeHost string, nodePort int32, memoryMB int32, virtualCpuCores int32) error {
	request := hadoop_yarn.UpdateNodeResourceRequestProto{
		NodeResourceMap: []*hadoop_yarn.NodeResourceMapProto{
			&hadoop_yarn.NodeResourceMapProto{
				NodeId: &hadoop_yarn.NodeIdProto{Host: &nodeHost, Port: &nodePort},
				ResourceOption: &hadoop_yarn.ResourceOptionProto{
					Resource: &hadoop_yarn.ResourceProto{
						Memory:       &memoryMB,
						VirtualCores: &virtualCpuCores,
					},
				},
			},
		},
	}
	response := hadoop_yarn.UpdateNodeResourceResponseProto{}
	return c.client.UpdateNodeResource(&request, &response)
}
