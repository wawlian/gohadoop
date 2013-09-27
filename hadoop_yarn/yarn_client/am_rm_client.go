package yarn_client

import (
  "github.com/gohadoop/hadoop_yarn"
  yarn_conf "github.com/gohadoop/hadoop_yarn/conf"
)

type AMRMClient struct {
  client hadoop_yarn.ApplicationMasterProtocolService
}

func CreateAMRMClient (conf yarn_conf.YarnConfiguration) (*AMRMClient, error) {
  c, err := hadoop_yarn.DialApplicationMasterProtocolService(conf)
  return &AMRMClient{client: c}, err
}

func (c *AMRMClient) RegisterApplicationMaster (host string, port int32, url string, applicationAttemptId *hadoop_yarn.ApplicationAttemptIdProto) (error) {
  request := hadoop_yarn.RegisterApplicationMasterRequestProto{Host: &host, RpcPort: &port, TrackingUrl: &url, ApplicationAttemptId: applicationAttemptId}
  response := hadoop_yarn.RegisterApplicationMasterResponseProto{}
  return c.client.RegisterApplicationMaster(&request, &response)
}

func (c *AMRMClient) FinishApplicationMaster (finalStatus *hadoop_yarn.FinalApplicationStatusProto, message string, url string, applicationAttemptId *hadoop_yarn.ApplicationAttemptIdProto) (error) {
  request := hadoop_yarn.FinishApplicationMasterRequestProto{FinalApplicationStatus: finalStatus, Diagnostics: &message, TrackingUrl: &url, ApplicationAttemptId: applicationAttemptId}
  response := hadoop_yarn.FinishApplicationMasterResponseProto{}
  return c.client.FinishApplicationMaster(&request, &response)
}

