package main

import (
  "log"
  "github.com/gohadooprpc/org/apache/hadoop/ipc/client"
  "github.com/gohadooprpc/hadoop_common"
  "github.com/gohadooprpc/hadoop_yarn"
  "github.com/nu7hatch/gouuid"
)

func main() {
  clientId, _ := uuid.NewV4()
  c := &ipc.Client{ClientId: clientId, Server: "0.0.0.0", Port: 28081}

  // Create GetApplicationsRequestProto
  methodName := "getApplications"
  protocolName := "org.apache.hadoop.yarn.api.ApplicationClientProtocolPB"
  var clientProtocolVersion uint64 = 1
  getAppsRpcProto := hadoop_common.RequestHeaderProto {MethodName: &methodName, DeclaringClassProtocolName: &protocolName, ClientProtocolVersion: &clientProtocolVersion}
  applicationStates := []hadoop_yarn.YarnApplicationStateProto{hadoop_yarn.YarnApplicationStateProto_ACCEPTED, hadoop_yarn.YarnApplicationStateProto_RUNNING, hadoop_yarn.YarnApplicationStateProto_SUBMITTED}
  getAppsReqProto := hadoop_yarn.GetApplicationsRequestProto {ApplicationStates: applicationStates}

  // Create GetApplicationsResponseProto 
  getAppsResProto := hadoop_yarn.GetApplicationsResponseProto{}
  err := c.Call(&getAppsRpcProto, &getAppsReqProto, &getAppsResProto)
  if err != nil {
    log.Fatal("Client.call failed", err)
  }
  log.Println("Returned response: ", getAppsResProto)

  // Create GetNewApplicationRequest
  methodName = "getNewApplication"
  protocolName = "org.apache.hadoop.yarn.api.ApplicationClientProtocolPB"
  getNewAppRpcProto := hadoop_common.RequestHeaderProto {MethodName: &methodName, DeclaringClassProtocolName: &protocolName, ClientProtocolVersion: &clientProtocolVersion}
  getNewAppReqProto := hadoop_yarn.GetNewApplicationRequestProto {}
  getNewAppResProto := hadoop_yarn.GetNewApplicationResponseProto {}
  err = c.Call(&getNewAppRpcProto, &getNewAppReqProto, &getNewAppResProto)
  if err != nil {
    log.Fatal("Client.call failed", err)
  }
  log.Println("Returned response: ", getNewAppResProto)
}
