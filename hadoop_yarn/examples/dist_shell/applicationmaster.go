package main

import (
  "os"
  "strings"
  "strconv"
  "log"
  "time"
  "github.com/gohadoop/hadoop_yarn"
  "github.com/gohadoop/hadoop_yarn/conf"
  "github.com/gohadoop/hadoop_yarn/yarn_client"
)

func parseAppAttemptId () (*hadoop_yarn.ApplicationAttemptIdProto, error) {
  appAttemptIdString := os.Getenv("APPLICATION_ATTEMPT_ID")
  log.Println("APPLICATION_ATTEMPT_ID: ", appAttemptIdString)

  appAttemptIdStrComponents := strings.Split(appAttemptIdString, "_")

  clusterTimeStamp, err := strconv.ParseInt(appAttemptIdStrComponents[1], 10, 64)
  if err != nil {
    return nil, err
  }

  i, err := strconv.Atoi(appAttemptIdStrComponents[2])
  if err != nil {
    return nil, err
  }
  var applicationId int32 = int32(i)

  i, err = strconv.Atoi(appAttemptIdStrComponents[3])
  if err != nil {
    return nil, err
  }
  var attemptId int32 = int32(i)

  return &hadoop_yarn.ApplicationAttemptIdProto{ApplicationId: &hadoop_yarn.ApplicationIdProto{ClusterTimestamp: &clusterTimeStamp, Id: &applicationId}, AttemptId: &attemptId}, nil 
}

func main() {
  var err error

  // Get applicationAttemptId from environment
  applicationAttemptId, err := parseAppAttemptId()
  if err != nil {
    log.Fatal("parseAppAttemptId: ", err)
  }

  // Create YarnConfiguration
  conf, _ := conf.NewYarnConfiguration()

  // Create AMRMClient
  rmClient, _ := yarn_client.CreateAMRMClient(conf, applicationAttemptId)
  log.Println("Created RM client: ", rmClient)

  // Register with ResourceManager
  log.Println("About to register application master.")
  err = rmClient.RegisterApplicationMaster("", -1, "")
  if err != nil {
    log.Fatal("rmClient.RegisterApplicationMaster ", err)
  }
  log.Println("Successfully registered application master.")

  // Add resource requests
  memory := int32(128)
  resource := hadoop_yarn.ResourceProto{Memory: &memory}
  rmClient.AddRequest(1, "*", &resource, 1)
 
  // Now call ResourceManager.allocate
  allocateResponse, err := rmClient.Allocate()
  if err == nil {
    log.Println("allocateResponse: ", *allocateResponse)
  }
  log.Println("#containers allocated: ", len(allocateResponse.AllocatedContainers)) 

  // Sleep for a while
  log.Println("Sleeping...")
  time.Sleep(3 * time.Second)
  log.Println("Sleeping... done!")

  // Try to get containers now...
  allocateResponse, err = rmClient.Allocate()
  if err == nil {
    log.Println("allocateResponse: ", *allocateResponse)
  }
  log.Println("#containers allocated: ", len(allocateResponse.AllocatedContainers)) 

  // Unregister with ResourceManager
  log.Println("About to unregister application master.")
  finalStatus := hadoop_yarn.FinalApplicationStatusProto_APP_SUCCEEDED
  err = rmClient.FinishApplicationMaster(&finalStatus, "done", "")
  if err != nil {
    log.Fatal("rmClient.RegisterApplicationMaster ", err)
  }
  log.Println("Successfully unregistered application master.")
}

