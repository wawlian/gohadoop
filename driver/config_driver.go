package main

import (
  "log"
  hadoop_conf "github.com/gohadooprpc/hadoop_common/conf"
)

func main() {
  
  conf, _ := hadoop_conf.NewConfigurationResources([]string{hadoop_conf.CORE_SITE, hadoop_conf.YARN_SITE})

  fsName, _ := conf.Get("fs.default.name", "XXX")
  log.Println("fs.default.name = ", fsName) 

  cores, _ := conf.GetInt("yarn.nodemanager.resource.cpu-cores", -1)
  log.Println("yarn.nodemanager.resource.cpu-cores = ", cores) 
}

