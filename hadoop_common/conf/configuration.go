package hadoop_common

import (
  "os"
  "log"
  "strconv"
  "io/ioutil"
  "encoding/xml"
)

const HADOOP_CONF_DIR = "HADOOP_CONF_DIR"
const CORE_SITE = "core-site.xml"
const HDFS_SITE = "hdfs-site.xml"
const YARN_SITE = "yarn-site.xml"
const MAPRED_SITE = "mapred-site.xml"
const CAPACITY_SCHEDULER = "capacity-scheduler.xml"

type Configuration interface {
  Get (key string, defaultValue string) (string, error)
  GetInt (key string, defaultValue int) (int, error)

  Set (key string, value string) (error)
  SetInt (key string, value int) (error)
}

type configurationImpl struct {
  Properties map[string]string 
}

type property struct {
  Name string `xml:"name"`
  Value string `xml:"value"`
}

type hadoopConfiguration struct {
  XMLName xml.Name `xml:"configuration"`
  Properties []property `xml:"property"`
}

func (conf *configurationImpl) Get (key string, defaultValue string) (string, error) {
  value, exists := conf.Properties[key]
  if !exists { 
    return defaultValue, nil
  }
  return value, nil
}

func (conf *configurationImpl) GetInt (key string, defaultValue int) (int, error) {
  value, exists := conf.Properties[key]
  if !exists { 
    return defaultValue, nil
  }
  return strconv.Atoi(value)
}

func (conf *configurationImpl) Set (key string, value string) (error) {
  conf.Properties[key] = value
  return nil
}

func (conf *configurationImpl) SetInt (key string, value int) (error) {
  conf.Properties[key] = strconv.Itoa(value)
  return nil
}

func NewConfiguration () (Configuration, error) {
  // $HADOOP_CONF_DIR/core-site.xml
  return NewConfigurationResources([]string{CORE_SITE})
}

func NewConfigurationResources (resources []string) (Configuration, error) {
  c := configurationImpl {Properties: make(map[string]string)}

  for _, resource := range resources {
    conf, err := os.Open(os.Getenv(HADOOP_CONF_DIR) + string(os.PathSeparator) + resource)
    if err != nil {
      log.Fatal("Couldn't open core-site.xml", err)
      return nil, err
    }
    confData, err := ioutil.ReadAll(conf)
    if err != nil {
      log.Fatal("Couldn't read core-site.xml", err)
      return nil, err
    }
    defer conf.Close()

    // Parse
    var hConf hadoopConfiguration
    err = xml.Unmarshal(confData, &hConf)
    if err != nil {
      log.Fatal("Couldn't parse core-site.xml", err)
      return nil, err
    }

    // Save into configurationImpl
    for _, kv := range hConf.Properties {
      c.Set(kv.Name, kv.Value)
    }
  }

  return &c, nil
}


