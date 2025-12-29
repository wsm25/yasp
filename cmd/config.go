package main

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"yasp/core"
)

func GetConfig(id int) (conf core.NodeConfig) {
	configFile := fmt.Sprintf("../config/node%d.json", id)
	jsonFile, err := os.Open(configFile)
	if err != nil {
		panic(fmt.Sprint("os.Open: ", err))
	}
	defer jsonFile.Close()

	data, err := io.ReadAll(jsonFile)
	if err != nil {
		panic(fmt.Sprint("ioutil.ReadAll: ", err))
	}
	json.Unmarshal([]byte(data), &conf)
	return
}
