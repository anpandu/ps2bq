package common

import (
	"io/ioutil"
	"os"

	log "github.com/sirupsen/logrus"
)

func ReadFileToString(fullpath string) string {
	file, err := os.Open(fullpath)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()
	result, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatal(err)
	}
	return string(result)
}
