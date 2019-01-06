package mapreduce

import (
	"encoding/json"
	"io/ioutil"
	"os"
)

type KeyValuePairs []KeyValue

//for sort the KeyValue by key
func (keyValuePairs KeyValuePairs) Len() int {
	return len(keyValuePairs)
}

func (keyValuePairs KeyValuePairs) Swap(i, j int) {
	keyValuePairs[i], keyValuePairs[j] = keyValuePairs[j], keyValuePairs[i]
}

func (keyValuePairs KeyValuePairs) Less(i, j int) bool {
	return keyValuePairs[i].Key < keyValuePairs[j].Key
}

func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTask int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	//
	// doReduce manages one reduce task: it should read the intermediate
	// files for the task, sort the intermediate key/value pairs by key,
	// call the user-defined reduce function (reduceF) for each key, and
	// write reduceF's output to disk.
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTask) yields the file
	// name from map task m.
	//
	// Your doMap() encoded the key/value pairs in the intermediate
	// files, so you will need to decode them. If you used JSON, you can
	// read and decode by creating a decoder and repeatedly calling
	// .Decode(&kv) on it until it returns an error.
	//
	// You may find the first example in the golang sort package
	// documentation useful.
	//
	// reduceF() is the application's reduce function. You should
	// call it once per distinct key, with a slice of all the values
	// for that key. reduceF() returns the reduced value for that key.
	//
	// You should write the reduce output as JSON encoded KeyValue
	// objects to the file named outFile. We require you to use JSON
	// because that is what the merger than combines the output
	// from all the reduce tasks expects. There is nothing special about
	// JSON -- it is just the marshalling format we chose to use. Your
	// output code will look something like this:
	//
	// enc := json.NewEncoder(file)
	// for key := ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//
	// Your code here (Part I).
	//
	allKeyValuePairs := make([]KeyValue, 0)

	for mapTask := 0; mapTask < nMap; mapTask++ {
		currMapTaskReduceFilename := reduceName(jobName, mapTask, reduceTask)
		currMapTaskOutput, err := ioutil.ReadFile(currMapTaskReduceFilename)
		if err != nil {
			panic(err)
		}
		currKeyValuePairs := make([]KeyValue, 0)
		if err = json.Unmarshal(currMapTaskOutput, &currKeyValuePairs); err != nil {
			panic(err)
		}
		for _, x := range currKeyValuePairs {
			allKeyValuePairs = append(allKeyValuePairs, x)
		}
	}

	//not need to sort. using a map is a better way
	//sort.Sort(KeyValuePairs(allKeyValuePairs))
	keyValuesMap := make(map[string][]string)
	for _, keyValuePair := range allKeyValuePairs {
		key, value := keyValuePair.Key, keyValuePair.Value
		keyValuesMap[key] = append(keyValuesMap[key], value)
	}

	outputFile, err := os.Create(outFile)
	if err != nil {
		panic(err)
	}

	enc := json.NewEncoder(outputFile)
	for key, value := range keyValuesMap {
		if err := enc.Encode(KeyValue{key, reduceF(key, value)}); err != nil {
			panic(err)
		}
	}

	if err := outputFile.Close(); err != nil {
		panic(err)
	}
}
