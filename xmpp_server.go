package main

import (
	"errors"
	"fmt"
	"github.com/alfredyang1986/blackmirror/bmerror"
	"github.com/alfredyang1986/blackmirror/bmkafka"
	"github.com/alfredyang1986/blackmirror/bmxmpp"
	"github.com/elodina/go-avro"
	kafkaAvro "github.com/elodina/go-kafka-avro"
	"os"
)

const rawMetricsSchema = `{"namespace": "net.elodina.kafka.metrics","type": "record","name": "XmppCmd","fields": [{"name": "id", "type": "string"},{"name": "reportUser",  "type": "string" },{"name": "msg",  "type": "string" }]}`

func main() {
	//本地测试
	os.Setenv("PKG_CONFIG_PATH", "$PKG_CONFIG_PATH:/usr/lib/pkgconfig/")
	os.Setenv("BM_KAFKA_CONF_HOME", "resource/kafkaconfig.json")
	os.Setenv("BM_XMPP_CONF_HOME", "resource/xmppconfig.json")
	bkc, err := bmkafka.GetConfigInstance()
	if err != nil {
		panic(err.Error())
	}
	topics := []string{"xmpp-topic"}
	bkc.SubscribeTopics(topics, subscribeFunc)
}

func subscribeFunc(a interface{}) {
	fmt.Println("subscribeFunc => ", a)

	bytes := a.([]byte)
	bkc, err := bmkafka.GetConfigInstance()
	if err != nil {
		panic(err.Error())
	}
	decoder := kafkaAvro.NewKafkaAvroDecoder(bkc.SchemaRepositoryUrl)
	decoded, err := decoder.Decode(bytes)
	if err != nil {
		panic(err.Error())
	}
	decodedRecord, ok := decoded.(*avro.GenericRecord)
	if ok {
		reportUser := decodedRecord.Get("reportUser").(string)
		msg := decodedRecord.Get("msg").(string)
		fmt.Println("reportUser=", reportUser, " => msg=", msg)
		bxc, err := bmxmpp.GetConfigInstance()
		bmerror.PanicError(err)
		err = bxc.Forward(reportUser, msg)
		bmerror.PanicError(err)
	} else {
		panic(errors.New("subscribeFunc Error"))
	}

	fmt.Println("subscribeFunc DONE!")
}
