 
// Kafka configuration
var kafka = require('kafka-node')
var Producer = kafka.Producer
// instantiate client with as connectstring host:port for  the ZooKeeper for the Kafka cluster
var client = new kafka.Client("localhost:2181/")
 
// name of the topic to produce to
var kafkaTopic = "my-replicated-topic";
 
    KeyedMessage = kafka.KeyedMessage,
    producer = new Producer(client),
    km = new KeyedMessage('key', 'message'),
    ProducerReady = false ;
 
producer.on('ready', function () {
    console.log("Producer is ready");
    ProducerReady = true;
});
  
producer.on('error', function (err) {
  console.error("Problem with producing Kafka message "+err);
})
 
 

exports.produceMessage = function(Code, Message) {
    KeyedMessage = kafka.KeyedMessage,
    kafkaKM = new KeyedMessage(Code, JSON.stringify(Message)),
    payloads = [
        { topic: kafkaTopic, messages: kafkaKM, partition: 0 },
    ];
    if (ProducerReady) {
    producer.send(payloads, function (err, data) {
        console.log(data);
    });
    } else {
        // the exception handling can be improved, for example schedule this message to be tried again later on
        console.error("sorry, Producer is not ready yet, failed to produce message to Kafka.");
    }
 
}