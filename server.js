var express = require('express');
var data = require('./superhero.json')
var app = express();
var kafka = require('./KafkaProducer')

app.use(function(req, res, next) {
  res.header("Access-Control-Allow-Origin", "*");
  res.header("Access-Control-Allow-Headers", "Origin, X-Requested-With, Content-Type, Accept");
  next();
});

app.get('/', function (req, res) {
    console.log("Got a GET request for the homepage");
    res.send(data);
    
})

app.post('/', function (req, res) {
    console.log("Got a POST request for the homepage");
    kafka.produceMessage(1, "Message from NodeJS")
    res.send('Kafka message posted successfully');
})

app.delete('/del_user', function (req, res) {
    console.log("Got a DELETE request for /del_user");
    res.send('Hello DELETE');
})

app.get('/getSuperHeroList', function (req, res) {
    console.log("Got a GET request for /getSuperHeroList");
    res.send(data);
})

app.get('/searchSuperHero', function (req, res) {
    filteredData = data.filter(function (o) {
        if (o.name == undefined || req.query.name == undefined)
            return;
        return (o.name.toUpperCase() === req.query.name.toUpperCase());
    });
    console.log("Got a GET request for /searchSuperHero");
    res.send(filteredData);
})


app.get('/ab*cd', function (req, res) {
    console.log("Got a GET request for /ab*cd");
    res.send('Page Pattern Match');
})

var server = app.listen(8081, function () {

    var host = server.address().address
    var port = server.address().port

    console.log("Example app listening at http://%s:%s", host, port)
})
