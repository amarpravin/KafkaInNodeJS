# NodeJS
Step by step NodeJS tutorials in Express Framework for Beginner

#Description
```
You can post messages using Kafka
```

```
Download latest version of Node.js installable file from https://nodejs.org/en/

To Setup Kafka in you local machine : http://kafka.apache.org/quickstart
```

# Create NodeJS project using npm
Open NodeJS Command Prompt -> In command prompt Go to your directory where you want to create project -> they type below command

```
> npm init
name: superhero
version: 1.0.0
description: api to retrieve superhero details
entry point: server.js
```

It will create a package.json file inside your project folder.

Let's create a Hello World program
Create an server.js file in your project and add below line and save.

```
console.log('hello world');
```

let's run this code.

```
node server
```

you can see "hello world" is printed to the console.

# Installing Express

```
> npm install express --save
```

The above command saves the installation locally in the node_modules directory and creates a directory express inside node_modules.
It will also add express inside the package.json file like this :-

```
  "dependencies": {
    "express": "^4.15.3"
  }
```
  
# install the following important modules along with express −

**body-parser** − This is a node.js middleware for handling JSON, Raw, Text and URL encoded form data.

```
> npm install body-parser --save
```

**cookie-parser** − Parse Cookie header and populate req.cookies with an object keyed by the cookie names.

```
> npm install cookie-parser --save
```

**multer** − This is a node.js middleware for handling multipart/form-data.  

```
> npm install multer --save
```

Replace below code into server.js

```
var express = require('express');
var app = express();

app.get('/', function (req, res) {
   res.send('Hello World');
})

var server = app.listen(8081, function () {
   var host = server.address().address
   var port = server.address().port
   
   console.log("Example app listening at http://%s:%s", host, port)
})
```

Save the above code in a file named server.js and run it with the following command.

```
> node server
```

You will see the following output −

```
Example app listening at http://0.0.0.0:8081
```

Open http://127.0.0.1:8081/ in any browser to see the following result.

You can route basic HTTP Request method like this :-

```
var express = require('express');
var app = express();

app.get('/', function (req, res) {
   console.log("Got a GET request for the homepage");
   res.send('Hello GET');
})

app.post('/', function (req, res) {
   console.log("Got a POST request for the homepage");
   res.send('Hello POST');
})

app.delete('/del_user', function (req, res) {
   console.log("Got a DELETE request for /del_user");
   res.send('Hello DELETE');
})

app.get('/list_user', function (req, res) {
   console.log("Got a GET request for /list_user");
   res.send('Page Listing');
})

app.get('/ab*cd', function(req, res) {   
   console.log("Got a GET request for /ab*cd");
   res.send('Page Pattern Match');
})

var server = app.listen(8081, function () {

   var host = server.address().address
   var port = server.address().port

   console.log("Example app listening at http://%s:%s", host, port)
})
```

Save the above code in a file named server.js and run it with the following command.

```
> node server
```

You will see the following output −

```
Example app listening at http://0.0.0.0:8081
```

Open http://127.0.0.1:8081/ in any browser to see the following result.
Also you can check below url in browser
http://127.0.0.1:8081/
http://127.0.0.1:8081/list_user
http://127.0.0.1:8081/abcd

#To Post the message to Kafka
```
1. Kafka server should be up and running into your machine or server
2. Copy "KafkaProducer.js" into root folder.
3. Import **var kafka = require('./KafkaProducer')** in server.js file
4. Replace post method as below :-
    app.post('/', function (req, res) {
        console.log("Got a POST request for the homepage");
        kafka.produceMessage(1, "Message from NodeJS")
        res.send('Kafka message posted successfully');
        })
```

