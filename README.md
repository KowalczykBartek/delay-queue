[![](https://img.shields.io/badge/unicorn-approved-ff69b4.svg)](https://www.youtube.com/watch?v=9auOCbH5Ns4)
![][license img]

## what it is ?
App that allows to schedule message processing for some predefined delay. Main idea is based on https://redislabs.com/ebook/part-2-core-concepts/chapter-6-application-components-in-redis/6-4-task-queues/6-4-2-delayed-tasks/ and just wrapped with some Netty code.
This is not-production ready (honestly it is nothing-ready) but shows how you can process delayed-messaged in very gently manner.

## how
Prepare Redis instance on default port 6379 (and localhost of  course) and invoke (to build and start).
```
./gradlew buildApp
java -jar ./build/libs/scheduler-server-1.0-SNAPSHOT.jar
```

Then just make a curl to schedule message.
```bash
curl --request PUT \
  --url http://localhost:8080/messages/someId \
  --header 'content-type: application/json' \
  --data '{
	"when" : "2007-12-03T10:15:30.00Z",
	"event" : {
		"message" : "hello"
	}
}'
```
Also, if you prefer to use your browser, Swagger console is available on port 8080.

## example messages consumption
Because looking at logs is not best option to show how something works - you can subscribe on all incoming delayed events:
```
curl --raw 127.0.0.1:9090/subscribe -v -k
```
as response, you will receive all events received from delay-queue application (this endpoint is chunked).

Entire stuff that is responsible for handling this endpoint and delay-messages consumption is stored under "example" package

[license img]:https://img.shields.io/badge/License-Apache%202-blue.svg
