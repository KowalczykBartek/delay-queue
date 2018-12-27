[![](https://img.shields.io/badge/unicorn-approved-ff69b4.svg)](https://www.youtube.com/watch?v=9auOCbH5Ns4)
![][license img]

App that allows to schedule an message processing for some predefined delay. Main idea is based on https://redislabs.com/ebook/part-2-core-concepts/chapter-6-application-components-in-redis/6-4-task-queues/6-4-2-delayed-tasks/ and wrapped with some Netty code.


## how
Just start an app (not gradle task :/) from App.java and prepare Redis instance on default port 6379 (and localhost of course)

Then just make a curl
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
