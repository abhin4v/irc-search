A search engine for IRC logs. Based on Lucene and Netty. Runs as an HTTP server, exposing REST endpoints for indexing and searching.


Build and Run
----------

First install [Apache Maven](http://maven.apache.org). Then execute this command to run the server at port 9090:

```
mvn package && java -cp target/dependency/*:target/* net.abhinavsarkar.ircsearch.Server 9090
```

Index chat lines
----------

<pre>
$ cat ireq | json_pp
{
   "botName" : "some",
   "chatLines" : [
      {
         "timestamp" : 12312312312,
         "user" : "abhinav",
         "message" : "hi"
      },
      {
         "timestamp" : 12312312312,
         "user" : "abhinav",
         "message" : "hi"
      }
   ],
   "channel" : "as",
   "server" : "ima"
}

$ curl -X POST -d @ireq localhost:9090/index
</pre>

Search
----------

<pre>
$ cat sreq | json_pp
{
   "botName" : "some",
   "page" : 0,
   "query" : "hi user:abhinav",
   "channel" : "as",
   "server" : "ima",
   "pageSize" : 10
}

$ curl -X POST -d @sreq localhost:9090/search -s | json_pp
{
   "page" : 0,
   "query" : "hi user:abhinav",
   "channel" : "as",
   "pageSize" : 10,
   "botName" : "some",
   "chatLines" : [
      {
         "timestamp" : 12312312312,
         "user" : "abhinav",
         "message" : "hi"
      },
      {
         "timestamp" : 12312312312,
         "user" : "abhinav",
         "message" : "hi"
      }
   ],
   "server" : "ima",
   "totalResults" : 24
}
</pre>

