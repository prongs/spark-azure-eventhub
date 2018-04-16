# Spark file format for reading eventhub capture output in Azure

In azure world, Eventhub is the equivalent of `Kafka`. 
Eventhub capture writes data in avro format with the following schema:

```json
{
    "type":"record",
    "name":"EventData",
    "namespace":"Microsoft.ServiceBus.Messaging",
    "fields":[
                 {"name":"SequenceNumber","type":"long"},
                 {"name":"Offset","type":"string"},
                 {"name":"EnqueuedTimeUtc","type":"string"},
                 {"name":"SystemProperties","type":{"type":"map","values":["long","double","string","bytes"]}},
                 {"name":"Properties","type":{"type":"map","values":["long","double","string","bytes"]}},
                 {"name":"Body","type":["null","bytes"]}
             ]
}
```
The field `Body` is supposed to contain data in as `byte[]`. 

Spark supports reading avro data via a fileformat provided by databricks. 
If we try to read Eventhub Capture output using that, the resulting dataframe 
would have a field `Body` of binary type. What we would like is to have
an ability to plug in a serialization/deserialization library around the 
field `Body` and have the spark file format understand it. `Protobuf`, `Thrift` etc. 
would be examples of such libraries. Both of them generate java classes
and `Reflection` could be used for understanding schema. To make dataframes, 
we need a `parser` to convert `byte[]` to a `Row`, preferably without reflection.

Currently this library supports only `protobuf`. Example usage would be

```scala
val dataframe = spark.read.option("proto.class.name", protoObject.getClass.getName).azureEventhub(file.getAbsolutePath)
``` 

Needless to say, the jar that contains the proto classes, needs to be in classpath. 

There are two modules: one for core logic, one for tests. I had to keep tests in a 
separate module since I didn't want my compiled proto classes to appear in the jar. 

Future plans would be 

* Support Thrift
* Support writing via spark (currently only reading is supported)
