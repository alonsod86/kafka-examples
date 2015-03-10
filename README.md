# kafka-examples
This app contains a brief example of an Apache Kafka consumer/producer controller written in Java

## Gzip compression
The producer is able to send data using gzip compression, reducing the amount of data sent to the broker significantly. 
No further configuration is required within the cluster, only specify the <b>serializer.class</b> property that you will find inside the project and enable gzip just like this:


If you wish gzip compression set 

  `serializer.class=es.cancamusa.kafka.codecs.GzipEncoder`
  
  `kafka.gzip=true`

If not

  `serializer.class=kafka.serializer.StringEncoder`
  
  `kafka.gzip=false`

When using Gzip compression the consumer will be able to decode the message stream using <b>GzipDecoder</b> class.
