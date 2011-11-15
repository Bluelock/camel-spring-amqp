# camel-spring-amqp

## Introduction

An [Apache Camel](http://camel.apache.org/ "Apache Camel") Component that will natively communicate with a [RabbitMQ](http://www.rabbitmq.com/ "RabbitMQ") broker. 
This is implemented using Spring's AMQP project, so it should ultimately become vendor-agnostic.

## Usage

URIs have the structure: 

`spring-amqp:<Exchange Name>:<Queue Name>:<Routing Key>?type=<Exchange Type>&durable=false&autodelete=true&exclusive=false`

The routing key is optional, but Queue Name and Exchange Name are required for consumers.

Options to the URI include the exchange type, which defaults to direct if none is specified.

For header based exchanges, the URI is similar but name/value pairs can be specified in place of the routing key. For example:

`spring-amqp:myExchange:qName:cheese=gouda?type=headers`

This example will fetch all messages where a header named "cheese" has the value of "gouda." You can also add additional name/value pairs:

`spring-amqp:myExchange:qName:cheese=gouda&fromage=jack?type=headers`

Which will create a binding for headers where "cheese" has the value of "gouda" AND "fromage" has the value of "jack." You can also choose to create an OR relationship:

`spring-amqp:myExchange:qName:cheese=gouda|fromage=jack?type=headers`

## Limitations

 - Transactions are currently not supported

 - TTLs have not yet been implemented

 - Lifecycle events (e.g. stop, shutdown) need to be refined

 - Unit tests require a running AMQP broker. I may end up creating a VM local Qpid instance as an AMQP broker...

 - The component is currently undergoing integration testing but has not yet been tested in a production environment

## License

This package, the Camel Spring AMQP component is licensed under the Mozilla Public License v2.0. See LICENSE for details.
