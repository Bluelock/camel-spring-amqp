# camel-spring-amqp

## Introduction

An [Apache Camel](http://camel.apache.org/ "Apache Camel") Component that will natively communicate with a [RabbitMQ](http://www.rabbitmq.com/ "RabbitMQ") broker. 
This is implemented using Spring's AMQP project, so it should ultimately become vendor-agnostic.

## Usage

URIs have the structure: 

`spring-amqp:<Exchange Name>:<Queue Name>:<Routing Key>?type=<Exchange Type>&durable=false&autodelete=true&exclusive=false`

The routing key is optional, but Queue Name and Exchange Name are required for consumers.

Options to the URI include the exchange type, which defaults to direct if none is specified.

## Limitations

 - Transactions are currently not supported

 - Header exchange is currently not implemented, but will be shortly

 - The component is currently undergoing integration testing but has not yet been tested in a production environment

 - Unit tests require a running AMQP broker. I may end up creating a VM local Qpid instance as an AMQP broker...

 - TTLs have not yet been implemented

## License

This package, the Camel Spring AMQP component is licensed under the Mozilla Public License v2.0. See LICENSE for details.
