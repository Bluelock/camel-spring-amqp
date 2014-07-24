# camel-spring-amqp

## Introduction

An [Apache Camel](http://camel.apache.org/) Component that will natively communicate with a [RabbitMQ](http://www.rabbitmq.com/ "RabbitMQ") broker. 
This is implemented using Spring's AMQP project, so it should ultimately become vendor-agnostic.

## Usage

The simplest Fanout exchange can be defined as:

`spring-amqp:<Exchange Name>`

`spring-amqp:<Exchange Name>:<Queue Name>`

Here a simple message producer will send a message to Exchange Name, and a simple consumer will bind the Exchange Name to the Queue Name.

If you wish to use a routing key, URIs have the structure: 

`spring-amqp:<Exchange Name>:<Queue Name>:<Routing Key>?type=<Exchange Type>`

The routing key is optional, but Queue Name and Exchange Name are required for consumers. Just the Exchange Name is required for producers.

Producers can also defer the routing key to the message header, where the ROUTING_KEY header could be set to the appropriate routing key.

Producers can override the exchange name specified in the URI by setting the EXCHANGE_NAME Camel message header.

Options to the URI include the exchange type, which defaults to direct if none is specified.

For header based exchanges, the URI is similar but name/value pairs can be specified in place of the routing key. For example:

`spring-amqp:myExchange:qName:cheese=gouda?type=headers`

This example will fetch all messages where a header named "cheese" has the value of "gouda." You can also add additional name/value pairs:

`spring-amqp:myExchange:qName:cheese=gouda&fromage=jack?type=headers`

Which will create a binding for headers where "cheese" has the value of "gouda" AND "fromage" has the value of "jack." You can also choose to create an OR relationship:

`spring-amqp:myExchange:qName:cheese=gouda|fromage=jack?type=headers`

## Additional Settings and Properties

Additional properties can be added to the endpoint as URI parameters. For example, to create a topic exchange that is non-exclusive and not durable:

`spring-amqp:myExchange_10:writeQueue:write.*?type=topic&durable=false&autodelete=true&exclusive=false`

Parameters available include:

<table>
    <tr>
        <td>prefetchCount</td>
        <td>How many messages a consumer should pre-fetch</td>
    </tr>
    <tr>
        <td>concurrentConsumers</td>
        <td>The number of concurrent threads that will consume messages from a queue</td>
    </tr>
    <tr>
        <td>transactional</td>
        <td>Mark messages coming to/from this endpoint as transactional</td>
    </tr>
    <tr>
        <td>autodelete</td>
        <td>Allow this endpoint to be automagically deleted from the broker once it is gone</td>
    </tr>
    <tr>
        <td>durable</td>
        <td>Make queues and exchanges created by this endpoint persistent</td>
    </tr>
    <tr>
        <td>type</td>
        <td>One of the AMQP exchange types: direct, fanout, headers, or topic. Defaults to direct.</td>
    </tr>
    <tr>
        <td>exclusive</td>
        <td>Mark this endpoint as an exclusive point for message exchanges</td>
    </tr>
    <tr>
        <td>acknowledgeMode</td>
        <td>Sets the acknowledge mode (NONE, AUTO)</td>
    </tr>
    <tr>
        <td>connection</td>
        <td>Configure a specific connection factory (for systems with multiple AMQP brokers)</td>
    </tr>
    <tr>
        <td>autoReply</td>
        <td>Consumer sends back a response message when ReplyTo header is present in the consumed message. Defaults to true.</td>
    </tr>
</table>

## Spring Integration

The camel-spring-amqp component will attempt to fetch as much information from the application context it sits within. 
For example, if we are using Spring we could issue the following:

	<bean id="messageConverter" class="amqp.spring.converter.XStreamConverter"/>
	<rabbit:connection-factory id="connectionFactory"/>
	<rabbit:template id="amqpTemplate" connection-factory="connectionFactory" message-converter="messageConverter"/>
	<rabbit:admin connection-factory="connectionFactory"/>

The message converter amqp.spring.converter.XStreamConverter is provided by the camel-spring-amqp component; it provides
JSON marshalling using the XStream libraries. If you would rather use the Jackson JSON marshalling (or another
conversion method) provided by the Spring AMQP framework, you can swap out the appropriate message converter class 
in the above example.

## Advanced Message Conversion

You may wish to use JSON marshalling for the majority of your inter-process communication, but may have
the need to do XML marshalling for REST API calls or the like. If you wish you can specify
multiple types of message conversion based on the content type of the message. For example, we may have
messages marshalled to JSON by default but want content types of application/text to just
be printed out as strings. Within the Spring XML DSL you may define:

	<bean id="jsonMessageConverter" class="amqp.spring.converter.XStreamConverter"/>
	<bean id="textMessageConverter" class="amqp.spring.converter.StringConverter"/>
	<bean id="messageConverter" class="amqp.spring.converter.ContentTypeConverterFactory">
	    <property name="converters">
	        <map>
	            <entry key="application/json" value-ref="jsonMessageConverter"/>
	            <entry key="application/xml" value-ref="textMessageConverter"/>
	        </map>
	    </property>
	    <property name="fallbackConverter" ref="jsonMessageConverter"/>
	</bean>

This would allow messages with a "Content-Type" of application/json to be marshalled
with the XStream converter, while messages with a content type of application/xml
will be marshalled into a simple character string. If no content type is specified,
the XStream JSON message converter will be used.

## Downloads and Maven Repository

Release builds of the Camel Spring AMQP Component are hosted within [the Sonatype repository](https://oss.sonatype.org/index.html). 
You can include this component within your Maven POM as:

	<dependency>
	    <groupId>com.bluelock</groupId>
	    <artifactId>camel-spring-amqp</artifactId>
	    <version>1.6.3</version>
	</dependency>

## Limitations

 - Transactions are currently not supported
 - Lifecycle events (e.g. stop, shutdown) need to be refined
 - Unit tests require a running AMQP broker. I may end up creating a VM local Qpid instance as an AMQP broker...

## To-Do

 - Validate with other AMQP brokers (such as Qpid)

## License

This package, the Camel Spring AMQP component is licensed under the Mozilla Public License v2.0. See LICENSE for details.
