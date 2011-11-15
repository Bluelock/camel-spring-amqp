/**
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 * 
 * Software distributed under the License is distributed on an "AS IS"
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
 * License for the specific language governing rights and limitations
 * under the License.
 * 
 * The Original Code is camel-spring-amqp.
 * 
 * The Initial Developer of the Original Code is Bluelock, LLC.
 * Copyright (c) 2007-2011 Bluelock, LLC. All Rights Reserved.
 */
package amqp.spring.camel.component;

import org.apache.camel.Exchange;
import org.apache.camel.impl.DefaultProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.amqp.support.converter.SimpleMessageConverter;

public class SpringAMQPProducer extends DefaultProducer {
    private static transient final Logger LOG = LoggerFactory.getLogger(SpringAMQPProducer.class);
    
    protected SpringAMQPEndpoint endpoint;
    private org.springframework.amqp.core.Exchange exchange;
    
    public SpringAMQPProducer(SpringAMQPEndpoint endpoint) {
        super(endpoint);
        this.endpoint = endpoint;
    }

    @Override
    public void process(Exchange exchange) throws Exception {
        SpringAMQPMessage message = exchange.getIn(SpringAMQPMessage.class);
        MessageConverter msgConverter;
        if(this.endpoint.getAmqpTemplate() instanceof RabbitTemplate) {
            RabbitTemplate rabbitTemplate = (RabbitTemplate) this.endpoint.getAmqpTemplate();
            msgConverter = rabbitTemplate.getMessageConverter();
        } else {
            LOG.warn("Cannot find RabbitMQ AMQP Template, falling back to simple message converter");
            msgConverter = new SimpleMessageConverter();
        }
        
        if(exchange.getPattern().isOutCapable()) {
            LOG.debug("Synchronous send and request for exchange {}", exchange.getExchangeId());
            Message amqpResponse = this.endpoint.getAmqpTemplate().sendAndReceive(this.endpoint.exchangeName, this.endpoint.routingKey, message.toAMQPMessage(msgConverter));
            SpringAMQPMessage camelResponse = SpringAMQPMessage.fromAMQPMessage(msgConverter, amqpResponse);
            exchange.setOut(camelResponse);
        } else {
            LOG.debug("Synchronous send for exchange {}", exchange.getExchangeId());
            this.endpoint.getAmqpTemplate().send(this.endpoint.exchangeName, this.endpoint.routingKey, message.toAMQPMessage(msgConverter));
        }
    }

    @Override
    public void start() throws Exception {
        super.start();
        
        this.exchange = this.endpoint.createAMQPExchange();
        this.endpoint.amqpAdministration.declareExchange(this.exchange);
        LOG.info("Declared exchange {}", this.exchange.getName());
    }
}
