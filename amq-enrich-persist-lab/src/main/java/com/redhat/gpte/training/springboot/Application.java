/*
 * Copyright 2016 Red Hat, Inc.
 * <p>
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 */
package com.redhat.gpte.training.springboot;

import org.apache.activemq.jms.pool.PooledConnectionFactory;
import org.apache.camel.Exchange;
import org.apache.camel.ExchangePattern;
import org.apache.camel.LoggingLevel;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.amqp.AMQPComponent;
import org.apache.camel.model.dataformat.JsonLibrary;
import org.apache.camel.processor.aggregate.AggregationStrategy;
import org.apache.qpid.jms.JmsConnectionFactory;
import org.fuse.usecase.AccountAggregator;
import org.fuse.usecase.ProcessorBean;
import org.globex.Account;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ImportResource;

@SpringBootApplication
// load regular Spring XML file from the classpath that contains the Camel XML DSL
@ImportResource({"classpath:spring/camel-context.xml"})
public class Application extends RouteBuilder {

    /**
     * A main method to start this application.
     */
    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }
 
	@Override
	public void configure() throws Exception {
       //Route 1
       from("amqp:queue:accountQueue")
        .setExchangePattern(ExchangePattern.InOnly)
        .unmarshal().json(JsonLibrary.Jackson, Account.class)//marshall it back to a class (Account)
       	.log(LoggingLevel.INFO, "org.fuse.usecase", "Unmarshalled: " + simple("${body}"))
        .multicast(new AccountAggregator()).parallelProcessing()//parallel - multicasts occurs concurrently -  caller thread will still wait until all messages has been fully processed, before it continues.
       	.log(LoggingLevel.INFO, "org.fuse.usecase", "Inside Multicast: " + simple("${body}"))// everything after .multicast will be processed until you call end()
        .to("direct:callRestEndpoint", "direct:callWSEndpoint")
        .end()//HERE it ends It will process both routes and the log before continuing to direct:insertDB
       	.log(LoggingLevel.INFO, "org.fuse.usecase", "After Multicast: " + simple("${body}"))
       	.to("direct:insertDB");
       
      	from("direct:insertDB")
       	.log(LoggingLevel.INFO, "org.fuse.usecase", "Before insert to DB:" + simple("${body}"))
       	.bean(new ProcessorBean(), "defineNamedParameters")//This is the method in the Processor bean that is called and body is the Account object - see method
       	.to("sql:INSERT INTO USECASE.T_ACCOUNT(CLIENT_ID,SALES_CONTACT,COMPANY_NAME,COMPANY_GEO,COMPANY_ACTIVE,CONTACT_FIRST_NAME,CONTACT_LAST_NAME,CONTACT_ADDRESS,CONTACT_CITY,CONTACT_STATE,CONTACT_ZIP,CONTACT_PHONE,CREATION_DATE,CREATION_USER)                          VALUES                          (:#CLIENT_ID,:#SALES_CONTACT,:#COMPANY_NAME,:#COMPANY_GEO,:#COMPANY_ACTIVE,:#CONTACT_FIRST_NAME,:#CONTACT_LAST_NAME,:#CONTACT_ADDRESS,:#CONTACT_CITY,:#CONTACT_STATE,:#CONTACT_ZIP,:#CONTACT_PHONE,:#CREATION_DATE,:#CREATION_USER);")
       	.log(LoggingLevel.INFO, "org.fuse.usecase", "Final Result:" + simple("${body}"));

      	//This Route is sending the Account object to the rest endpoint where it will be enriched eg: by changing GEO from NA to North America
      	from("direct:callRestEndpoint")
      	.log(LoggingLevel.INFO, "org.fuse.usecase", "Recieved Broadcast to Rest Service: " + simple("${body}"))
        .marshal().json(JsonLibrary.Jackson) // need to change it to JSON for sending via HTTP
        .setHeader("Accept", constant("application/json"))
        .setHeader(Exchange.CONTENT_TYPE, constant("application/json"))
        .setHeader("CamelHttpMethod", constant("POST"))
        .setHeader("CamelCxfRsUsingHttpAPI", constant("True"))
        .setHeader(Exchange.HTTP_URI, constant("http4://localhost:8082/rest/customerservice/enrich")) //overriding the URI as it uses the original one /customers/...etc
      	.log(LoggingLevel.INFO, "org.fuse.usecase", "Sending Broadcast to Rest Service: " + simple("${body}"))
        .to("http4://localhost:8082/rest/customerservice/enrich")//Note HTTP4
      	.log(LoggingLevel.INFO, "org.fuse.usecase", "Rest Service Response" + simple("${body}"));
       
      	//This Route is sending the Account object to the SOAP endpoint where it will be enriched eg: by adding Sales Contact 
      	// this happens in CustomerWSImpl.java -> ca.setSalesContact(getRandomSales(sales));
       from("direct:callWSEndpoint")
      	.log(LoggingLevel.INFO, "org.fuse.usecase", "Recieved Broadcast to SOAP Service: " + simple("${body}"))
        .to("cxf:bean:customerWebService")
     	.log(LoggingLevel.INFO, "org.fuse.usecase", "SOAP " + simple("${body}"));

	}
	
	   @Bean(name = "amqp-component")
	    AMQPComponent amqpComponent(AMQPConfiguration config) {
	        JmsConnectionFactory qpid = new JmsConnectionFactory(config.getUsername(), config.getPassword(), "amqp://"+ config.getHost() + ":" + config.getPort());
	        //qpid.setTopicPrefix("topic://");

	        PooledConnectionFactory factory = new PooledConnectionFactory();
	        factory.setConnectionFactory(qpid);

	        return new AMQPComponent(factory);
	    }

}
