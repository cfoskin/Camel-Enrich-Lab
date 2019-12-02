package org.fuse.usecase;

import org.apache.camel.Exchange;
import org.apache.camel.processor.aggregate.AggregationStrategy;
import org.apache.cxf.message.MessageContentsList;
import org.globex.Account;
import org.globex.Company;
import org.globex.CorporateAccount;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;

/**
 * Aggregator implementation which extract the id and salescontact
 * from CorporateAccount and update the Account
 */
public class AccountAggregator implements AggregationStrategy {

    @Override
    public Exchange aggregate(Exchange oldExchange, Exchange newExchange) {
    	// EXCLUDE-BEGINs

    	if (oldExchange == null) {
            return newExchange;
        }

        if (oldExchange.getIn().getBody() instanceof String)  {
        	return newExchange;
        }
//        
    	Account acc = oldExchange.getIn().getBody(Account.class);
    	CorporateAccount ca = newExchange.getIn().getBody(CorporateAccount.class);
    	
    	
    	acc.setSalesRepresentative(ca.getSalesContact());
    	acc.setClientId(ca.getId());
    	
    	return oldExchange;
    }
    
}