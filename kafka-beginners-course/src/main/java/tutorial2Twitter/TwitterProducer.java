package tutorial2Twitter;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;


public class TwitterProducer {
	
	Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName());
	String consumerKey ="dAARUueAIlYKXSIy1wtPKMBxa";
	String consumerSecret = "WsOK1X8W2SuPelOwO66W8OiZTFNZL2mgWiCeeivOwZlpKbG0Ip";
	String token = "1189955286896906245-CDtOy40mt9H6xkDkQCAMicQskrt03J";
	String secret = "iXzMdqZjmaVA1xIboQk8zcdI4zG7IuJ3Q1PGetKFc8CVT";
	
	public TwitterProducer() {
		
	}
	

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		

			TwitterProducer tp = new TwitterProducer();
			tp.run();
		
		
	}
	
	public void run() {
		
		logger.info("Setup");
		//create a twitter client
		BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);
		Client client = CreateTwitterClient(msgQueue);
		client.connect();
		
		// create a kafka producer
	
		// loop to send tweets to kafka
		
		while (!client.isDone()) {
			  String msg = null;
			  try {
				msg = msgQueue.poll(5,TimeUnit.SECONDS);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				client.stop();
			}
			  if(msg != null) {
				  logger.info(msg);
			  }
			  logger.info("End of application");
			}
	}
	

	
	
	public Client CreateTwitterClient(BlockingQueue<String> msgQueue) {
		
		
		Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
		StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
		// Optional: set up some followings and track terms
		// for the time being we will only follow terms. so lines 44 and 46 are commented
		//List<Long> followings = Lists.newArrayList(1234L, 566788L);
		List<String> terms = Lists.newArrayList("bitcoin");
		//hosebirdEndpoint.followings(followings);
		hosebirdEndpoint.trackTerms(terms);

		// These secrets should be read from a config file
		Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);
		
		ClientBuilder builder = new ClientBuilder()
				  .name("Hosebird-Client-01")                              // optional: mainly for the logs
				  .hosts(hosebirdHosts)
				  .authentication(hosebirdAuth)
				  .endpoint(hosebirdEndpoint)
				  .processor(new StringDelimitedProcessor(msgQueue));
				  // optional: use this if you want to process client events

				Client hosebirdClient = builder.build();
				return hosebirdClient;
	}

}
