package io.example.twitter.kafka;

import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.commons.codec.digest.DigestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.PreDestroy;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static com.google.common.collect.Lists.newArrayList;

@Component
public class TwitterProducer implements ApplicationListener<ContextRefreshedEvent> {

    @Value("${twitter.consumerKey}")
    private String consumerKey = System.getenv("TW_CONSUMER_KEY");
    @Value("${twitter.consumerSecret}")
    private String consumerSecret = System.getenv("TW_CONSUMER_SECRET");
    @Value("${twitter.accessToken}")
    private String accessToken = System.getenv("TW_ACCESS_TOKEN");
    @Value("${twitter.accessTokenSecret}")
    private String accessTokenSecret = System.getenv("TW_ACCESS_TOKEN_SECRET");

    private final Logger logger = LoggerFactory.getLogger(TwitterProducer.class);

    @Value("${kafka.topic}")
    private String topic;

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    private Thread thread;
    private boolean stop;

    public TwitterProducer() {
        thread = new Thread(() -> {
            BlockingQueue<String> tweetQueue = new LinkedBlockingQueue<>(100000);
            Client twitterClient = createTwitterClient(tweetQueue);
            twitterClient.connect();

            try {
                while (!stop && !twitterClient.isDone()) {
                    String tweet = tweetQueue.poll(5, TimeUnit.SECONDS);

                    if (tweet != null) {
                        kafkaTemplate.send(topic, tweet);
                        logger.info("[x] Sending twitter tweet: " + DigestUtils.md5Hex(tweet));
                    }
                }
            } catch (Exception ex) {
                logger.error("[x] Twitter client failed", ex);
                twitterClient.stop();
                stop = true;
            }
        });
    }

    @Override
    public void onApplicationEvent(ContextRefreshedEvent event) {
        if (!thread.isAlive()) {
            thread.run();
        }
    }

    @PreDestroy
    public void stop() {
        this.stop = true;
    }

    private Client createTwitterClient(BlockingQueue<String> msgQueue) {
        String consumerKey = System.getenv("TW_CONSUMER_KEY");
        String consumerSecret = System.getenv("TW_CONSUMER_SECRET");
        String accessToken = System.getenv("TW_ACCESS_TOKEN");
        String accessTokenSecret = System.getenv("TW_ACCESS_TOKEN_SECRET");

        Hosts hosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();
        List<Long> followings = newArrayList(1234L, 566788L);
        List<String> terms = newArrayList("kafka", "java", "node", "sport");
        endpoint.followings(followings);
        endpoint.trackTerms(terms);

        Authentication auth = new OAuth1(consumerKey, consumerSecret, accessToken, accessTokenSecret);

        ClientBuilder builder = new ClientBuilder()
                .name("Yoh0xFF-Demo-Client-01")
                .hosts(hosts)
                .authentication(auth)
                .endpoint(endpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        return builder.build();
    }
}
