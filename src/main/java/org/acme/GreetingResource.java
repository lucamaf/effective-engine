package org.acme;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.TopicListing;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

@Path("/hello")
public class GreetingResource {
    private static final Logger LOG = Logger.getLogger(GreetingResource.class);
    private HashMap<String,String> conf = new HashMap<String,String>();
    @ConfigProperty(name = "bootstrap.servers") 
    String servers;

    @ConfigProperty(name = "client.id") 
    String client;

    @GET
    @Produces(MediaType.TEXT_PLAIN)
    public String hello() {
        // First we need to initialize Kafka properties
        //properties.put("bootstrap.servers","localhost:9092");
        //properties.put("client.id","java-admin-client");
        LOG.info("***** Topics *****");
        conf.put("bootstrap.servers",servers);
        conf.put("client.id",client);
        printTopicDetails(conf);
        LOG.info("***** Topics Description *****");
        printTopicDescription(conf);
        return "Hello RESTEasy";
    }

    

    private static void printTopicDetails(HashMap conf) {
        Collection <TopicListing> listings;
        // Create  an AdminClient using the properties initialized earlier
        try (AdminClient client = AdminClient.create(conf)) {
            listings = getTopicListing(client, true);
            listings.forEach(
                topic -> System.out.println("Name: " + topic.name() + ", isInternal: " + topic.isInternal()));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
            LOG.warn("Failed to get topic list {0}", e.getCause());
        }
    }
    private static void printTopicDescription(HashMap conf) {
        Collection <TopicListing> listings;
        // Create  an AdminClient using the properties initialized earlier
        try (AdminClient client = AdminClient.create(conf)) {
            listings = getTopicListing(client, false);
            List <String> topics = listings.stream().map(TopicListing::name)
                .collect(Collectors.toList());
            DescribeTopicsResult result = client.describeTopics(topics);
            result.values().forEach((key, value) -> {
                try {
                    System.out.println(key + ": " + value.get());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } catch (ExecutionException e) {
                    LOG.warn("Failed to execute", e.getCause());
                }
            });
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
            LOG.warn("Failed to get topic list", e.getCause());
        }
    }
    private static Collection <TopicListing> getTopicListing(AdminClient client, boolean isInternal)
    throws InterruptedException, ExecutionException {
        ListTopicsOptions options = new ListTopicsOptions();
        options.listInternal(isInternal);
        return client.listTopics(options).listings().get();
    }
}