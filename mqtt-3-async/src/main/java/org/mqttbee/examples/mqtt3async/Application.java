package org.mqttbee.examples.mqtt3async;

import org.mqttbee.api.mqtt.MqttClient;
import org.mqttbee.api.mqtt.mqtt3.Mqtt3AsyncClient;

import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.IntStream;

/**
 * This application demonstrates the simplest use of the MQTT 3 Async client of mqtt-bee by using defaults wherever
 * possible.
 *
 * Each operations returns a completable future, which is used to react to success or failure by either logging and
 * aborting in the failure case, or progressing to the next step on success.
 *
 * The flow is:
 * <ol>
 *     <li>Build the client</li>
 *     <li>Connect the client</li>
 *     <li>Subscribe to test/topic</li>
 *     <li>Publish five messages to test/topic</li>
 *     <li>Wait for all five messages to be received</li>
 *     <li>Disconnect client</li>
 * </ol>
 */
public class Application {

    private static final Logger LOGGER = Logger.getLogger(Application.class.getName());

    public static void main(String... args) {
        LOGGER.info("Starting scenario.");
        buildClient();
    }

    private static void buildClient() {
        Mqtt3AsyncClient client = MqttClient.builder()
                .useMqttVersion3()
                .identifier(UUID.randomUUID().toString())
                .buildAsync();
        LOGGER.info(() -> "Built client: " + client);
        connectClient(client);
    }

    private static void connectClient(Mqtt3AsyncClient client) {
        client.connect()
                .whenComplete((mqtt3ConnAck, throwable) -> {
                    if (throwable != null) {
                        LOGGER.log(Level.SEVERE, "Unable to connect to broker.", throwable);
                    } else {
                        LOGGER.info("Connected.");
                        subscribe(client);
                    }
                });
    }

    private static void subscribe(Mqtt3AsyncClient client) {
        CountDownLatch countDownLatch = new CountDownLatch(5);
        client.subscribeWith()
                .topicFilter("test/topic")
                .callback(mqtt3Publish -> {
                    LOGGER.info("Received " + new String(mqtt3Publish.getPayloadAsBytes()) + " from " + mqtt3Publish.getTopic());
                    countDownLatch.countDown();
                })
                .send()
                .whenComplete((mqtt3SubAck, throwable) -> {
                    if (throwable != null) {
                        LOGGER.log(Level.SEVERE, "Failed to subscribe.", throwable);
                        client.disconnect();
                    } else {
                        LOGGER.info("Subscribed.");
                        publish(client, countDownLatch);
                    }
                });
    }

    private static void publish(Mqtt3AsyncClient client, CountDownLatch countDownLatch) {
        IntStream.range(0, 5).forEach(index -> client.publishWith()
                .topic("test/topic")
                .payload(("Test " + index).getBytes())
                .send()
                .whenComplete((mqtt3Publish, throwable) -> {
                    if (throwable != null) {
                        // handle failure to publish
                    } else {
                        // handle successful publish, e.g. logging or incrementing a metric
                    }
                })
        );

        try {
            countDownLatch.await(5, TimeUnit.SECONDS);
            LOGGER.info("Successfully received all messages.");
        } catch (InterruptedException e) {
            LOGGER.log(Level.SEVERE, "Didn't receive all messages within five seconds.", e);
        } finally {
            client.disconnect();
        }
    }

}
