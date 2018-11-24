package org.mqttbee.examples.mqtt3blocking;

import org.mqttbee.api.mqtt.MqttClient;
import org.mqttbee.api.mqtt.MqttGlobalPublishFilter;
import org.mqttbee.api.mqtt.mqtt3.Mqtt3BlockingClient;
import org.mqttbee.api.mqtt.mqtt3.message.publish.Mqtt3Publish;

import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Demonstrates the use of the blocking API flavour client. The application requires an mqtt broker to be running
 * and listening (without SSL) on port 1883 and not require authentication.
 *
 * With the blocking client each operation will block until successful or throw an exception.
 *
 * The following steps are coded here to demonstrate a simple use case:
 *
 * <ol>
 *     <li>Create the client</li>
 *     <li>Connect to the server</li>
 *     <li>Created a 'publishes' instance, which is used to receive messages</li>
 *     <li>Create a subscription</li>
 *     <li>Publish a message to the subscribed topic</li>
 *     <li>Actually receive the message using the 'publishes'</li>
 *     <li>Disconnect the client</li>
 * </ol>
 *
 * Defaults were used wherever possible. See the JavaDoc and the user guide for details on more advanced features and
 * options.
 */
public class Application {

    private static final Logger LOGGER = Logger.getLogger(Application.class.getName());

    public static final void main(String... args) {
        LOGGER.info("Running.");
        Mqtt3BlockingClient client = MqttClient.builder()
                .useMqttVersion3()
                .identifier(UUID.randomUUID().toString())
                .buildBlocking();

        client.connect();

        try {
            Mqtt3BlockingClient.Mqtt3Publishes publishes = client.publishes(MqttGlobalPublishFilter.ALL_SUBSCRIPTIONS);

            client.subscribeWith()
                    .topicFilter("test/topic")
                    .send();

            client.publishWith()
                    .topic("test/topic")
                    .payload("test".getBytes())
                    .send();

            Mqtt3Publish receivedMessage = publishes.receive(5, TimeUnit.SECONDS).orElseThrow(() -> new RuntimeException("No message received."));
            LOGGER.info("Received: " + receivedMessage);
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Something went wrong.", e);
        } finally {
            client.disconnect();
        }
    }
}
