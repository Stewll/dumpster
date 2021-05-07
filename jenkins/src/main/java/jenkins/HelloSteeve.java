package jenkins;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.mqtt5.Mqtt5BlockingClient;
import com.hivemq.client.mqtt.mqtt5.Mqtt5Client;
import com.hivemq.client.mqtt.mqtt5.message.connect.Mqtt5Connect;
import com.hivemq.client.mqtt.mqtt5.message.connect.connack.Mqtt5ConnAck;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5PublishResult;

import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@NoArgsConstructor
public class HelloSteeve {

	private String host = "broker.hivemq.com";
	private String clientId = UUID.randomUUID().toString();

	private Mqtt5BlockingClient client = Mqtt5Client.builder().identifier(clientId).serverHost(host).buildBlocking();

	public void testing() throws InterruptedException {
		String topic = "kaweechelchen";
		// String message = "random";

		// Mqtt5ConnAck connAckMessage = client.connect();
		Mqtt5Connect connectMessage = Mqtt5Connect.builder().cleanStart(false).build();
		Mqtt5ConnAck connAckMessage = /*client.connectWith().send();*/ client.connect(connectMessage);
		System.out.println(connAckMessage);

		// subscribeToTopic(topic);

		subscribeToTopic("kaweechelchen/#");

		for (Integer i = 10; i > 0; i--) {
			publishMessage(topic, topic + " " + i.toString(), MqttQos.EXACTLY_ONCE);
			publishMessage("kaweechelchen/schnuddelhong",
					"kaweechelchen/schnuddelhong" + " " + (Integer.valueOf(-i)).toString(), MqttQos.AT_LEAST_ONCE);
			Thread.sleep(1000);
		}

	}

	public void subscribeToTopic(String topic) {

		client.toAsync().subscribeWith().topicFilter(topic).qos(MqttQos.AT_LEAST_ONCE).callback(publish -> {
			System.out.println("Received message on topic " + publish.getTopic() + ": "
					+ new String(publish.getPayloadAsBytes(), StandardCharsets.UTF_8));
		}).send();

	}

	public void publishMessage(String topic, String message, MqttQos qos) {
		Mqtt5PublishResult publishResult = client.publishWith().topic(topic).payload(message.getBytes()).qos(qos)
				.send();

		/*Flowable<Mqtt5PublishResult> publishResult = client.toRx().publish(Flowable.range(0, 100)
				.map(i -> Mqtt5Publish.builder().topic(topic + i).payload(message.getBytes()).retain(false).build()));*/

		System.out.println(publishResult);
	}

	public static void main(String[] args) throws InterruptedException {
		HelloSteeve helloSteeve = new HelloSteeve();
		helloSteeve.testing();

	}

}
