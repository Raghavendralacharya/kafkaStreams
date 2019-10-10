package kafka.kafkaStreams;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import java.util.regex.Pattern;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Suppressed;
import org.apache.kafka.streams.kstream.Suppressed.BufferConfig;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.WindowStore;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class sampleKafkaStreamAggregation {
	// private static final Logger logger = LogManager.getRootLogger();
	// Logger logger = Logger.getLogger(MyClass.class.getName());
	static final String inputTopic = "plaintextinput";
	static final String outputTopic = "wordcountoutput1";
	protected static final int LOG_EVENT = 0;
	static JSONParser parser = new JSONParser();

	public Object clone() throws CloneNotSupportedException {
		return super.clone();
	}

	public static void main(final String[] args) {
		try {
			final Properties props = new Properties();
			props.put(StreamsConfig.APPLICATION_ID_CONFIG, "StreamingWordCount");
			props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
			props.put(StreamsConfig.STATE_DIR_CONFIG, "state-store");
			props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
			props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
			props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10 * 1000);
			props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);

			// logger.info("Start Reading Messages");
			StreamsBuilder streamsBuilder = new StreamsBuilder();
			// createWordCountStream(streamsBuilder);
			// windowStream(streamsBuilder);
			System.out.println("Stream data");
			filterTopic(streamsBuilder);
			// windowData(streamsBuilder);
			// joinTopic(streamsBuilder);
			@SuppressWarnings("resource")
			KafkaStreams streams = new KafkaStreams(streamsBuilder.build(), props);
			streams.start();
			Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
		} catch (Exception e) {
			System.out.println(e);
		}
	}

	static void createWordCountStream(final StreamsBuilder builder) {
		final Serde<String> stringSerde = Serdes.String();
		final Serde<Long> longSerde = Serdes.Long();
		final KStream<String, String> textLines = builder.stream(inputTopic);

		final Pattern pattern = Pattern.compile("\\W+", Pattern.UNICODE_CHARACTER_CLASS);

		final KTable<String, Long> wordCounts = textLines
				.flatMapValues(value -> Arrays.asList(pattern.split(value.toLowerCase()))).groupBy((key, word) -> word)
				.count();
		wordCounts.toStream().peek((k, v) -> System.out.println("Key = " + k + " Value = " + v));
		// Write the `KTable<String, Long>` to the output topic.

		wordCounts.toStream().to(outputTopic, Produced.with(stringSerde, null));
	}

	@SuppressWarnings("deprecation")
	static void windowStream(final StreamsBuilder builder) {
		try {
			// long windowSizeMs = TimeUnit.MINUTES.toMillis(5);
			final Pattern pattern = Pattern.compile("\\W+", Pattern.UNICODE_CHARACTER_CLASS);
			final KStream<String, String> textLines = builder.stream(inputTopic,
					Consumed.with(Serdes.String(), Serdes.String()));
			KTable<Windowed<String>, Long> wordcount = textLines
					.flatMapValues(value -> Arrays.asList(pattern.split(value.toLowerCase())))
					.groupBy((key, word) -> word)
					.windowedBy(TimeWindows.of(Duration.ofSeconds(5).plus(Duration.ofMinutes(1)))).count();

			wordcount.toStream().peek((k, v) -> System.out.println("Key = " + k.key() + " Value = " + v.toString()));
			wordcount.toStream((k, v) -> k.key()).to(outputTopic);

//			final KTable<Windowed<String>, Long> aggregated = textLines
//					.groupByKey()
//					.reduce((aggValue, newValue) -> aggValue + newValue,
//					        TimeWindows.of(TimeUnit.MINUTES.toMillis(2))
//					                   .until(TimeUnit.DAYS.toMillis(1) /* keep for one day */), 
//					        "queryStoreName");
		} catch (Exception e) {
			System.out.println("error" + e);
		}
	}

	static void filterTopic(final StreamsBuilder builder) {
		final Serde<String> stringSerde = Serdes.String();
		final Serde<Long> longSerde = Serdes.Long();
		try {
			System.out.println("filter data");
			final KStream<String, String> textLines = builder.stream(inputTopic,
					Consumed.with(Serdes.String(), Serdes.String()));
//			textLines.foreach(new ForeachAction<String, String>() {
//				public void apply(String key, String value) {
//					System.out.println(key + ": " + value);
//				}
//			});
			final KStream<String, String> FatalEvent = textLines.filter(new Predicate<String, String>() {
				@Override
				public boolean test(String key, String value) {
					try {
						JSONObject json = (JSONObject) parser.parse(value);
						if (json.get("log_event").equals("FATAL")) {
							System.out.println("return true");
							return true;
						} else {
							System.out.println("return false" + json.get("log_event"));
							return false;
						}
					} catch (ParseException e) {
						// TODO Auto-generated catch block
						System.out.println("catch block executed");
						e.printStackTrace();
						return false;
					}
				}
			});
			FatalEvent.peek((k, v) -> System.out.println("Key = " + k + "Value = " + v));
			KTable<Windowed<String>, Long> Fatalcount = FatalEvent.groupBy((key, word) -> "value")
					// .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
					.windowedBy(TimeWindows.of(Duration.ofMinutes(3)).advanceBy(Duration.ofMinutes(1))
							.grace(Duration.ofMillis(0)))
					.count(Materialized.<String, Long, WindowStore<Bytes, byte[]>>as("count").withCachingDisabled()
							.withKeySerde(Serdes.String()))
					.suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()));
			
//			final KStream<Windowed<String>, Long> FatalStream = Fatalcount.toStream();
//			Fatalcount.mapValues(new ValueMapper<String, String>() {
//				@Override
//				public String apply(String s) {
//					JSONObject obj = new JSONObject();
//					obj.put("count", s);
//					return obj.toString();
//				}
//			});
			final KStream<Object, Object> FatalStream = Fatalcount.toStream()
					.map((key, value) -> new KeyValue<>(keyMapper(key), valueMapper(value)));
			// Fatalcount.toStream().mapValues(v -> );
			//final KStream<Object, Object> FatalStream = Fatalcount.toStream().map((key, value) -> new KeyValue<>(key.key(), "Count for product with ID 123: " + value));
			FatalStream.peek((k, v) -> System.out.println("Key = " + k.toString() + "Value = " + v.toString()));
			
			FatalStream.to(outputTopic);
			// .map((k, v) -> new KeyValue<>(k.key(), v))

		} catch (Exception e) {
			System.out.println("error" + e);
		}
	}

	@SuppressWarnings("unchecked")
	static String valueMapper(long s) {
		JSONObject obj = new JSONObject();
		obj.put("count", s);
		return obj.toString();

	}
	@SuppressWarnings("unchecked")
	static String keyMapper(Windowed<String> key) {
		JSONObject obj = new JSONObject();
		obj.put("window_start", key.window().start());
		obj.put("window_end",key.window().end());
		obj.put("window_start_ins", key.window().startTime());
		obj.put("window_end_ins",key.window().endTime());
		obj.put("key", key.key());
		//obj.put("topic",)
		return obj.toString();

	}
	
	static void windowData(final StreamsBuilder builder) {
		final Serde<String> stringSerde = Serdes.String();
		final Serde<Long> longSerde = Serdes.Long();
		try {
			System.out.println("filter data");
			final KStream<String, String> textLines = builder.stream(inputTopic);
//			textLines.foreach(new ForeachAction<String, String>() {
//				public void apply(String key, String value) {
//					System.out.println(key + ": " + value);
//				}
//			});
			final KStream<String, String> FatalEvent = textLines.filter(new Predicate<String, String>() {
				@Override
				public boolean test(String key, String value) {
					try {
						JSONObject json = (JSONObject) parser.parse(value);
						if (json.get("log_event").equals("FATAL")) {
							System.out.println("return true");
							return true;
						} else {
							System.out.println("return false" + json.get("log_event"));
							return false;
						}
					} catch (ParseException e) {
						// TODO Auto-generated catch block
						System.out.println("catch block executed");
						e.printStackTrace();
						return false;
					}
				}
			});
			FatalEvent.peek((k, v) -> System.out.println("Key = " + k + "Value = " + v));
//			KTable<Windowed<String>, Long> Fatalcount = FatalEvent
//					.groupBy((key, word) -> "value")
			// .windowedBy(TimeWindows.of(Duration.ofMinutes(3)).advanceBy(Duration.ofMinutes(1))).count();

//			KTable<Windowed<String>, Long> Fatalcount = FatalEvent
//					.groupBy((key, word) -> "value").windowedBy(TimeWindows.of(Duration.ofMinutes(3)))
//		    .aggregate(
//		      () -> 0L, /* initializer */
//		    	(aggKey, newValue, aggValue) -> aggValue + newValue, /* adder */
//		      Materialized.<String, Long, WindowStore<Bytes, byte[]>>as("time-windowed-aggregated-stream-store") /* state store name */
//		        .withValueSerde(Serdes.Long())); /* serde for aggregate value */

			final KTable<String, String> Fatalcount = textLines.groupBy((key, word) -> "value")
					.reduce((aggValue, newValue) -> aggValue + newValue);

			Fatalcount.toStream().peek((k, v) -> System.out.println("Key = " + k + "Value = " + v));
			Fatalcount.toStream((k, v) -> k.toString()).to(outputTopic);
		} catch (Exception e) {
			System.out.println("error" + e);
		}

	}

	static void joinTopic(final StreamsBuilder builder) {
		try {
			System.out.println("join Topic");
			ArrayList<String> inputTopiclist = new ArrayList<String>();
			inputTopiclist.add("plaintextinput");
			inputTopiclist.add("plaintextinput1");
			final KStream<String, String> textLines = builder.stream(inputTopiclist);
			textLines.foreach(new ForeachAction<String, String>() {
				public void apply(String key, String value) {
					System.out.println(key + ": " + value);
				}
			});
			textLines.to(outputTopic);
		} catch (Exception e) {
			System.out.println("error" + e);
		}
	}

}
