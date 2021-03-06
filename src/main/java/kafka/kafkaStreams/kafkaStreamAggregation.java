//package kafka.kafkaStreams;
//
//import java.time.Duration;
//import java.util.ArrayList;
//import java.util.Arrays;
//import java.util.Properties;
//import java.util.concurrent.TimeUnit;
//import java.util.logging.Logger;
//import java.util.regex.Pattern;
//
//import org.apache.kafka.common.serialization.Serde;
//import org.apache.kafka.common.serialization.Serdes;
//import org.apache.kafka.streams.KafkaStreams;
//import org.apache.kafka.streams.StreamsBuilder;
//import org.apache.kafka.streams.StreamsConfig;
//import org.apache.kafka.streams.kstream.Consumed;
//import org.apache.kafka.streams.kstream.ForeachAction;
//import org.apache.kafka.streams.kstream.KGroupedStream;
//import org.apache.kafka.streams.kstream.KStream;
//import org.apache.kafka.streams.kstream.KTable;
//import org.apache.kafka.streams.kstream.Predicate;
//import org.apache.kafka.streams.kstream.Produced;
//import org.apache.kafka.streams.kstream.TimeWindows;
//import org.apache.kafka.streams.kstream.Windowed;
//import org.json.simple.JSONObject;
//import org.json.simple.parser.JSONParser;
//import org.json.simple.parser.ParseException;
//
//public class kafkaStreamAggregation {
//	// private static final Logger logger = LogManager.getRootLogger();
//	// Logger logger = Logger.getLogger(MyClass.class.getName());
//	static final String inputTopic = "plaintextinput";
//	static final String outputTopic = "wordcountoutput1";
//	protected static final int LOG_EVENT = 0;
//	static JSONParser parser = new JSONParser();
//
//	public Object clone() throws CloneNotSupportedException {
//		return super.clone();
//	}
//
//	public static void main(final String[] args) {
//		try {
//			final Properties props = new Properties();
//			props.put(StreamsConfig.APPLICATION_ID_CONFIG, "StreamingWordCount");
//			props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
//			props.put(StreamsConfig.STATE_DIR_CONFIG, "state-store");
//			props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
//			props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
//			props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10 * 1000);
//			props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
//
//			// logger.info("Start Reading Messages");
//			StreamsBuilder streamsBuilder = new StreamsBuilder();
//			// createWordCountStream(streamsBuilder);
//			 windowStream(streamsBuilder);
//			System.out.println("Stream data");
//			//filterTopic(streamsBuilder);
//			windowData(streamsBuilder);
//			//joinTopic(streamsBuilder);
//			@SuppressWarnings("resource")
//			KafkaStreams streams = new KafkaStreams(streamsBuilder.build(), props);
//			streams.start();
//			Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
//		} catch (Exception e) {
//			System.out.println(e);
//		}
//	}
//
//	static void createWordCountStream(final StreamsBuilder builder) {
//		final Serde<String> stringSerde = Serdes.String();
//		final Serde<Long> longSerde = Serdes.Long();
//		final KStream<String, String> textLines = builder.stream(inputTopic);
//
//		final Pattern pattern = Pattern.compile("\\W+", Pattern.UNICODE_CHARACTER_CLASS);
//
//		final KTable<String, Long> wordCounts = textLines
//				.flatMapValues(value -> Arrays.asList(pattern.split(value.toLowerCase()))).groupBy((key, word) -> word)
//				.count();
//		wordCounts.toStream().peek((k, v) -> System.out.println("Key = " + k + " Value = " + v));
//		// Write the `KTable<String, Long>` to the output topic.
//
//		wordCounts.toStream().to(outputTopic, Produced.with(stringSerde, null));
//	}
//
//	@SuppressWarnings("deprecation")
//	static void windowStream(final StreamsBuilder builder) {
//		try {
//			// long windowSizeMs = TimeUnit.MINUTES.toMillis(5);
//			final Pattern pattern = Pattern.compile("\\W+", Pattern.UNICODE_CHARACTER_CLASS);
//			final KStream<String, String> textLines = builder.stream(inputTopic,
//					Consumed.with(Serdes.String(), Serdes.String()));
//			KTable<Windowed<String>, Long> wordcount = textLines
//					.flatMapValues(value -> Arrays.asList(pattern.split(value.toLowerCase())))
//					.groupBy((key, word) -> word)
//					.windowedBy(TimeWindows.of(Duration.ofSeconds(5).plus(Duration.ofMinutes(1)))).count();
//			
//			wordcount.toStream().peek((k, v) -> System.out.println("Key = " + k.key() + " Value = " + v.toString()));
//			wordcount.toStream((k, v) -> k.key()).to(outputTopic);
//
////			final KTable<Windowed<String>, Long> aggregated = textLines
////					.groupByKey()
////					.reduce((aggValue, newValue) -> aggValue + newValue,
////					        TimeWindows.of(TimeUnit.MINUTES.toMillis(2))
////					                   .until(TimeUnit.DAYS.toMillis(1) /* keep for one day */), 
////					        "queryStoreName");
//		} catch (Exception e) {
//			System.out.println("error" + e);
//		}
//	}
//
//	static void filterTopic(final StreamsBuilder builder) {
//		final Serde<String> stringSerde = Serdes.String();
//		final Serde<Long> longSerde = Serdes.Long();
//		try {
//			System.out.println("filter data");
//			final KStream<String, String> textLines = builder.stream(inputTopic);
////			textLines.foreach(new ForeachAction<String, String>() {
////				public void apply(String key, String value) {
////					System.out.println(key + ": " + value);
////				}
////			});
//			final KStream<String, String> FatalEvent = textLines.filter(new Predicate<String, String>() {
//				@Override
//				public boolean test(String key, String value) {
//					try {
//						JSONObject json = (JSONObject) parser.parse(value);
//						if (json.get("log_event").equals("FATAL")) {
//							System.out.println("return true");
//							return true;
//						} else {
//							System.out.println("return false" + json.get("log_event"));
//							return false;
//						}
//					} catch (ParseException e) {
//						// TODO Auto-generated catch block
//						System.out.println("catch block executed");
//						e.printStackTrace();
//						return false;
//					}
//				}
//			});
//			FatalEvent.peek((k, v) -> System.out.println("Key = " + k + "Value = " + v));
//			KTable<Windowed<String>, Long> Fatalcount =  FatalEvent
//					.groupBy((key, word) -> "value")
//             .windowedBy(TimeWindows.of(Duration.ofMinutes(3)).advanceBy(Duration.ofMinutes(1))).count();
//			
//			Fatalcount.toStream().peek((k, v) -> System.out.println("Key = " + k + "Value = " + v));
//			Fatalcount.toStream((k, v) -> k.toString()).to(outputTopic);
//		} catch (Exception e) {
//			System.out.println("error" + e);
//		}
//
//	}
//
//	static void windowData(final StreamsBuilder builder) {
//		final Serde<String> stringSerde = Serdes.String();
//		final Serde<Long> longSerde = Serdes.Long();
//		try {
//			System.out.println("filter data");
//			final KStream<String, String> textLines = builder.stream(inputTopic);
////			textLines.foreach(new ForeachAction<String, String>() {
////				public void apply(String key, String value) {
////					System.out.println(key + ": " + value);
////				}
////			});
//			final KStream<String, String> FatalEvent = textLines.filter(new Predicate<String, String>() {
//				@Override
//				public boolean test(String key, String value) {
//					try {
//						JSONObject json = (JSONObject) parser.parse(value);
//						if (json.get("log_event").equals("FATAL")) {
//							System.out.println("return true");
//							return true;
//						} else {
//							System.out.println("return false" + json.get("log_event"));
//							return false;
//						}
//					} catch (ParseException e) {
//						// TODO Auto-generated catch block
//						System.out.println("catch block executed");
//						e.printStackTrace();
//						return false;
//					}
//				}
//			});
//			FatalEvent.peek((k, v) -> System.out.println("Key = " + k + "Value = " + v));
//			@SuppressWarnings("unchecked")
//			KTable<Windowed<String>, String> Fatalcount =  (KTable<Windowed<String>, String>) FatalEvent
//					.groupBy((key, word) -> "value")
//             .windowedBy(TimeWindows.of(Duration.ofMinutes(3)).advanceBy(Duration.ofMinutes(1)));
//			
//			Fatalcount.toStream().peek((k, v) -> System.out.println("Key = " + k + "Value = " + v));
//			Fatalcount.toStream((k, v) -> k.toString()).to(outputTopic);
//		} catch (Exception e) {
//			System.out.println("error" + e);
//		}
//
//	}
//	
//	static void joinTopic(final StreamsBuilder builder) {
//		try {
//			System.out.println("join Topic");
//			ArrayList<String> inputTopiclist=new ArrayList<String>();
//			inputTopiclist.add("plaintextinput");
//			inputTopiclist.add("plaintextinput1");
//			final KStream<String, String> textLines = builder.stream(inputTopiclist);
//			textLines.foreach(new ForeachAction<String, String>() {
//				public void apply(String key, String value) {
//					System.out.println(key + ": " + value);
//				}
//			});
//			textLines.to(outputTopic);
//		} catch (Exception e) {
//			System.out.println("error" + e);
//		}
//	}
//
//}
