package de.wlsc.beam.example;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.extensions.jackson.ParseJsons;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;

@SpringBootApplication
public class Application {

  public static void main(String[] args) {
    SpringApplication.run(Application.class, args);
  }

  @EventListener(ApplicationReadyEvent.class)
  public void start() {

    Pipeline pipeline = Pipeline.create();

    pipeline.apply("Read line by line", TextIO.read().from("input.json"))
            .apply(ParseJsons.of(Employee.class))
            .setCoder(SerializableCoder.of(Employee.class))
            .apply("Convert to KV", MapElements.via(new DataDeserializer()))
            .apply(GroupByKey.create())
            .apply("Calculate average bonus", ParDo.of(new ElementProcessor()))
            .apply(TextIO.write().to("output.json").withoutSharding());

    pipeline.run();
  }

}
