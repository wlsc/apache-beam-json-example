package de.wlsc.beam.example;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

public class ElementProcessor extends DoFn<KV<String, Iterable<Long>>, String> {

  @ProcessElement
  public void processElement(ProcessContext context) {

    String employeeName = context.element().getKey();
    Iterable<Long> bonuses = context.element().getValue();
    long sum = 0;
    for (long bonus : bonuses) {
      sum += bonus;
    }
    context.output(String.format("{\"%s\" : %d}", employeeName, sum));

  }
}
