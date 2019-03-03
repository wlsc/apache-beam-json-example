package de.wlsc.beam.example;

import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;

public class DataDeserializer extends SimpleFunction<Employee, KV<String, Long>> {

  @Override
  public KV<String, Long> apply(Employee employee) {
    return KV.of(employee.getName(), employee.getBonus());
  }
}
