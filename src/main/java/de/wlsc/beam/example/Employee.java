package de.wlsc.beam.example;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.Currency;

@Data
@NoArgsConstructor
public class Employee implements Serializable {

  private String name;
  private long bonus;
  private Currency currency;
}
