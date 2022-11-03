package com.nus.cool.core.util.writer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Testing ListDataWriter.
 */
public class ListDataWriterTest {

  @Test
  public void testListDataWriter() {
    String[] input1 = {"s11", "s12"};
    String[] input2 = {"s21", "s22"};
    List<String> out = new ArrayList<>();
    ListDataWriter writer = new ListDataWriter(out);
    try {
      writer.initialize();
      writer.add(input1);
      writer.add(input2);
      writer.finish();
      Assert.assertEquals(out.get(0), Arrays.toString(input1));
      Assert.assertEquals(out.get(1), Arrays.toString(input2));
      writer.close();
    } catch (IOException e) {
      System.err.println(e);
    }
  }
}
