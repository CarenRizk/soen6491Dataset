package org.apache.beam.sdk.transforms.windowing;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.List;
import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;


@RunWith(JUnit4.class)
public class TriggerTest {

  @Test
  public void testTriggerToString() throws Exception {
    assertEquals("AfterWatermark.pastEndOfWindow()", AfterWatermark.pastEndOfWindow().toString());
    assertEquals(
        "Repeatedly.forever(AfterWatermark.pastEndOfWindow())",
        Repeatedly.forever(AfterWatermark.pastEndOfWindow()).toString());
  }

  @Test
  public void testIsCompatible() throws Exception {
    assertTrue(new Trigger1(null).isCompatible(new Trigger1(null)));
    assertTrue(
        new Trigger1(Arrays.asList(new Trigger2(null)))
            .isCompatible(new Trigger1(Arrays.asList(new Trigger2(null)))));

    assertFalse(new Trigger1(null).isCompatible(new Trigger2(null)));
    assertFalse(
        new Trigger1(Arrays.asList(new Trigger1(null)))
            .isCompatible(new Trigger1(Arrays.asList(new Trigger2(null)))));
  }

  private static class Trigger1 extends Trigger {

    private Trigger1(List<Trigger> subTriggers) {
      super(subTriggers);
    }

    @Override
    protected Trigger getContinuationTrigger(List<Trigger> continuationTriggers) {
      return null;
    }

    @Override
    public Instant getWatermarkThatGuaranteesFiring(BoundedWindow window) {
      return null;
    }

    @Override
    public boolean mayFinish() {
      return false;
    }
  }

  private static class Trigger2 extends Trigger {

    private Trigger2(List<Trigger> subTriggers) {
      super(subTriggers);
    }

    @Override
    protected Trigger getContinuationTrigger(List<Trigger> continuationTriggers) {
      return null;
    }

    @Override
    public Instant getWatermarkThatGuaranteesFiring(BoundedWindow window) {
      return null;
    }

    @Override
    public boolean mayFinish() {
      return false;
    }
  }
}
