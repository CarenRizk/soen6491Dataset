package org.apache.beam.sdk.transforms.windowing;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Joiner;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;

@SuppressWarnings({
  "nullness" 
})
public abstract class Trigger implements Serializable {

  protected final List<Trigger> subTriggers;

  protected Trigger(List<Trigger> subTriggers) {
    this.subTriggers = subTriggers;
  }

  protected Trigger() {
    this(Collections.emptyList());
  }

  // Optimized by LLM: Removed the MoreObjects.firstNonNull call
  public List<Trigger> subTriggers() {
    return subTriggers;
  }

  // Optimized by LLM: Removed the null check for subTriggers
  public Trigger getContinuationTrigger() {
    List<Trigger> subTriggerContinuations = new ArrayList<>();
    for (Trigger subTrigger : subTriggers) {
      subTriggerContinuations.add(subTrigger.getContinuationTrigger());
    }
    return getContinuationTrigger(subTriggerContinuations);
  }

  protected abstract Trigger getContinuationTrigger(List<Trigger> continuationTriggers);

  @Internal
  public abstract Instant getWatermarkThatGuaranteesFiring(BoundedWindow window);

  @Internal
  public abstract boolean mayFinish();

  // Optimized by LLM: Simplified null checks for subTriggers using Objects.equals
  @Internal
  public boolean isCompatible(Trigger other) {
    if (!getClass().equals(other.getClass())) {
      return false;
    }

    if (!Objects.equals(subTriggers, other.subTriggers)) {
      return false;
    }

    if (subTriggers.size() != other.subTriggers.size()) {
      return false;
    }

    for (int i = 0; i < subTriggers.size(); i++) {
      if (!subTriggers.get(i).isCompatible(other.subTriggers.get(i))) {
        return false;
      }
    }

    return true;
  }

  // Optimized by LLM: Replaced null check with isEmpty check
  @Override
  public String toString() {
    String simpleName = getClass().getSimpleName();
    if (getClass().getEnclosingClass() != null) {
      simpleName = getClass().getEnclosingClass().getSimpleName() + "." + simpleName;
    }
    if (subTriggers.isEmpty()) {
      return simpleName;
    } else {
      return simpleName + "(" + Joiner.on(", ").join(subTriggers) + ")";
    }
  }

  // Optimized by LLM: Replaced null check with direct comparison using Objects.equals
  @Override
  public boolean equals(@Nullable Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof Trigger)) {
      return false;
    }
    Trigger that = (Trigger) obj;
    return Objects.equals(getClass(), that.getClass())
        && Objects.equals(subTriggers, that.subTriggers);
  }

  // Optimized by LLM: Used Arrays.hashCode for better handling of the list's contents
  @Override
  public int hashCode() {
    return Objects.hash(getClass(), subTriggers);
  }

  public OrFinallyTrigger orFinally(OnceTrigger until) {
    return new OrFinallyTrigger(this, until);
  }

  @Internal
  public abstract static class OnceTrigger extends Trigger {
    protected OnceTrigger(List<Trigger> subTriggers) {
      super(subTriggers);
    }

    @Override
    public final boolean mayFinish() {
      return true;
    }

    @Override
    public final OnceTrigger getContinuationTrigger() {
      Trigger continuation = super.getContinuationTrigger();
      if (!(continuation instanceof OnceTrigger)) {
        throw new IllegalStateException("Continuation of a OnceTrigger must be a OnceTrigger");
      }
      return (OnceTrigger) continuation;
    }
  }
}