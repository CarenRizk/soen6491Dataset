/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.transforms.windowing;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Joiner;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.MoreObjects;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;


@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public abstract class Trigger implements Serializable {

  protected final List<Trigger> subTriggers;

  protected Trigger(List<Trigger> subTriggers) {
    this.subTriggers = subTriggers;
  }

  protected Trigger() {
    this(Collections.emptyList());
  }

  public List<Trigger> subTriggers() {
    return MoreObjects.firstNonNull(subTriggers, Collections.emptyList());
  }

  
  public Trigger getContinuationTrigger() {
    if (subTriggers == null) {
      return getContinuationTrigger(null);
    }

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

  
  @Internal
  public boolean isCompatible(Trigger other) {
    if (!getClass().equals(other.getClass())) {
      return false;
    }

    if (subTriggers == null) {
      return other.subTriggers == null;
    } else if (other.subTriggers == null) {
      return false;
    } else if (subTriggers.size() != other.subTriggers.size()) {
      return false;
    }

    for (int i = 0; i < subTriggers.size(); i++) {
      if (!subTriggers.get(i).isCompatible(other.subTriggers.get(i))) {
        return false;
      }
    }

    return true;
  }

  @Override
  public String toString() {
    String simpleName = getClass().getSimpleName();
    if (getClass().getEnclosingClass() != null) {
      simpleName = getClass().getEnclosingClass().getSimpleName() + "." + simpleName;
    }
    if (subTriggers == null || subTriggers.isEmpty()) {
      return simpleName;
    } else {
      return simpleName + "(" + Joiner.on(", ").join(subTriggers) + ")";
    }
  }

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
