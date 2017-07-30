/*
 * Copyright © 2017 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.proto;

import co.cask.cdap.api.ProgramStatus;
import co.cask.cdap.internal.schedule.trigger.Trigger;
import co.cask.cdap.internal.schedule.trigger.TriggerBuilder;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.proto.id.StreamId;

import java.util.EnumSet;
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Represents a trigger in a REST request/response.
 */
public abstract class ProtoTrigger implements Trigger {

  /**
   * Represents all known trigger types in REST requests/responses.
   */
  public enum Type {
    TIME,
    PARTITION,
    STREAM_SIZE,
    PROGRAM_STATUS,
    AND(true),
    OR(true);

    private final boolean isComposite;

    Type() {
      this.isComposite = false;
    }

    Type(boolean isComposite) {
      this.isComposite = isComposite;
    }

    public boolean isComposite() {
      return isComposite;
    }
  }

  private final Type type;

  private ProtoTrigger(Type type) {
    this.type = type;
  }

  public Type getType() {
    return type;
  }

  public abstract void validate();

  /**
   * Represents a time trigger in REST requests/responses.
   */
  public static class TimeTrigger extends ProtoTrigger {

    protected final String cronExpression;

    public TimeTrigger(String cronExpression) {
      super(Type.TIME);
      this.cronExpression = cronExpression;
      validate();
    }

    public String getCronExpression() {
      return cronExpression;
    }

    @Override
    public boolean equals(Object o) {
      return this == o ||
        o != null
          && getClass().equals(o.getClass())
          && Objects.equals(getCronExpression(), ((TimeTrigger) o).getCronExpression());
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(getCronExpression());
    }

    @Override
    public String toString() {
      return "TimeTrigger(" + getCronExpression() + "\")";
    }

    @Override
    public void validate() {
      validateNotNull(getCronExpression(), "cron expression");
    }
  }

  /**
   * Represents a partition trigger in REST requests/responses.
   */
  public static class PartitionTrigger extends ProtoTrigger {

    protected final DatasetId dataset;
    protected final int numPartitions;

    public PartitionTrigger(DatasetId dataset, int numPartitions) {
      super(Type.PARTITION);
      this.dataset = dataset;
      this.numPartitions = numPartitions;
      validate();
    }

    public DatasetId getDataset() {
      return dataset;
    }

    public int getNumPartitions() {
      return numPartitions;
    }

    @Override
    public void validate() {
      ProtoTrigger.validateNotNull(getDataset(), "dataset");
      ProtoTrigger.validateNotNull(getDataset().getNamespace(), "dataset namespace");
      ProtoTrigger.validateNotNull(getDataset().getDataset(), "dataset name");
      ProtoTrigger.validateInRange(getNumPartitions(), "number of partitions", 1, null);
    }

    @Override
    public boolean equals(Object o) {
      return this == o ||
        o != null &&
          getClass().equals(o.getClass()) &&
          Objects.equals(getDataset(), ((PartitionTrigger) o).getDataset()) &&
          Objects.equals(getNumPartitions(), ((PartitionTrigger) o).getNumPartitions());
    }

    @Override
    public int hashCode() {
      return Objects.hash(getDataset(), getNumPartitions());
    }

    @Override
    public String toString() {
      return String.format("PartitionTrigger(%s, %d partitions)", getDataset(), getNumPartitions());
    }
  }

  /**
   * Abstract base class for composite trigger in REST requests/responses.
   */
  public abstract static class AbstractCompositeTrigger extends ProtoTrigger {
    private final Trigger[] triggers;

    public AbstractCompositeTrigger(Type type, Trigger... triggers) {
      super(type);
      this.triggers = triggers;
      validate();
    }

    public Trigger[] getTriggers() {
      return triggers;
    }

    @Override
    public void validate() {
      if (!getType().isComposite()) {
        throw new IllegalArgumentException("Trigger type " + getType().name() + " is not a composite trigger.");
      }
      Trigger[] internalTriggers = getTriggers();
      ProtoTrigger.validateNotNull(internalTriggers, "internal trigger");
      if (internalTriggers.length == 0) {
        throw new IllegalArgumentException(String.format("Triggers passed in to construct a trigger " +
                                                           "of type %s cannot be empty.", getType().name()));
      }
      for (Trigger trigger : internalTriggers) {
        if (trigger == null) {
          throw new IllegalArgumentException(String.format("Triggers passed in to construct a trigger " +
                                                             "of type %s cannot contain null.", getType().name()));
        }
      }
    }
  }

  /**
   * Represents an AND trigger in REST requests/responses.
   */
  public static class AndTrigger extends AbstractCompositeTrigger implements TriggerBuilder{

    public AndTrigger(Trigger... triggers) {
      super(Type.AND, triggers);
    }

    @Override
    public Trigger build(String namespace, String applicationName, String applicationVersion) {
      int numTriggers = getTriggers().length;
      Trigger[] builtTriggers = new Trigger[numTriggers];
      for (int i = 0; i < numTriggers; i++) {
        Trigger trigger = getTriggers()[i];
        builtTriggers[i] = trigger instanceof TriggerBuilder ?
          ((TriggerBuilder) trigger).build(namespace, applicationName, applicationVersion) : trigger;
      }
      return new AndTrigger(builtTriggers);
    }
  }

  /**
   * Represents an OR trigger in REST requests/responses.
   */
  public static class OrTrigger extends AbstractCompositeTrigger implements TriggerBuilder{

    public OrTrigger(Trigger... triggers) {
      super(Type.OR, triggers);
    }

    @Override
    public Trigger build(String namespace, String applicationName, String applicationVersion) {
      int numTriggers = getTriggers().length;
      Trigger[] builtTriggers = new Trigger[numTriggers];
      for (int i = 0; i < numTriggers; i++) {
        Trigger trigger = getTriggers()[i];
        builtTriggers[i] = trigger instanceof TriggerBuilder ?
          ((TriggerBuilder) trigger).build(namespace, applicationName, applicationVersion) : trigger;
      }
      return new OrTrigger(builtTriggers);
    }
  }

  /**
   * Represents a stream size trigger in REST requests/responses.
   */
  public static class StreamSizeTrigger extends ProtoTrigger {

    protected final StreamId streamId;
    protected final int triggerMB;

    public StreamSizeTrigger(StreamId streamId, int triggerMB) {
      super(Type.STREAM_SIZE);
      this.streamId = streamId;
      this.triggerMB = triggerMB;
      validate();
    }

    public StreamId getStreamId() {
      return streamId;
    }

    public int getTriggerMB() {
      return triggerMB;
    }

    @Override
    public void validate() {
      ProtoTrigger.validateNotNull(getStreamId(), "stream");
      ProtoTrigger.validateNotNull(getStreamId().getNamespace(), "stream namespace");
      ProtoTrigger.validateNotNull(getStreamId().getStream(), "stream name");
      ProtoTrigger.validateInRange(getTriggerMB(), "trigger in MB", 1, null);
    }

    @Override
    public boolean equals(Object o) {
      return this == o ||
        o != null &&
          getClass().equals(o.getClass()) &&
          Objects.equals(getStreamId(), ((StreamSizeTrigger) o).getStreamId()) &&
          Objects.equals(getTriggerMB(), ((StreamSizeTrigger) o).getTriggerMB());
    }

    @Override
    public int hashCode() {
      return Objects.hash(getStreamId(), getTriggerMB());
    }

    @Override
    public String toString() {
      return String.format("StreamSizeTrigger(%s, %d MB)", getStreamId(), getTriggerMB());
    }
  }

  /**
   * Represents a program status trigger for REST requests/responses
   */
  public static class ProgramStatusTrigger extends ProtoTrigger {
    protected final ProgramId programId;
    protected final Set<ProgramStatus> programStatuses;

    public ProgramStatusTrigger(ProgramId programId, Set<ProgramStatus> programStatuses) {
      super(Type.PROGRAM_STATUS);

      this.programId = programId;
      this.programStatuses = programStatuses;
      validate();
    }

    public ProgramId getProgramId() {
      return programId;
    }

    public Set<ProgramStatus> getProgramStatuses() {
      return programStatuses;
    }

    @Override
    public void validate() {
      if (getProgramStatuses().contains(ProgramStatus.INITIALIZING) ||
          getProgramStatuses().contains(ProgramStatus.RUNNING)) {
        throw new IllegalArgumentException(String.format(
                "Cannot allow triggering program %s with status %s: COMPLETED, FAILED, KILLED statuses are supported",
                programId.getProgram(), programId.getType()));
      }

      ProtoTrigger.validateNotNull(getProgramId(), "program id");
      ProtoTrigger.validateNotNull(getProgramStatuses(), "program statuses");
    }

    @Override
    public int hashCode() {
      return Objects.hash(getProgramId(), getProgramStatuses());
    }

    @Override
    public boolean equals(Object o) {
      return this == o ||
        o != null &&
          getClass().equals(o.getClass()) &&
          Objects.equals(getProgramStatuses(), ((ProgramStatusTrigger) o).getProgramStatuses()) &&
          Objects.equals(getProgramId(), ((ProgramStatusTrigger) o).getProgramId());
    }

    @Override
    public String toString() {
      return String.format("ProgramStatusTrigger(%s, %s)", getProgramId().getProgram(),
                                                           getProgramStatuses().toString());
    }
  }

  /**
   * A Trigger builder that builds a ProgramStatusTrigger.
   */
  public static class ProgramStatusTriggerBuilder implements TriggerBuilder {
    private final String programNamespace;
    private final String programApplication;
    private final String programApplicationVersion;
    private final ProgramType programType;
    private final String programName;
    private final EnumSet<ProgramStatus> programStatuses;

    public ProgramStatusTriggerBuilder(@Nullable String programNamespace, @Nullable String programApplication,
                                       @Nullable String programApplicationVersion, String programType,
                                       String programName, ProgramStatus... programStatuses) {
      this.programNamespace = programNamespace;
      this.programApplication = programApplication;
      this.programApplicationVersion = programApplicationVersion;
      this.programType = ProgramType.valueOf(programType);
      this.programName = programName;

      // User can not specify any program statuses, or specify null, which is an array of length 1 containing null
      if (programStatuses.length == 0 || (programStatuses.length == 1 && programStatuses[0] == null)) {
        throw new IllegalArgumentException("Must set a program state for the triggering program");
      }
      this.programStatuses = EnumSet.of(programStatuses[0], programStatuses);
    }

    @Override
    public ProgramStatusTrigger build(String namespace, String applicationName, String applicationVersion) {
      // Inherit environment attributes from the deployed application
      ProgramId programId = new ApplicationId(
        firstNonNull(programNamespace, namespace),
        firstNonNull(programApplication, applicationName),
        firstNonNull(programApplicationVersion, applicationVersion)).program(programType, programName);
      return new ProgramStatusTrigger(programId, programStatuses);
    }

    private String firstNonNull(@Nullable String first, String second) {
      if (first != null) {
        return first;
      }
      return second;
    }
  }

  private static void validateNotNull(@Nullable Object o, String name) {
    if (o == null) {
      throw new IllegalArgumentException(name + " must not be null");
    }
  }

  private static <V extends Comparable<V>>
  void validateInRange(@Nullable V value, String name, @Nullable V minValue, @Nullable V maxValue) {
    if (value == null) {
      throw new IllegalArgumentException(name + " must be specified");
    }
    if (minValue != null && value.compareTo(minValue) < 0) {
      throw new IllegalArgumentException(name + " must be greater than or equal to" + minValue + " but is " + value);
    }
    if (maxValue != null && value.compareTo(maxValue) > 0) {
      throw new IllegalArgumentException(name + " must be less than or equal to " + maxValue + " but is " + value);
    }
  }
}

