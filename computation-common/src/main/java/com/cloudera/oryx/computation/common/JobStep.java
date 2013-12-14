/*
 * Copyright (c) 2013, Cloudera, Inc. All Rights Reserved.
 *
 * Cloudera, Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"). You may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for
 * the specific language governing permissions and limitations under the
 * License.
 */

package com.cloudera.oryx.computation.common;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;

import com.google.common.base.Preconditions;
import com.typesafe.config.Config;
import org.apache.crunch.GroupingOptions;
import org.apache.crunch.PipelineExecution;
import org.apache.crunch.Source;
import org.apache.crunch.Target;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.impl.mr.MRPipelineExecution;
import org.apache.crunch.io.From;
import org.apache.crunch.io.To;
import org.apache.crunch.types.PType;
import org.apache.crunch.types.avro.AvroType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobStatus;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.oryx.common.log.MemoryHandler;
import com.cloudera.oryx.common.settings.ConfigUtils;
import com.cloudera.oryx.common.servcomp.Namespaces;
import com.cloudera.oryx.common.servcomp.OryxConfiguration;
import com.cloudera.oryx.common.servcomp.Store;

/**
 * Abstract superclass of all "steps".
 *
 * @author Sean Owen
 */
public abstract class JobStep extends Configured implements Tool, HasState {

  private static final Logger log = LoggerFactory.getLogger(JobStep.class);

  public static final String CONFIG_SERIALIZATION_KEY = "CONFIG_SERIALIZATION";

  private JobStepConfig config;
  private Date startTime;
  private Date endTime;
  private MRPipelineExecution exec;

  protected abstract JobStepConfig parseConfig(String[] args);

  protected JobStepConfig getConfig() {
    return config;
  }

  @Override
  public final Collection<StepState> getStepStates() throws IOException {
    String name = getCustomJobName();
    StepStatus status = determineStatus();
    StepState state = new StepState(startTime, endTime, name, status);
    if (status == StepStatus.COMPLETED) {
      // This completed, -- perhaps without ever running, which means Hadoop doesn't report state because
      // it was skipped. It's complete.
      state.setMapProgress(1.0f);
      state.setReduceProgress(1.0f);
    } else {
      float[] progresses = determineProgresses();
      if (progresses != null) {
        // 0 is setup progress; we don't care
        state.setMapProgress(progresses[1]);
        state.setReduceProgress(progresses[2]);
      }
    }
    return Collections.singletonList(state);
  }

  private StepStatus determineStatus() throws IOException {
    JobContext job = getJob();
    if (job == null) {
      return StepStatus.COMPLETED;
    }
    JobClient client = new JobClient(new JobConf(getConf()));
    try {
      JobID jobID = job.getJobID();
      if (jobID == null) {
        return StepStatus.PENDING;
      }
      RunningJob runningJob =
          client.getJob(new org.apache.hadoop.mapred.JobID(jobID.getJtIdentifier(), jobID.getId()));
      if (runningJob == null) {
        return StepStatus.PENDING;
      }
      int state = runningJob.getJobState();
      // Can't use switch since these are no longer compile-time constants in Hadoop 2.x
      if (state == JobStatus.PREP) {
        return StepStatus.PENDING;
      }
      if (state == JobStatus.RUNNING) {
        return StepStatus.RUNNING;
      }
      if (state == JobStatus.FAILED) {
        return StepStatus.FAILED;
      }
      if (state == JobStatus.KILLED) {
        return StepStatus.CANCELLED;
      }
      if (state == JobStatus.SUCCEEDED) {
        return StepStatus.COMPLETED;
      }
      throw new IllegalArgumentException("Unknown Hadoop job state " + state);
    } finally {
      client.close();
    }
  }

  private JobContext getJob() {
    return exec == null ? null : exec.getJobs().get(0).getJob();
  }

  /**
   * @return three progress values, in [0,1], as a {@code float[]}, representing setup, mapper and reducer progress
   */
  private float[] determineProgresses() throws IOException {
    if (exec == null) {
      return null;
    }
    JobClient client = new JobClient(new JobConf(getConf()));
    try {
      JobID jobID = getJob().getJobID();
      if (jobID == null) {
        return null;
      }
      RunningJob runningJob =
          client.getJob(new org.apache.hadoop.mapred.JobID(jobID.getJtIdentifier(), jobID.getId()));
      if (runningJob == null) {
        return null;
      }

      return new float[] { runningJob.setupProgress(), runningJob.mapProgress(), runningJob.reduceProgress() };
    } finally {
      client.close();
    }
  }

  @Override
  public final int run(String[] args) throws InterruptedException, IOException, JobException {

    MemoryHandler.setSensibleLogFormat();

    if (log.isInfoEnabled()) {
      log.info("args are {}", Arrays.toString(args));
    }

    config = parseConfig(args);

    String name = getCustomJobName();
    log.info("Running {}", name);

    long start = System.currentTimeMillis();
    startTime = new Date(start);
    MRPipeline pipeline = createPipeline();
    try {
      if (pipeline == null) {
        log.info("{} does not need to run", name);
      } else {

        exec = pipeline.runAsync();
        log.info("Waiting for {} to complete", name);
        exec.waitUntilDone();

        PipelineExecution.Status exitStatus = exec.getStatus();
        if (exitStatus == PipelineExecution.Status.KILLED || exitStatus == PipelineExecution.Status.FAILED) {
          exec.kill();
          throw new JobException(name + " failed in state " + exitStatus);
        }

        log.info("Finished {}", name);
        postRun();

      }
    } catch (InterruptedException ie) {
      log.warn("Interrupted {}", name);
      exec.kill();
      throw ie;
    } finally {
      long end = System.currentTimeMillis();
      endTime = new Date(end);
      log.info("Completed {} in {}s", this, (end - start) / 1000L);
      if (pipeline != null) {
        pipeline.done();
      }
    }

    return 0;
  }

  /**
   * Subclasses override this to make the {@link MRPipeline}.
   *
   * @return {@link MRPipeline} encapsulating the MapReduce to run for this step or null if there is nothing
   *  to run
   */
  protected MRPipeline createPipeline() throws IOException {
    return null;
  }

  /**
   * Subclasses override this to perform any steps after the job has finished.
   */
  protected void postRun() throws IOException {
    // do nothing
  }

  protected boolean isHighMemoryStep() {
    return false;
  }

  /**
   * Creates a new {@link MRPipeline} instance that contains common configuration
   * settings.
   *
   * @return a new {@link MRPipeline} instance, suitably configured
   */
  protected final MRPipeline createBasicPipeline(Class<?> jarClass) throws IOException {
    Configuration conf = OryxConfiguration.get(getConf());

    conf.setBoolean("mapred.map.output.compress", true);
    conf.setClass("mapred.map.output.compress.codec", SnappyCodec.class, CompressionCodec.class);

    conf.setBoolean("mapred.output.compress", true);
    conf.set("mapred.output.compression.type", "BLOCK");
    conf.setClass("mapred.output.compression.codec", SnappyCodec.class, CompressionCodec.class);
    // Set old-style equivalents for Avro/Crunch's benefit
    conf.set("avro.output.codec", "snappy");

    conf.setBoolean("mapreduce.map.speculative", true);
    conf.setBoolean("mapreduce.reduce.speculative", true);
    conf.setBoolean("mapreduce.tasktracker.outofband.heartbeat", true);
    conf.setInt("mapreduce.job.jvm.numtasks", -1);

    //conf.setBoolean("crunch.disable.deep.copy", true);

    Config appConfig = ConfigUtils.getDefaultConfig();

    int mapMemoryMB = appConfig.getInt("computation-layer.mapper-memory-mb");
    log.info("Mapper memory: {}", mapMemoryMB);
    int mapHeapMB = (int) (mapMemoryMB / 1.3); // Matches CDH's default
    log.info("Mappers have {}MB heap and can access {}MB RAM", mapHeapMB, mapMemoryMB);
    if (conf.get("mapreduce.map.java.opts") != null) {
      log.info("Overriding previous setting of {}, which was '{}'",
               "mapreduce.map.java.opts",
               conf.get("mapreduce.map.java.opts"));
    }
    conf.set("mapreduce.map.java.opts",
             "-Xmx" + mapHeapMB + "m -XX:+UseCompressedOops -XX:+UseParallelGC -XX:+UseParallelOldGC");
    log.info("Set {} to '{}'", "mapreduce.map.java.opts", conf.get("mapreduce.map.java.opts"));
    // See comment below on CM
    conf.setInt("mapreduce.map.java.opts.max.heap", mapHeapMB);

    int reduceMemoryMB = appConfig.getInt("computation-layer.reducer-memory-mb");
    log.info("Reducer memory: {}", reduceMemoryMB);
    if (isHighMemoryStep()) {
      reduceMemoryMB *= appConfig.getInt("computation-layer.worker-high-memory-factor");
      log.info("Increasing {} to {} for high-memory step", "mapreduce.reduce.memory.mb", reduceMemoryMB);
    }
    conf.setInt("mapreduce.reduce.memory.mb", reduceMemoryMB);

    int reduceHeapMB = (int) (reduceMemoryMB / 1.3); // Matches CDH's default
    log.info("Reducers have {}MB heap and can access {}MB RAM", reduceHeapMB, reduceMemoryMB);
    if (conf.get("mapreduce.reduce.java.opts") != null) {
      log.info("Overriding previous setting of {}, which was '{}'",
               "mapreduce.reduce.java.opts",
               conf.get("mapreduce.reduce.java.opts"));
    }
    conf.set("mapreduce.reduce.java.opts",
             "-Xmx" + reduceHeapMB + "m -XX:+UseCompressedOops -XX:+UseParallelGC -XX:+UseParallelOldGC");
    log.info("Set {} to '{}'", "mapreduce.reduce.java.opts", conf.get("mapreduce.reduce.java.opts"));
    // I see this in CM but not in Hadoop docs; probably won't hurt as it's supposed to result in
    // -Xmx appended to opts above, which is at worst redundant
    conf.setInt("mapreduce.reduce.java.opts.max.heap", reduceHeapMB);

    conf.setInt("yarn.scheduler.capacity.minimum-allocation-mb", 128);
    conf.setInt("yarn.app.mapreduce.am.resource.mb", 384);

    // Pass total config state
    conf.set(CONFIG_SERIALIZATION_KEY, ConfigUtils.getDefaultConfig().root().render());

    // Make sure to set any args to conf above this line!

    setConf(conf);

    Job job = Job.getInstance(conf);

    // Basic File IO settings
    FileInputFormat.setMaxInputSplitSize(job, 1L << 30); // ~1073MB
    FileInputFormat.setMinInputSplitSize(job, 1L << 27); // ~134MB
    SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK);
    FileOutputFormat.setCompressOutput(job, true);
    FileOutputFormat.setOutputCompressorClass(job, SnappyCodec.class);

    log.info("Created pipeline configuration {}", job.getConfiguration());

    return new MRPipeline(jarClass, getCustomJobName(), job.getConfiguration());
  }

  protected final boolean validOutputPath(String outputPathKey) throws IOException {
    Preconditions.checkArgument(outputPathKey != null && outputPathKey.endsWith("/"),
        "%s should end with /", outputPathKey);

    String jobName = getCustomJobName();

    Store store = Store.get();
    if (store.exists(outputPathKey, false)) {
      log.info("Output path {} already exists", outputPathKey);
      if (store.exists(outputPathKey + "_SUCCESS", true)) {
        log.info("{} shows _SUCCESS present; skipping", jobName);
        return false;
      }
      log.info("{} seems to have stopped during an earlier run; deleting output at {} and recomputing",
          jobName, outputPathKey);
      store.recursiveDelete(outputPathKey);
    } else {
      log.info("Output path is clear for writing: {}", outputPathKey);
    }
    return true;
  }

  protected final <T> Source<T> input(String inputPathKey, PType<T> ptype) {
    return avroInput(inputPathKey, ptype);
  }

  protected final <T> Source<T> avroInput(String inputPathKey, PType<T> avroType) {
    return From.avroFile(Namespaces.toPath(inputPathKey), (AvroType<T>) avroType);
  }

  protected final Source<String> textInput(String inputPathKey) {
    return From.textFile(Namespaces.toPath(inputPathKey));
  }

  protected final Target avroOutput(String outputPathKey) {
    return To.avroFile(Namespaces.toPath(outputPathKey));
  }

  protected final Target output(String outputPathKey) {
    return avroOutput(outputPathKey);
  }

  protected final Target compressedTextOutput(Configuration conf, String outputPathKey) {
    // The way this is used, it doesn't seem like we can just set the object in getConf(). Need
    // to set the copy in the MRPipeline directly?
    conf.setClass("mapred.output.compression.codec", GzipCodec.class, CompressionCodec.class);
    conf.setClass("mapred.map.output.compress.codec", SnappyCodec.class, CompressionCodec.class);
    return To.textFile(Namespaces.toPath(outputPathKey));
  }

  protected final GroupingOptions groupingOptions() {
    return groupWithComparator(null);
  }

  protected final GroupingOptions groupWithComparator(
      Class<? extends RawComparator<?>> comparator) {
    return groupingOptions(HashPartitioner.class, comparator);
  }

  protected final GroupingOptions groupingOptions(
      Class<? extends Partitioner> partitionerClass,
      Class<? extends RawComparator<?>> comparator) {
    return groupingOptions(partitionerClass, comparator, comparator);
  }

  protected final GroupingOptions groupingOptions(
      Class<? extends Partitioner> partitionerClass,
      Class<? extends RawComparator<?>> groupingComparator,
      Class<? extends RawComparator<?>> sortComparator) {
    GroupingOptions.Builder b = GroupingOptions.builder()
        .partitionerClass(partitionerClass)
        .numReducers(getNumReducers());

    if (groupingComparator != null) {
      b.groupingComparatorClass(groupingComparator);
    }
    if (sortComparator != null) {
      b.sortComparatorClass(sortComparator);
    }
    return b.build();
  }

  protected final int getNumReducers() {
    String parallelismString = ConfigUtils.getDefaultConfig().getString("computation-layer.parallelism");
    if ("auto".equals(parallelismString)) {
      return 8; // Better idea for a default?
    } else {
      return Integer.parseInt(parallelismString);
    }
  }

  public static void run(Tool step, String[] args) throws IOException, InterruptedException, JobException {
    log.info("Running step {}", step.getClass().getSimpleName());
    try {
      int result = ToolRunner.run(OryxConfiguration.get(), step, args);
      if (result != 0) {
        throw new JobException(step + " hasd bad exit status: " + result);
      }
    } catch (IOException ioe) {
      throw ioe;
    } catch (InterruptedException ie) {
      throw ie;
    } catch (Exception e) {
      throw new JobException(e);
    }
  }

  protected final String defaultCustomJobName() {
    StringBuilder name = new StringBuilder(100);
    name.append("Oryx-").append(config.getInstanceDir());
    name.append('-').append(config.getGenerationID());
    name.append('-').append(getClass().getSimpleName());
    return name.toString();
  }

  protected String getCustomJobName() {
    return defaultCustomJobName();
  }

  @Override
  public final String toString() {
    return getClass().getSimpleName();
  }

}
