/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.engine.processing.streamprocessor;

import io.zeebe.engine.util.EngineRule;
import io.zeebe.engine.util.WorkflowExecutor;
import io.zeebe.model.bpmn.BpmnModelInstance;
import io.zeebe.protocol.record.intent.WorkflowInstanceIntent;
import io.zeebe.protocol.record.value.BpmnElementType;
import io.zeebe.test.util.bpmn.random.ExecutionPath;
import io.zeebe.test.util.bpmn.random.RandomWorkflowGenerator;
import io.zeebe.test.util.record.RecordingExporter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class RandomWorkflowExecutionPropertyTest {

  @Rule public final EngineRule engineRule = EngineRule.singlePartition();

  @Parameter public TestDataRecord record;

  private final WorkflowExecutor workflowExecutor = new WorkflowExecutor(engineRule);

  /**
   * This test takes a random workflow and execution path in that workflow. A process instance is
   * started and the workflow is executed according to the random execution path. The test passes if
   * it reaches the end of the workflow.
   */
  @Test
  public void shouldExecuteWorkflowToEnd() {
    final BpmnModelInstance model = record.getBpmnModel();
    engineRule.deployment().withXmlResource(model).deploy();

    final ExecutionPath path = record.getExecutionPath();

    path.getSteps().forEach(workflowExecutor::applyStep);

    // wait for termination of the process
    RecordingExporter.workflowInstanceRecords(WorkflowInstanceIntent.ELEMENT_COMPLETED)
        .withElementType(BpmnElementType.PROCESS)
        .withBpmnProcessId(path.getProcessId())
        .await();
  }

  /*
   * Some notes on scaling of these tests:
   * With 10 workflows and 100 paths there is a theoretical maximum of 1000 records.
   * However, in tests the number of actual records was around 300, which can execute in about 1 m.
   *
   * Having a high number of random execution paths has only a small effect as there are rarely 100
   * different execution paths for any given workflow. Having a high number of paths gives us a good
   * chance to exhaust all possible paths within a given workflow.
   *
   * This is only true if the complexity of the workflows stays constant.
   * Increasing the maximum number of blocks, depth or branches could increase the number of
   * possible paths exponentially
   */
  @Parameters(name = "{0}")
  public static Collection<TestDataRecord> getTestRecord() {
    final List<TestDataRecord> records = new ArrayList<>();

    final Random random = new Random();

    for (int i = 0; i < 10; i++) {
      final long workflowSeed = random.nextLong();

      final RandomWorkflowGenerator generator =
          new RandomWorkflowGenerator(workflowSeed, null, null, null);

      final BpmnModelInstance bpmnModelInstance = generator.buildWorkflow();

      final Set<ExecutionPath> paths = new HashSet<>();
      for (int p = 0; p < 100; p++) {

        final long pathSeed = random.nextLong();

        final ExecutionPath path = generator.findRandomExecutionPath(pathSeed);

        final boolean isDifferentPath = paths.add(path);

        if (isDifferentPath) {
          records.add(new TestDataRecord(workflowSeed, pathSeed, bpmnModelInstance, path));
        }
      }
    }

    return records;
  }

  private static final class TestDataRecord {
    private final long workFlowSeed;
    private final long executionPathSeed;

    private final BpmnModelInstance bpmnModel;
    private final ExecutionPath executionPath;

    private TestDataRecord(
        final long workFlowSeed,
        final long executionPathSeed,
        final BpmnModelInstance bpmnModel,
        final ExecutionPath executionPath) {
      this.workFlowSeed = workFlowSeed;
      this.executionPathSeed = executionPathSeed;
      this.bpmnModel = bpmnModel;
      this.executionPath = executionPath;
    }

    public BpmnModelInstance getBpmnModel() {
      return bpmnModel;
    }

    public ExecutionPath getExecutionPath() {
      return executionPath;
    }

    @Override
    public String toString() {
      return "TestDataRecord{"
          + "workFlowSeed="
          + workFlowSeed
          + ", executionPathSeed="
          + executionPathSeed
          + '}';
    }
  }
}
