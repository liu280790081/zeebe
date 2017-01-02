package org.camunda.tngp.broker.system.threads;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.agrona.concurrent.Agent;
import org.agrona.concurrent.AgentRunner;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.BusySpinIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.status.AtomicCounter;
import org.agrona.concurrent.status.CountersManager;
import org.camunda.tngp.broker.services.Counters;
import org.camunda.tngp.broker.system.ConfigurationManager;
import org.camunda.tngp.broker.system.threads.cfg.ThreadingCfg;
import org.camunda.tngp.broker.system.threads.cfg.ThreadingCfg.BrokerIdleStrategy;
import org.camunda.tngp.servicecontainer.Injector;
import org.camunda.tngp.servicecontainer.Service;
import org.camunda.tngp.servicecontainer.ServiceStartContext;
import org.camunda.tngp.servicecontainer.ServiceStopContext;
import org.camunda.tngp.util.agent.AgentRunnerFactory;
import org.camunda.tngp.util.agent.AgentRunnerService;
import org.camunda.tngp.util.agent.CompositeAgentRunnerServiceFactory;
import org.camunda.tngp.util.agent.SharedAgentRunnerService;

public class AgentRunnerServicesImpl implements AgentRunnerServices, Service<AgentRunnerServices>
{
    protected static final String AGENT_NAME_NETWORK_RECEIVER = "network-receiver";
    protected static final String AGENT_NAME_NETWORK_SENDER = "network-sender";
    protected static final String AGENT_NAME_LOG_APPENDER = "log-appender";
    protected static final String AGENT_NAME_LOG_STREAM_PROCESSOR = "log-stream-processor";
    protected static final String AGENT_NAME_CONDUCTOR = "conductor";

    static int maxThreadCount = Runtime.getRuntime().availableProcessors() - 1;

    protected final Injector<Counters> countersInjector = new Injector<>();

    protected AgentRunnerService networkReceiverAgentRunnerService;
    protected AgentRunnerService networkSenderAgentRunnerService;
    protected AgentRunnerService logAppenderAgentRunnerService;
    protected AgentRunnerService logStreamProcessorAgentRunnerService;
    protected AgentRunnerService conductorAgentRunnerService;

    protected final int availableThreads;

    protected final List<AtomicCounter> errorCounters = new ArrayList<>();

    protected final BrokerIdleStrategy brokerIdleStrategy;
    protected final int maxIdleTimeMs;

    public AgentRunnerServicesImpl(ConfigurationManager configurationManager)
    {
        final ThreadingCfg cfg = configurationManager.readEntry("threading", ThreadingCfg.class);

        int numberOfThreads = cfg.numberOfThreads;

        if (numberOfThreads > maxThreadCount)
        {
            System.err.println("WARNING: configured thread count (" + numberOfThreads + ") is larger than maxThreadCount " +
                    maxThreadCount + "). Falling back max thread count.");
            numberOfThreads = maxThreadCount;
        }

        availableThreads = numberOfThreads;
        brokerIdleStrategy = cfg.idleStrategy;
        maxIdleTimeMs = cfg.maxIdleTimeMs;
    }

    @Override
    public void start(ServiceStartContext serviceContext)
    {
        final CountersManager countersManager = countersInjector.getValue().getCountersManager();
        final IdleStrategy idleStrategy = createIdleStrategy(brokerIdleStrategy);
        final AgentRunnerFactory agentRunnerFactory = new DefaultAgentRunnerFactory(countersManager, idleStrategy);

        if (availableThreads >= 5)
        {
            final int threads = availableThreads - 5;
            final int logStreamProcessorThreadCount = 1 + (int) Math.ceil(threads / 2);
            final int logAppenderThreadCount = 1 + (int) Math.floor(threads / 2);

            networkReceiverAgentRunnerService = new SharedAgentRunnerService(agentRunnerFactory, AGENT_NAME_NETWORK_RECEIVER);
            networkSenderAgentRunnerService = new SharedAgentRunnerService(agentRunnerFactory, AGENT_NAME_NETWORK_SENDER);
            logAppenderAgentRunnerService = new SharedAgentRunnerService(agentRunnerFactory, AGENT_NAME_LOG_APPENDER, logAppenderThreadCount);
            logStreamProcessorAgentRunnerService = new SharedAgentRunnerService(agentRunnerFactory, AGENT_NAME_LOG_STREAM_PROCESSOR, logStreamProcessorThreadCount);
            conductorAgentRunnerService = new SharedAgentRunnerService(agentRunnerFactory, AGENT_NAME_CONDUCTOR);
        }
        else if (availableThreads == 4)
        {
            networkReceiverAgentRunnerService = new SharedAgentRunnerService(agentRunnerFactory, AGENT_NAME_NETWORK_RECEIVER);
            logAppenderAgentRunnerService = new SharedAgentRunnerService(agentRunnerFactory, AGENT_NAME_LOG_APPENDER);
            logStreamProcessorAgentRunnerService = new SharedAgentRunnerService(agentRunnerFactory, AGENT_NAME_LOG_STREAM_PROCESSOR);

            final CompositeAgentRunnerServiceFactory compositeAgentRunnerServiceFactory = new CompositeAgentRunnerServiceFactory(agentRunnerFactory,
                    AGENT_NAME_NETWORK_SENDER,
                    AGENT_NAME_CONDUCTOR);

            networkSenderAgentRunnerService = compositeAgentRunnerServiceFactory.createAgentRunnerService(AGENT_NAME_NETWORK_SENDER);
            conductorAgentRunnerService = compositeAgentRunnerServiceFactory.createAgentRunnerService(AGENT_NAME_CONDUCTOR);
        }
        else if (availableThreads == 3)
        {
            networkReceiverAgentRunnerService = new SharedAgentRunnerService(agentRunnerFactory, AGENT_NAME_NETWORK_RECEIVER);
            logAppenderAgentRunnerService = new SharedAgentRunnerService(agentRunnerFactory, AGENT_NAME_LOG_APPENDER);

            final CompositeAgentRunnerServiceFactory compositeAgentRunnerServiceFactory = new CompositeAgentRunnerServiceFactory(agentRunnerFactory,
                    AGENT_NAME_LOG_STREAM_PROCESSOR,
                    AGENT_NAME_NETWORK_SENDER,
                    AGENT_NAME_CONDUCTOR);

            logStreamProcessorAgentRunnerService = compositeAgentRunnerServiceFactory.createAgentRunnerService(AGENT_NAME_LOG_STREAM_PROCESSOR);
            networkSenderAgentRunnerService = compositeAgentRunnerServiceFactory.createAgentRunnerService(AGENT_NAME_NETWORK_SENDER);
            conductorAgentRunnerService = compositeAgentRunnerServiceFactory.createAgentRunnerService(AGENT_NAME_CONDUCTOR);
        }
        else if (availableThreads == 2)
        {
            CompositeAgentRunnerServiceFactory compositeAgentRunnerServiceFactory = new CompositeAgentRunnerServiceFactory(agentRunnerFactory,
                    AGENT_NAME_NETWORK_RECEIVER,
                    AGENT_NAME_LOG_APPENDER);

            networkReceiverAgentRunnerService = compositeAgentRunnerServiceFactory.createAgentRunnerService(AGENT_NAME_NETWORK_RECEIVER);
            logAppenderAgentRunnerService = compositeAgentRunnerServiceFactory.createAgentRunnerService(AGENT_NAME_LOG_APPENDER);

            compositeAgentRunnerServiceFactory = new CompositeAgentRunnerServiceFactory(agentRunnerFactory,
                    AGENT_NAME_LOG_STREAM_PROCESSOR,
                    AGENT_NAME_NETWORK_SENDER,
                    AGENT_NAME_CONDUCTOR);

            logStreamProcessorAgentRunnerService = compositeAgentRunnerServiceFactory.createAgentRunnerService(AGENT_NAME_LOG_STREAM_PROCESSOR);
            networkSenderAgentRunnerService = compositeAgentRunnerServiceFactory.createAgentRunnerService(AGENT_NAME_NETWORK_SENDER);
            conductorAgentRunnerService = compositeAgentRunnerServiceFactory.createAgentRunnerService(AGENT_NAME_CONDUCTOR);
        }
        else
        {
            final CompositeAgentRunnerServiceFactory compositeAgentRunnerServiceFactory = new CompositeAgentRunnerServiceFactory(agentRunnerFactory,
                    AGENT_NAME_NETWORK_RECEIVER,
                    AGENT_NAME_LOG_APPENDER,
                    AGENT_NAME_LOG_STREAM_PROCESSOR,
                    AGENT_NAME_NETWORK_SENDER,
                    AGENT_NAME_CONDUCTOR);

            networkReceiverAgentRunnerService = compositeAgentRunnerServiceFactory.createAgentRunnerService(AGENT_NAME_NETWORK_RECEIVER);
            logAppenderAgentRunnerService = compositeAgentRunnerServiceFactory.createAgentRunnerService(AGENT_NAME_LOG_APPENDER);
            logStreamProcessorAgentRunnerService = compositeAgentRunnerServiceFactory.createAgentRunnerService(AGENT_NAME_LOG_STREAM_PROCESSOR);
            networkSenderAgentRunnerService = compositeAgentRunnerServiceFactory.createAgentRunnerService(AGENT_NAME_NETWORK_SENDER);
            conductorAgentRunnerService = compositeAgentRunnerServiceFactory.createAgentRunnerService(AGENT_NAME_CONDUCTOR);
        }
    }

    protected IdleStrategy createIdleStrategy(BrokerIdleStrategy idleStrategy)
    {
        switch (idleStrategy)
        {
            case BUSY_SPIN:
                return new BusySpinIdleStrategy();
            default:
                return new BackoffIdleStrategy(1000, 100, 100, TimeUnit.MILLISECONDS.toNanos(maxIdleTimeMs));
        }
    }

    @Override
    public void stop(ServiceStopContext stopContext)
    {
        try
        {
            networkReceiverAgentRunnerService.close();
            logStreamProcessorAgentRunnerService.close();
            logAppenderAgentRunnerService.close();
            networkSenderAgentRunnerService.close();
            conductorAgentRunnerService.close();
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }

        for (AtomicCounter atomicCounter : errorCounters)
        {
            atomicCounter.close();
        }
    }

    @Override
    public AgentRunnerServices get()
    {
        return this;
    }

    public Injector<Counters> getCountersManagerInjector()
    {
        return countersInjector;
    }

    @Override
    public AgentRunnerService networkReceiverAgentRunnerService()
    {
        return networkReceiverAgentRunnerService;
    }

    @Override
    public AgentRunnerService networkSenderAgentRunnerService()
    {
        return networkSenderAgentRunnerService;
    }

    @Override
    public AgentRunnerService logAppenderAgentRunnerService()
    {
        return logAppenderAgentRunnerService;
    }

    @Override
    public AgentRunnerService logStreamProcessorAgentRunnerService()
    {
        return logStreamProcessorAgentRunnerService;
    }

    @Override
    public AgentRunnerService conductorAgentRunnerSerive()
    {
        return conductorAgentRunnerService;
    }

    class DefaultAgentRunnerFactory implements AgentRunnerFactory
    {
        private final CountersManager countersManager;
        private final IdleStrategy idleStrategy;

        DefaultAgentRunnerFactory(CountersManager countersManager, IdleStrategy idleStrategy)
        {
            this.countersManager = countersManager;
            this.idleStrategy = idleStrategy;
        }

        @Override
        public AgentRunner createAgentRunner(Agent agent)
        {
            final String errorCounterName = String.format("%s.errorCounter", agent.roleName());
            final AtomicCounter errorCounter = countersManager.newCounter(errorCounterName);
            errorCounters.add(errorCounter);
            return new AgentRunner(idleStrategy, (t) -> t.printStackTrace(), errorCounter, agent);
        }
    };

}
