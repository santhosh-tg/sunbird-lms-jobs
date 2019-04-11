package org.sunbird.jobs.samza.task;

import org.apache.samza.config.Config;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.InitableTask;
import org.apache.samza.task.TaskContext;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.TaskCoordinator;
import org.sunbird.jobs.samza.service.Service;
import org.sunbird.jobs.samza.util.JobLogger;

import java.util.HashMap;
import java.util.Map;

public class IndexerTask implements StreamTask, InitableTask {

    private JobLogger LOGGER = new JobLogger(IndexerTask.class);
    private static Service service = new Service();

    @Override
    public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) throws Exception {
        Map<String, Object> event = getMessage(envelope);
        LOGGER.info("event = " + event);
        service.process(event);
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> getMessage(IncomingMessageEnvelope envelope) {
        try {
            return (Map<String, Object>) envelope.getMessage();
        } catch (Exception e) {
            e.printStackTrace();
            LOGGER.error("IndexerTask:getMessage: Invalid message = " + envelope.getMessage(), e);
            return new HashMap<String, Object>();
        }
    }

    @Override
    public void init(Config config, TaskContext taskContext) throws Exception {
        LOGGER.info("Config values = " + config.toString());
    }
}
