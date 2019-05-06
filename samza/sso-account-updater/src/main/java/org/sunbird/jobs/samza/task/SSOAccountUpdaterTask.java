package org.sunbird.jobs.samza.task;

import java.util.HashMap;
import java.util.Map;

import org.apache.samza.config.Config;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.task.InitableTask;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskContext;
import org.apache.samza.task.TaskCoordinator;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.jobs.samza.service.SSOAccountUpdaterService;
import org.sunbird.jobs.samza.util.JobLogger;

public class SSOAccountUpdaterTask implements StreamTask, InitableTask {

    private static SSOAccountUpdaterService service = new SSOAccountUpdaterService();
    private JobLogger Logger = new JobLogger(SSOAccountUpdaterService.class);

    @Override
    public void init(Config config, TaskContext context) throws Exception {
        try {
            service.initialize(config);
            Logger.info("SSOAccountUpdaterTask:init: Task initialized");
        } catch (Exception e) {
            Logger.error("SSOAccountUpdaterTask:init: Task initialization failed", e);
            throw e;
        }
    }

    @Override
    public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) throws Exception {
        Map<String, Object> message = getMessage(envelope);
        try {
            service.processMessage(message);
        } catch (ProjectCommonException e) {
            Logger.error("SSOAccountUpdaterTask:process: Error while processing message", message, e);
        } catch (Exception e) {
            Logger.error("SSOAccountUpdaterTask:process: Generic error while processing message", message, e);
            throw e;
        }
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> getMessage(IncomingMessageEnvelope envelope) {
        try {
            return (Map<String, Object>) envelope.getMessage();
        } catch (Exception e) {
            Logger.error("SSOAccountUpdaterTask:getMessage: Invalid message = " + envelope.getMessage(), e);
            return new HashMap();
        }
    }

}
