package uk.co.finleyofthewoods.simplebatchkafkaprocessor.schedulers;

import net.javacrumbs.shedlock.spring.annotation.SchedulerLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.job.parameters.JobParameters;
import org.springframework.batch.core.job.parameters.JobParametersBuilder;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class JobScheduler {
    private static final Logger logger = LoggerFactory.getLogger(JobScheduler.class);
    private final JobOperator operator;
    private final Job job;

    public JobScheduler(JobOperator operator, Job job) {
        this.operator = operator;
        this.job = job;
    }

    @Scheduled(
            fixedDelayString = "${spring.batch.job.scheduling.fixed-delay}",
            initialDelayString = "${spring.batch.job.scheduling.initial-delay}")
    @SchedulerLock(
            name = "batch-kafka-job",
            lockAtMostFor = "${shedlock.lock-at-most-for}",
            lockAtLeastFor = "${shedlock.lock-at-least-for}")
    public void runJob() {
        try {
            JobParameters params = new JobParametersBuilder()
                    .addLong("timestamp", System.currentTimeMillis())
                    .toJobParameters();

            logger.info("Starting job: {}", job.getName());
            JobExecution jobExecution = operator.start(job, params);
            logger.info("Job started. Job ID: {}, Status: {}", jobExecution.getId(), jobExecution.getStatus());
        } catch (Exception e) {
            logger.error("Error running job", e);
            throw new RuntimeException("Failed to run batch job", e);
        }
    }
}
