package com.heb.pm.batch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.configuration.JobRegistry;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.NoSuchJobException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.Objects;

/**
 * Service to use to kick off internal batch jobs.
 *
 * @author d116773
 * @since 1.12.0
 */
@Service("jobLauncherService")
public class JobLauncherServiceImpl implements JobLauncherService {

	private static final Logger logger = LoggerFactory.getLogger(JobLauncherServiceImpl.class);

	@Autowired
	private transient JobRegistry jobRegistry;

	@Autowired
	private transient JobLauncher jobLauncher;

	@Autowired
	@Qualifier("asynchronousJobLauncher")
	private transient JobLauncher asynchronousJobLauncher;

	/**
	 * Kicks off a job. This supports running the job synchronously or asynchronously.
	 *
	 * When running synchronously, the method will wait for the job to complete. If the job fails,
	 * then the method will throw a {@link JobFailedException} with the exit message from Spring.
	 *
	 * When running asynchronously, the method will return after kicking off the job. Any error in trying to start
	 * the job will throw a {@link JobFailedException}. It will be the responsibility of the job or the calling
	 * function to monitor the job if needed.
	 *
	 * If the job is not found, the method will throw an IllegalArgumentException.
	 *
	 * @param jobName The name of the job to run.
	 * @param jobParametersBuilder The JobParametersBuilder to use to construct the parameters to the job.
	 * @param runAsynchronously Whether or not to run the job asynchronously. This will default to false.
	 */
	@Override
	// We need this in case we need to annotate any method calling this one as @Transactional. This will keep
	// the job from throwing an error since it's starting inside an existing transaction.
	@Transactional(propagation = Propagation.NOT_SUPPORTED)
	public void launchJob(String jobName, JobParametersBuilder jobParametersBuilder, Boolean runAsynchronously)  {

		Job j;

		logger.info(String.format("Launching job %s.", jobName));

		// Get the job from the job repository.
		try {

			j = this.jobRegistry.getJob(jobName);
			logger.debug(String.format("Job %s found.", jobName));
		} catch (NoSuchJobException e) {

			throw new IllegalArgumentException(String.format("Job %s is not defined.", jobName));
		}

		try {

			if (Objects.nonNull(runAsynchronously) && runAsynchronously) {

				logger.debug(String.format("Starting job %s asynchronously.", jobName));
				this.asynchronousJobLauncher.run(j, jobParametersBuilder.toJobParameters());
				logger.debug(String.format("Started job %s asynchronously.", jobName));
			} else {

				logger.debug(String.format("Starting job %s synchronously.", jobName));
				JobExecution jobExecution = this.jobLauncher.run(j, jobParametersBuilder.toJobParameters());
				if (!ExitStatus.COMPLETED.getExitCode().equals(jobExecution.getExitStatus().getExitCode())) {
					throw new JobFailedException(jobExecution.getExitStatus());
				}
			}
        } catch (JobFailedException jobEx) {

            logger.error("Error:", jobEx);
            // When job failed exception occurs, then throw it.
            throw jobEx;
        } catch (Exception e) {

			logger.error("Error:", e);
            throw new JobFailedException(ExitStatus.FAILED);
        }
	}
}
