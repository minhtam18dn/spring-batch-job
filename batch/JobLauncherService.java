package com.heb.pm.batch;

import org.springframework.batch.core.JobParametersBuilder;

/**
 * Interface for services to use to kick off internal batch jobs.
 *
 * @author d116773
 * @since 1.12.0
 */
public interface JobLauncherService {

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
	void launchJob(String jobName, JobParametersBuilder jobParametersBuilder, Boolean runAsynchronously);
}
