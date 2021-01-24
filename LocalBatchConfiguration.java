package com.heb.pm.batch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.configuration.JobRegistry;
import org.springframework.batch.core.configuration.support.JobRegistryBeanPostProcessor;
import org.springframework.batch.core.configuration.support.MapJobRegistry;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.explore.support.SimpleJobExplorer;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.support.MapJobRepositoryFactoryBean;
import org.springframework.batch.support.transaction.ResourcelessTransactionManager;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.*;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

/**
 * Sets up the batch configuration when running locally. This will use the map job repository rather than
 * connecting to a database.
 *
 * @author d116773
 * @since 1.10.0
 */
@Configuration
@Profile({"local", "test"})
@ImportResource("classpath:/jobs.xml")
public class LocalBatchConfiguration {

	private static final Logger logger = LoggerFactory.getLogger(LocalBatchConfiguration.class);

	/**
	 * Create a transaction manager for the job repository.
	 *
	 * @return A transaction manager for the job repository.
	 */
	@Bean(name = "jobTransactionManager")
	public PlatformTransactionManager jobTransactionManager() {
		return new ResourcelessTransactionManager();
	}

	/**
	 * Creates the job repository factory.
	 *
	 * @param txManager The transaction manager for the repository to use.
	 * @return The job repository factory.
	 * @throws Exception
	 */
	@Bean(name = "jobRepositoryFactory")
	public MapJobRepositoryFactoryBean mapJobRepositoryFactory(@Qualifier("jobTransactionManager")PlatformTransactionManager txManager) throws Exception {

		logger.info("Instantiating map job repository factory");

		MapJobRepositoryFactoryBean factory = new MapJobRepositoryFactoryBean(txManager);
		factory.afterPropertiesSet();
		return factory;
	}

	/**
	 * Creates the job repository.
	 *
	 * @param factoryBean The job repository factory bean used to create the job repository.
	 * @return The job repository.
	 * @throws Exception
	 */
	@Bean(name = "jobRepository")
	@Primary
	public JobRepository jobRepository(MapJobRepositoryFactoryBean factoryBean) throws Exception {

		logger.info("Instantiating map job repository");

		return factoryBean.getObject();
	}

	/**
	 * Creates the job explorer.
	 *
	 * @param factoryBean The job repository factory bean to use when creating the job explorer.
	 * @return The job explorer.
	 */
	@Bean
	@Primary
	public JobExplorer jobExplorer(MapJobRepositoryFactoryBean factoryBean) {

		logger.info("Instantiating job explorer");

		return new SimpleJobExplorer(factoryBean.getJobInstanceDao(), factoryBean.getJobExecutionDao(),
				factoryBean.getStepExecutionDao(), factoryBean.getExecutionContextDao());
	}

	/**
	 * Creates the job launcher.
	 *
	 * @param jobRepository The job repository to use.
	 * @return The job launcher.
	 */
	@Bean
	@Primary
	public JobLauncher jobLauncher(JobRepository jobRepository) {

		logger.info("Instantiating job launcher");

		SimpleJobLauncher simpleJobLauncher = new SimpleJobLauncher();
		simpleJobLauncher.setJobRepository(jobRepository);

		return simpleJobLauncher;
	}

	/**
	 * Creates a job launcher that will kick off jobs asynchronously.
	 *
	 * @param jobRepository The job repository to use.
	 * @return An asynchronous job launcher.
	 */
	@Bean(name = "asynchronousJobLauncher")
	public JobLauncher asynchronousJobLauncher(JobRepository jobRepository) {

		logger.info("Instantiating asynchronous job launcher");

		SimpleJobLauncher simpleJobLauncher = new SimpleJobLauncher();
		simpleJobLauncher.setJobRepository(jobRepository);
		simpleJobLauncher.setTaskExecutor(new SimpleAsyncTaskExecutor());
		return simpleJobLauncher;
	}

	/**
	 * Constructs an in-memory job registry.
	 *
	 * @return An in-memory job registry.
	 * @throws Exception
	 */
	@Bean
	public JobRegistry jobRegistry() throws Exception {

		logger.info("Instantiating job registry.");
		return new MapJobRegistry();
	}

	/**
	 * Constructs a JobRegistryBeanPostProcessor. This was needed to be able to find jobs programatically, though
	 * I'm not positive as to why.
	 *
	 * @return A JobRegistryBeanPostProcessor.
	 * @throws Exception
	 */
	@Bean
	public JobRegistryBeanPostProcessor jobRegistryBeanPostProcessor() throws Exception {

		JobRegistryBeanPostProcessor jobRegistryBeanPostProcessor = new JobRegistryBeanPostProcessor();
		jobRegistryBeanPostProcessor.setJobRegistry(jobRegistry());
		return jobRegistryBeanPostProcessor;
	}
}
