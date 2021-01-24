package com.heb.pm.batch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.configuration.JobRegistry;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.support.JobRegistryBeanPostProcessor;
import org.springframework.batch.core.explore.support.JobExplorerFactoryBean;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.support.JobRepositoryFactoryBean;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.annotation.*;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;

/**
 * Provides configuration for the application when running in non-local profiles.
 *
 * @author s573181
 * @since 1.0.0
 */
@Configuration
@EnableBatchProcessing
@Profile({"dev", "cert", "prod"})
@ImportResource("classpath:/jobs.xml")
public class DeployedBatchConfiguration {

	private static final Logger logger = LoggerFactory.getLogger(DeployedBatchConfiguration.class);

	/**
	 * Creates the DB connection to the job database.
	 *
	 * @return The DB connection to the job database.
	 */
	@Bean(name = "jobDataSource")
	@ConfigurationProperties(prefix = "job.datasource")
	public DataSource jobDataSource() {
		return DataSourceBuilder.create().build();
	}

	/**
	 * Create a transaction manager for the job repository.
	 *
	 * @return A transaction manager for the job repository.
	 */
	@Bean(name = "jobTransactionManager")
	public PlatformTransactionManager jobTransactionManager(@Qualifier("jobDataSource") DataSource dataSource) {
		return new DataSourceTransactionManager(dataSource);
	}

	/**
	 * Creates the job repository factory.
	 *
	 * @param txManager The transaction manager for the repository to use.
	 * @return The job repository factory.
	 */
	@Bean(name = "jobRepositoryFactory")
	public JobRepositoryFactoryBean jobRepositoryFactory(
			@Qualifier("jobDataSource") DataSource dataSource,
			@Qualifier("jobTransactionManager") PlatformTransactionManager txManager) {

		logger.debug("Instantiating DB job repository factory");

		JobRepositoryFactoryBean jobRepositoryFactoryBean = new JobRepositoryFactoryBean();
		jobRepositoryFactoryBean.setDataSource(dataSource);
		jobRepositoryFactoryBean.setTransactionManager(txManager);
		jobRepositoryFactoryBean.setTablePrefix("spm.BATCH_");
		jobRepositoryFactoryBean.setIsolationLevelForCreate("ISOLATION_READ_COMMITTED");
		return jobRepositoryFactoryBean;
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
	public JobRepository jobRepository(@Qualifier("jobRepositoryFactory") JobRepositoryFactoryBean factoryBean) throws Exception {

		logger.debug("Instantiating map job repository");

		return factoryBean.getObject();
	}

	/**
	 * Creates the job explorer.
	 *
	 * @param dataSource The data source to use when creating the job explorer.
	 * @return The job explorer.
	 */
	@Bean
	@Primary
	public JobExplorerFactoryBean jobExplorer(@Qualifier("jobDataSource") DataSource dataSource) {

		logger.debug("Instantiating job explorer");

		JobExplorerFactoryBean jobExplorerFactoryBean = new JobExplorerFactoryBean();
		jobExplorerFactoryBean.setDataSource(dataSource);
		jobExplorerFactoryBean.setTablePrefix("spm.BATCH_");
		return jobExplorerFactoryBean;
	}

	/**
	 * Creates the job launcher.
	 *
	 * @param jobRepository The job repository to use.
	 * @return The job jauncher.
	 */
	@Bean
	public JobLauncher jobLauncher(JobRepository jobRepository) {

		logger.debug("Instantiating job launcher");

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
	 * Constructs a JobRegistryBeanPostProcessor. This was needed to be able to find jobs programatically, though
	 * I'm not positive as to why.
	 *
	 * @return A JobRegistryBeanPostProcessor.
	 * @throws Exception
	 */
	@Bean
	public JobRegistryBeanPostProcessor jobRegistryBeanPostProcessor(JobRegistry jobRegistry) throws Exception {

		JobRegistryBeanPostProcessor jobRegistryBeanPostProcessor = new JobRegistryBeanPostProcessor();
		jobRegistryBeanPostProcessor.setJobRegistry(jobRegistry);
		return jobRegistryBeanPostProcessor;
	}
}
