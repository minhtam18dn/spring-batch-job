package com.heb.pm.core.service.maintenance.ecommerce;

import com.heb.pm.core.event.LegacyEventProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;
import java.util.List;
import java.util.Objects;

/**
 * Service for processing ProductRelationship data.
 *
 * @author d116773
 * @since 1.23.0
 */
public class ProductRelationshipService {

	private static final Logger logger = LoggerFactory.getLogger(ProductRelationshipService.class);

	private final transient JdbcTemplate jdbcTemplate;

	private final transient LegacyEventProcessor legacyEventProcessor;

	@Value("${app.operation.updateProductRelationship}")
	private transient String operationName;

	/**
	 * Constructs a new ProductRelationshipService.
	 *
	 * @param dataSource The DataSource to use to run queries against.
	 * @param legacyEventProcessor The LegacyEventProcessor to use to save off generated events.
	 */
	public ProductRelationshipService(DataSource dataSource, LegacyEventProcessor legacyEventProcessor) {

		this.jdbcTemplate = new JdbcTemplate(dataSource);
		this.legacyEventProcessor = legacyEventProcessor;
	}

	/**
	 * Updates a ProductRelationship.
	 *
	 * @param relationshipRequest The request to update a ProductRelationship.
	 */
	public void updateProductRelationship(ExtendedProductRelationshipRequest relationshipRequest) {

		ProductRelationshipMaintenance productRelationshipMaintenance = new ProductRelationshipMaintenance(this.jdbcTemplate,
				this.getOperationName(), logger, legacyEventProcessor);

		productRelationshipMaintenance.updateProductRelationship(List.of(relationshipRequest));
	}


	/**
	 * Returns the name of the operation to use if updates are being performed through an online transaction.
	 *
	 * @return The name of the operation to use if updates are being performed through an online transaction.
	 */
	public String getOperationName() {

		if (Objects.isNull(this.operationName)) {
			this.operationName = "I18J007";
		}
		return this.operationName;
	}

	/**
	 * Sets The name of the operation to use if updates are being performed through an online transaction.
	 *
	 * @param operationName The name of the operation to use if updates are being performed through an online transaction.
	 * @return This object for further configuration.
	 */
	public ProductRelationshipService setOperationName(String operationName) {
		this.operationName = operationName;
		return this;
	}
}
