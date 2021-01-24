package com.heb.pm.core.service.maintenance.ecommerce;

import com.heb.pm.core.event.LegacyEventProcessor;
import com.heb.pm.dao.core.entity.ProductOnline;
import com.heb.pm.core.model.ProductOnlineRange;
import com.heb.pm.core.model.SalesChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Service that manages product online (Show on Site) information.
 *
 * @author d116773
 * @since 1.22.0
 */
@Service
public class ProductOnlineService {

	private static final Logger logger = LoggerFactory.getLogger(ProductOnlineService.class);

	private final transient JdbcTemplate jdbcTemplate;
	private final transient LegacyEventProcessor legacyEventProcessor;

	private final transient ProductOnlineLookup productOnlineLookup;

	@Value("${app.operation.updateProductOnline}")
	private transient String operationName;

	/**
	 * Constructs a new ProductOnlineService.
	 *
	 * @param jdbcTemplate The JdbcTemplate to use to run queries.
	 * @param legacyEventProcessor The LegacyEventProcessor to use to save off generated events.
	 */
	@Autowired
	public ProductOnlineService(JdbcTemplate jdbcTemplate, LegacyEventProcessor legacyEventProcessor) {

		this.jdbcTemplate = jdbcTemplate;
		this.legacyEventProcessor = legacyEventProcessor;
		this.productOnlineLookup = new ProductOnlineLookup(jdbcTemplate);
	}

	/**
	 * Returns the name of the operation to use if updates are being performed through an online transaction.
	 *
	 * @return The name of the operation to use if updates are being performed through an online transaction.
	 */
	public String getOperationName() {

		if (Objects.isNull(this.operationName)) {
			this.operationName = "I18J006";
		}
		return this.operationName;
	}

	/**
	 * Sets The name of the operation to use if updates are being performed through an online transaction.
	 *
	 * @param operationName The name of the operation to use if updates are being performed through an online transaction.
	 * @return This object for further configuration.
	 */
	public ProductOnlineService setOperationName(String operationName) {
		this.operationName = operationName;
		return this;
	}

	/**
	 * Returns a list of ProductOnlineRange for a given product or product group.
	 *
	 * It only returns current and future events.
	 *
	 * This will throw an error if the product does not exist and return an empty list if the product or group
	 * exists but there is no product online information.
	 *
	 * @param productId The ID of product or product group to look for information for.
	 * @return The list of ProductOnlineRanges for the supplied product ID.
	 */
	public List<ProductOnlineRange> getByProductId(Long productId) {

		return this.productOnlineLookup.getByProductId(productId)
				.stream()
				.map(ProductOnlineService::productOnlineToRange)
				.collect(Collectors.toList());
	}

	/**
	 * Returns a list of ProductOnlineRange for a given product or product group for a given sales channel..
	 *
	 * It only returns current and future events.
	 *
	 * This will throw an error if the product or sales channel does not exist and return an empty list if
	 * the product or group and sales channel exist but there is no product online information.
	 *
	 * @param productId The ID of product or product group to look for information for.
	 * @param salesChannelCode The sales channel code to look for information for.
	 * @return The list of ProductOnlineRanges for the supplied product ID.
	 */
	public List<ProductOnlineRange> getByProductIdAndSalesChannel(Long productId, String salesChannelCode) {

		return this.productOnlineLookup.readListByProductAndSalesChannel(productId, salesChannelCode)
				.stream()
				.map(ProductOnlineService::productOnlineToRange)
				.collect(Collectors.toList());
	}

	/**
	 * Handles processing a ExtendedProductOnlineRequest.
	 *
	 * @param request The user's request.
	 * @return The updated list of ProductOnlineRanges after the request is complete.
	 */
	public List<ProductOnlineRange> processRequest(ExtendedProductOnlineRequest request) {

		ProductOnlineMaintenance productOnlineMaintenance = new ProductOnlineMaintenance(this.jdbcTemplate,
				this.legacyEventProcessor, this.getOperationName(), logger);

		return productOnlineMaintenance.processRequest(request)
				.stream()
				.map(ProductOnlineService::productOnlineToRange)
				.collect(Collectors.toList());
	}

	/**
	 * Converts a ProductOnline entity object to a ProductOnlineRange model object.
	 *
	 * @param productOnline The ProductOnline entity to convert.
	 * @return The converted ProductOnlineRange.
	 */
	private static ProductOnlineRange productOnlineToRange(ProductOnline productOnline) {

		SalesChannel salesChannel = SalesChannel.of(productOnline.getKey().getSalesChannelCode().getId(),
				productOnline.getKey().getSalesChannelCode().getDescription());

		return ProductOnlineRange.of(salesChannel, productOnline.getKey().getEffectiveDate(), productOnline.getExpirationDate());
	}
}
