package com.heb.pm.core.service.maintenance.ecommerce;

import com.heb.pm.core.event.LegacyEventProcessor;
import com.heb.pm.core.exception.ValidationException;
import com.heb.pm.core.maintenance.ProductOnlineRequest;
import com.heb.pm.core.service.maintenance.common.TableUpdateSets;
import com.heb.pm.dao.core.DatabaseConstants;
import com.heb.pm.dao.core.LegacyEventGenerator;
import com.heb.pm.dao.core.entity.LegacyEvent;
import com.heb.pm.dao.core.entity.ProductOnline;
import com.heb.pm.dao.core.entity.codes.LegacyEventFunction;
import com.heb.pm.dao.core.entity.codes.SalesChannelType;
import com.heb.pm.dao.core.preparedstatementsetters.ProductOnlineUpdater;
import com.heb.pm.dao.core.preparedstatementsetters.UpdateType;
import com.heb.pm.dao.core.quicklookup.ProductLookup;
import com.heb.pm.dao.core.rowmappers.ProductOnlineRowMapper;
import com.heb.pm.util.DateUtils;
import com.heb.pm.util.InstantUtils;
import com.heb.pm.util.JdbcUtils;
import org.slf4j.Logger;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.annotation.Transactional;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Class that manages updating product online information.
 *
 * @author d116773
 * @since 1.22.0
 */
// Since this can be called from batch jobs, I want to allow for different loggers. To do this, it is
// passed in as a parameter to the constructor, so the logger can't be static or final.
@SuppressWarnings("PMD.LoggerIsNotStaticFinal")
public class ProductOnlineMaintenance {

	private static final String NOT_FOUND_ERROR = "%d is neither a product ID nor a product group ID.";

	private static final String AUDIT_INSERT_SQL = "INSERT INTO EMD.PRODUCT_ONLINE_AUD (PROD_ID, SALS_CHNL_CD, EFF_DT, " +
			"AUD_REC_CRE8_TS, ACT_CD, EXPRN_DT, PROD_ID_SW, CRE8_TS, CRE8_UID, LST_UPDT_TS, LST_UPDT_UID) " +
			"SELECT PROD_ID, SALS_CHNL_CD, EFF_DT, SYSDATE, 'PURGE', EXPRN_DT, PROD_ID_SW, CRE8_TS, CRE8_UID, LST_UPDT_TS, LST_UPDT_UID " +
			"FROM EMD.PRODUCT_ONLINE WHERE PROD_ID = ? AND SALS_CHNL_CD = ? AND EFF_DT = ?";

	private static final String SELECT_ACTIVE_FOR_DATE = ProductOnlineRowMapper.SELECT_SQL +
			" WHERE PROD_ID = ? AND SALS_CHNL_CD = ? AND EXPRN_DT > ?";

	private static final ProductOnlineRowMapper PRODUCT_ONLINE_ROW_MAPPER = new ProductOnlineRowMapper();

	private final transient JdbcTemplate jdbcTemplate;
	private final transient String jobName;
	private final transient Logger logger;
	private final transient ProductOnlineLookup productOnlineLookup;
	private final transient LegacyEventProcessor legacyEventProcessor;
	private final transient ProductLookup productLookup;

	/**
	 * Constructs a new ProductOnlineMaintenance.
	 *
	 * @param jdbcTemplate The JdbcTemplate to use to run queries.
	 * @param legacyEventProcessor The LegacyEventProcessor to save legacy events with.
	 * @param jobName The name of the job or operation doing these changes.
	 * @param logger The logger for the class to use.
	 */
	public ProductOnlineMaintenance(JdbcTemplate jdbcTemplate, LegacyEventProcessor legacyEventProcessor, String jobName, Logger logger) {
		this.jdbcTemplate = jdbcTemplate;
		this.jobName = jobName;
		this.logger = logger;
		this.productOnlineLookup = new ProductOnlineLookup(jdbcTemplate);
		this.legacyEventProcessor = legacyEventProcessor;
		this.productLookup = new ProductLookup(this.jdbcTemplate);
	}

	/**
	 * Handles processing a ExtendedProductOnlineRequest.
	 *
	 * @param request The user's request.
	 * @return The updated list of ProductOnlineRanges after the request is complete.
	 */
	@Transactional
	public List<ProductOnline> processRequest(ExtendedProductOnlineRequest request) {

		this.validateRequest(request);
		this.determineIdType(request);

		if (ProductOnlineRequest.RequestType.ADD.equals(request.getRequestType())) {

			return this.processAdd(request);
		} else {

			return this.processEnd(request);
		}
	}

	/**
	 * Handles updating the end date of an existing product online record.
	 *
	 * @param request The user's request to update the end date of a product online event.
	 * @return The updated list of ProductOnlineRanges after the request is complete.
	 */
	@Transactional
	public List<ProductOnline> processEnd(ExtendedProductOnlineRequest request) {

		// Get the product_online records to update.
		List<ProductOnline> existingProductOnlines = this.jdbcTemplate.query(SELECT_ACTIVE_FOR_DATE,
				JdbcUtils.argsAsArray(request.getProductId(), request.getSalesChannel().getSalesChannelCode(),
						java.sql.Date.valueOf(request.getEndDate())), PRODUCT_ONLINE_ROW_MAPPER);

		// Find which records need to be updated or deleted.
		TableUpdateSets<ProductOnline> recordsToUpdate = this.processEnd(request, existingProductOnlines);

		this.issueMaintenance(request, recordsToUpdate);

		// Fetch and return the updated data.
		return this.productOnlineLookup.readListByProductAndSalesChannel(request.getProductId(), request.getSalesChannel().getSalesChannelCode());
	}

	/**
	 * Determines which records need to be updated or deleted.
	 *
	 * @param request The user's request to update the end of a product oneline.
	 * @param productOnlinesToProcess The existing current or future records.
	 * @return The records that need to be updated or deleted.
	 */
	@Transactional
	protected TableUpdateSets<ProductOnline> processEnd(ExtendedProductOnlineRequest request, List<ProductOnline> productOnlinesToProcess) {

		TableUpdateSets<ProductOnline> updatesToMake = new TableUpdateSets<>();

		Instant endDate = InstantUtils.startOfDate(request.getEndDate());

		for (ProductOnline productOnline : productOnlinesToProcess) {

			// If the end date occurs during the product online event, change the end date and add it to the update list.
			if (productOnline.getKey().getEffectiveDate().isBefore(endDate) && endDate.isBefore(productOnline.getExpirationDate())) {
				productOnline.setExpirationDate(endDate)
						.setLastUpdateUserId(request.getUserId())
						.setLastUpdateTime(Instant.now());
				updatesToMake.getUpdates().add(productOnline);
			}

			// If this is a future, get rid of it.
			if (productOnline.getKey().getEffectiveDate().isAfter(endDate)) {
				updatesToMake.getDeletes().add(productOnline);
				productOnline.setLastUpdateUserId(request.getUserId()); // This is just to get the right user ID in the message.
			}
		}

		return updatesToMake;
	}


	/**
	 * Handles adding a new time-frame a series of product online events. This may mean extending existing timeframes
	 * or adding a new one.
	 *
	 * @param request The user's request to update to add a time-frame to the product online series.
	 * @return The updated list of ProductOnlineRanges after the request is complete.
	 */
	@Transactional
	public List<ProductOnline> processAdd(ExtendedProductOnlineRequest request) {

		// Get all the existing records.
		List<ProductOnline> existingProductOnlines = this.productOnlineLookup.readListByProductAndSalesChannel(request.getProductId(),
				request.getSalesChannel().getSalesChannelCode());

		// Find which records need to be updated or deleted.
		TableUpdateSets<ProductOnline> recordsToUpdate = this.processAdd(request, existingProductOnlines);

		this.issueMaintenance(request, recordsToUpdate);

		// Fetch and return the updated data.
		return this.productOnlineLookup.readListByProductAndSalesChannel(request.getProductId(), request.getSalesChannel().getSalesChannelCode());
	}

	/**
	 * Holds the logic of what to do with all the exising and new product_online events.
	 *
	 * @param request The user's ExtendedProductOnlineRequest.
	 * @param existingProductOnlines The list of current and future product_online records.
	 * @return The collection of changes that need to be applied based on the current data and what the user is trying to do.
	 */
	@Transactional
	protected TableUpdateSets<ProductOnline> processAdd(ExtendedProductOnlineRequest request, List<ProductOnline> existingProductOnlines) {

		Instant startDate = InstantUtils.startOfDate(request.getEffectiveDate());
		Instant endDate = InstantUtils.startOfDate(request.getEndDate());

		TableUpdateSets<ProductOnline> updatesToMake = new TableUpdateSets<>();

		Instant todayAsInstant = InstantUtils.startOfToday();

		for (ProductOnline productOnline : existingProductOnlines) {

			// If it is a future, remove it
			if (productOnline.getKey().getEffectiveDate().isAfter(todayAsInstant)) {
				productOnline.setLastUpdateUserId(request.getUserId()); // This is for the user ID in the event.
				updatesToMake.getDeletes().add(productOnline);
				continue;
			}

			// If the one we are looking at is currently active, end it.
			if (productOnline.getExpirationDate().isAfter(todayAsInstant)) {

				productOnline.setExpirationDate(todayAsInstant)
						.setLastUpdateUserId(request.getUserId())
						.setLastUpdateTime(Instant.now());
				updatesToMake.getUpdates().add(productOnline);
			}
		}

		// Create a ProductOnline for the user's request.
		ProductOnline newEvent = ProductOnline.of(request.getProductId(),
				SalesChannelType.of(request.getSalesChannel().getSalesChannelCode()),
				startDate);

		newEvent.setLastUpdateTime(Instant.now())
				.setLastUpdateUserId(request.getUserId())
				.setCreateTime(Instant.now())
				.setCreateUserId(request.getUserId())
				.setProductIdSwitch(request.getIdIsProduct())
				.setExpirationDate(endDate);

		updatesToMake.getInserts().add(newEvent);

		return updatesToMake;
	}

	/**
	 * Does the work of calling the insert, update, and delete functions for a collection of ProductOnline updates and
	 * issuing the events for the changes.
	 *
	 * @param request The user's original ExtendedProductOnlineRequest.
	 * @param recordsToUpdate The collection of changes to make.
	 */
	@Transactional
	protected void issueMaintenance(ExtendedProductOnlineRequest request, TableUpdateSets<ProductOnline> recordsToUpdate) {

		List<LegacyEvent> legacyEvents = new LinkedList<>();

		// Issue the updates.
		if (!recordsToUpdate.getUpdates().isEmpty()) {
			legacyEvents.addAll(this.doUpdates(recordsToUpdate.getUpdates()));
		}

		// Issue any deletes.
		if (!recordsToUpdate.getDeletes().isEmpty()) {
			legacyEvents.addAll(this.doDeletes(recordsToUpdate.getDeletes()));
		}

		if (!recordsToUpdate.getInserts().isEmpty()) {
			// There will be at most one insert.
			legacyEvents.add(this.doInserts(recordsToUpdate.getInserts().iterator().next()));
		}

		// If anything was changed...
		if (!legacyEvents.isEmpty()) {

			logger.info(String.format("Ending events for product %d: %d updates and %d deletes.", request.getProductId(),
					recordsToUpdate.getUpdates().size(), recordsToUpdate.getDeletes().size()));

			// Add a PRM2 event and publish.
			legacyEvents.add(LegacyEventGenerator.generatePRM2(request.getProductId(), this.jobName, request.getUserId(), LegacyEventFunction.UPDATE));
			this.legacyEventProcessor.addAndFlush(legacyEvents);
		}
	}

	/**
	 * Handles the actual inserting of records into product_online.
	 *
	 * @param recordToInsert The ProductOnline object to insert.
	 * @return A list of LegacyEvents that are triggered by the insert.
	 */
	@Transactional
	protected LegacyEvent doInserts(ProductOnline recordToInsert) {

		ProductOnlineUpdater productOnlineUpdater = new ProductOnlineUpdater(List.of(recordToInsert), UpdateType.INSERT);

		this.jdbcTemplate.batchUpdate(ProductOnlineUpdater.INSERT_SQL, productOnlineUpdater);

		return LegacyEventGenerator.generatePRON(recordToInsert.getKey().getProductId(), recordToInsert.getKey().getSalesChannelCode().getId(),
				recordToInsert.getKey().getEffectiveDate(), this.jobName, recordToInsert.getLastUpdateUserId(), LegacyEventFunction.ADD);
	}

	/**
	 * Handles the actual updating of records in product_online.
	 *
	 * @param recordsToUpdate The ProductOnline objects to update.
	 * @return A list of LegacyEvents that are triggered by the updates.
	 */
	@Transactional
	protected List<LegacyEvent> doUpdates(Set<ProductOnline> recordsToUpdate) {

		ProductOnlineUpdater productOnlineUpdater = new ProductOnlineUpdater(recordsToUpdate, UpdateType.UPDATE);

		this.jdbcTemplate.batchUpdate(ProductOnlineUpdater.UPDATE_SQL, productOnlineUpdater);

		return recordsToUpdate.stream().map(r -> LegacyEventGenerator.generatePRON(r.getKey().getProductId(), r.getKey().getSalesChannelCode().getId(),
				r.getKey().getEffectiveDate(), this.jobName, r.getLastUpdateUserId(), LegacyEventFunction.UPDATE)).collect(Collectors.toList());
	}

	/**
	 * Handles the actual deleting of records from product_online.
	 *
	 * @param recordsToDelete The ProductOnline objects to delete.
	 * @return A list of LegacyEvents that are triggered by the deletes.
	 */
	@Transactional
	protected List<LegacyEvent> doDeletes(Set<ProductOnline> recordsToDelete) {

		// The delete trigger is missing from product_online, so we need to copy the stuff we're deleting into the audits table.
		recordsToDelete.forEach(d -> this.jdbcTemplate.update(AUDIT_INSERT_SQL,
				JdbcUtils.argsAsArray(d.getKey().getProductId(),
						d.getKey().getSalesChannelCode().getId(),
						Timestamp.from(d.getKey().getEffectiveDate()))));

		// Now we can do the deletes.
		ProductOnlineUpdater productOnlineUpdater = new ProductOnlineUpdater(recordsToDelete, UpdateType.DELETE);
		this.jdbcTemplate.batchUpdate(ProductOnlineUpdater.DELETE_SQL, productOnlineUpdater);

		return recordsToDelete.stream().map(r -> LegacyEventGenerator.generatePRON(r.getKey().getProductId(), r.getKey().getSalesChannelCode().getId(),
				r.getKey().getEffectiveDate(), this.jobName, r.getLastUpdateUserId(), LegacyEventFunction.DELETE)).collect(Collectors.toList());
	}

	/**
	 * Sets the idIsProduct property of a ExtendedProductOnlineRequest. This will be true if the productId field
	 * contains the ID of a product and false if it contains a product group.
	 *
	 * @param productOnlineRequest The ExtendedProductOnlineRequest to set the idIsProduct of. This object will be
	 *                             modified.
	 */
	private void determineIdType(ExtendedProductOnlineRequest productOnlineRequest) {

		productOnlineRequest.setIdIsProduct(this.productLookup.isProductId(productOnlineRequest.getProductId()));
	}

	/**
	 * Validates an ExtendedProductOnlineRequest to make sure it's OK to process.
	 *
	 * @param productOnlineUpdateRequest The request to validate.
	 */
	public void validateRequest(ExtendedProductOnlineRequest productOnlineUpdateRequest) {

		List<String> errors = new LinkedList<>(this.validateDatesForAdd(productOnlineUpdateRequest));

		errors.addAll(this.validateDatesForEnd(productOnlineUpdateRequest));

		if (Objects.isNull(productOnlineUpdateRequest.getUserId())) {
			errors.add("User ID is required.");
		}

		if (Objects.isNull(productOnlineUpdateRequest.getProductId())) {
			errors.add("Product ID is required.");
		} else {
			if (this.productOnlineLookup.notFound(productOnlineUpdateRequest.getProductId())) {
				errors.add(String.format(NOT_FOUND_ERROR, productOnlineUpdateRequest.getProductId()));
			}
		}

		if (Objects.isNull(productOnlineUpdateRequest.getSalesChannel())) {
			errors.add("Sales channel is required.");
		}

		if (Objects.isNull(productOnlineUpdateRequest.getRequestType())) {
			errors.add("A request type is required.");
		} else {
			try {
				SalesChannelType.of(productOnlineUpdateRequest.getSalesChannel().getSalesChannelCode());
			} catch (Exception e) {
				errors.add(String.format("'%s' is not a valid sales channel.", productOnlineUpdateRequest.getSalesChannel()));
			}
		}

		if (!errors.isEmpty()) {
			throw new ValidationException("Unable to validate product online request.", errors);
		}
	}

	private List<String> validateDatesForEnd(ExtendedProductOnlineRequest productOnlineRequest) {

		List<String> errors = new LinkedList<>();

		if (Objects.equals(ProductOnlineRequest.RequestType.END, productOnlineRequest.getRequestType())) {

			if (Objects.isNull(productOnlineRequest.getEndDate())) {
				errors.add("An end date is required.");
			} else {

				if (productOnlineRequest.getEndDate().isBefore(DateUtils.today())) {
					errors.add("End date must be greater than today.");
				}
			}
		}

		return errors;
	}

	private List<String> validateDatesForAdd(ExtendedProductOnlineRequest productOnlineRequest) {

		List<String> errors = new LinkedList<>();

		if (Objects.equals(ProductOnlineRequest.RequestType.ADD, productOnlineRequest.getRequestType())) {

			if (Objects.isNull(productOnlineRequest.getEffectiveDate()) || Objects.isNull(productOnlineRequest.getEndDate())) {
				errors.add("An effective and end date are required.");
			} else {

				if (productOnlineRequest.getEffectiveDate().isBefore(DateUtils.today())) {
					errors.add("Effective date must be today or greater.");
				}
				if (productOnlineRequest.getEndDate().isBefore(productOnlineRequest.getEffectiveDate())) {
					errors.add("End date must be after the effective date.");
				}
				if (productOnlineRequest.getEndDate().isAfter(DatabaseConstants.FOREVER_AS_DATE)) {
					errors.add("End date must be on or before 12/31/9999");
				}
			}

		}

		return errors;
	}
}
