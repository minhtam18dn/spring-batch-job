package com.heb.pm.core.service.maintenance.ecommerce;

import com.heb.pm.core.exception.ValidationException;
import com.heb.pm.core.maintenance.FulfillmentChannelRequest;
import com.heb.pm.core.service.maintenance.common.MaintenanceConfig;
import com.heb.pm.core.service.maintenance.common.TableUpdateSets;
import com.heb.pm.dao.core.DatabaseConstants;
import com.heb.pm.dao.core.LegacyEventGenerator;
import com.heb.pm.dao.core.entity.LegacyEvent;
import com.heb.pm.dao.core.entity.ProductFulfillmentChannel;
import com.heb.pm.dao.core.entity.ProductFulfillmentChannelKey;
import com.heb.pm.dao.core.entity.codes.FulfillmentChannelType;
import com.heb.pm.dao.core.entity.codes.LegacyEventFunction;
import com.heb.pm.dao.core.entity.codes.SalesChannelType;
import com.heb.pm.dao.core.preparedstatementsetters.ProductFulfillmentChannelUpdater;
import com.heb.pm.dao.core.preparedstatementsetters.UpdateType;
import com.heb.pm.dao.core.quicklookup.ProductLookup;
import com.heb.pm.util.DateUtils;
import com.heb.pm.util.InstantUtils;
import com.heb.pm.util.JdbcUtils;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Class that manages updating fulfillment channel information.
 *
 * @author m314029
 * @since 1.24.0
 */
// Since this can be called from batch jobs, I want to allow for different loggers. To do this, it is
// passed in as a parameter to the constructor, so the logger can't be static or final.
@SuppressWarnings("PMD.LoggerIsNotStaticFinal")
public class FulfillmentChannelMaintenance {

	private static final String AUDIT_FOR_DELETE_SQL = "INSERT INTO EMD.PROD_FLFL_CHNL_AUD (PROD_ID, SALS_CHNL_CD, FLFL_CHNL_CD, " +
			"AUD_REC_CRE8_TS, ACT_CD, EFF_DT, EXPRN_DT, LST_UPDT_TS, LST_UPDT_UID) " +
			"SELECT PROD_ID, SALS_CHNL_CD, FLFL_CHNL_CD, SYSDATE, 'PURGE', EFF_DT, EXPRN_DT, LST_UPDT_TS, LST_UPDT_UID " +
			"FROM EMD.PROD_FLFL_CHNL WHERE PROD_ID = ? AND SALS_CHNL_CD = ? AND FLFL_CHNL_CD = ?";


	private final transient FulfillmentChannelLookup fulfillmentChannelLookup;
	private final transient ProductLookup productLookup;
	private final transient MaintenanceConfig config;

	/**
	 * Constructs a new FulfillmentChannelMaintenance.
	 *
	 * @param config The configuration this class should be using.
	 */
	public FulfillmentChannelMaintenance(MaintenanceConfig config) {
		this.config = config;
		this.fulfillmentChannelLookup = new FulfillmentChannelLookup(this.config.getJdbcTemplate());
		this.productLookup = new ProductLookup(this.config.getJdbcTemplate());
	}

	/**
	 * Handles processing a ExtendedFulfillmentChannelRequest.
	 *
	 * @param request The user's request.
	 * @return The updated list of ProductOnlineRanges after the request is complete.
	 */
	@Transactional
	public List<ProductFulfillmentChannel> processRequest(ExtendedFulfillmentChannelRequest request) {

		this.validate(request);

		if (FulfillmentChannelRequest.Type.ADD.equals(request.getType())) {

			this.processAdd(request);
		} else {

			this.processDelete(request);
		}

		return this.fulfillmentChannelLookup.readListByProductAndSalesChannel(request.getProductId(), request.getFulfillmentChannel().getSalesChannelCode());
	}

	/**
	 * Handles updating the end date of an existing product online record.
	 *
	 * @param request The user's request to update the end date of a product online event.
	 */
	@Transactional
	public void processDelete(ExtendedFulfillmentChannelRequest request) {

		// See if there is an existing record.
		Optional<ProductFulfillmentChannel> existingRecordAsOptional = this.fulfillmentChannelLookup.readByProductAndSalesAndFulfillmentChannel(request.getProductId(),
				request.getFulfillmentChannel().getSalesChannelCode(), request.getFulfillmentChannel().getFulfillmentChannelCode());

		// If not, we're done.
		if (existingRecordAsOptional.isEmpty()) {
			return;
		}

		TableUpdateSets<ProductFulfillmentChannel> recordsToUpdate = processDelete(existingRecordAsOptional.get(), request.getUserId());

		// Do the updates.
		this.issueMaintenance(request, recordsToUpdate);
	}

	/**
	 * Hold the logic to figure out what to do with a record marked for deletion.
	 *
	 * @param existingRecord The ProductFulfillmentChannel record in the DB.
	 * @param userId The user who is requesting the delete.
	 * @return A TableUpdateSets with the changes that will need to be made.
	 */
	protected static TableUpdateSets<ProductFulfillmentChannel> processDelete(ProductFulfillmentChannel existingRecord, String userId) {

		// Find which records need to be updated or deleted.
		TableUpdateSets<ProductFulfillmentChannel> recordsToUpdate = new TableUpdateSets<>();

		// If the record in the DB has not started yet...
		if (existingRecord.getEffectiveDate().isAfter(InstantUtils.startOfToday())) {

			// delete it.
			recordsToUpdate.getDeletes().add(existingRecord);
		} else {

			// otherwise, end it today.
			existingRecord.setExpirationDate(InstantUtils.startOfToday())
					.setLastUpdateUserId(userId);
			recordsToUpdate.getUpdates().add(existingRecord);
		}

		return recordsToUpdate;
	}

	/**
	 * Handles adding a new time-frame a series of fulfillment channel events. This may mean extending existing timeframes
	 * or adding a new one.
	 *
	 * @param request The user's request to update to add a time-frame to the fulfillment channel series.
	 */
	protected void processAdd(ExtendedFulfillmentChannelRequest request) {

		// Get all records for the sales channel.
		List<ProductFulfillmentChannel> existingFulfillmentChannels =
				this.fulfillmentChannelLookup.readListByProductAndSalesChannel(request.getProductId(), request.getFulfillmentChannel().getSalesChannelCode());

		// Find which records need to be updated or deleted.
		TableUpdateSets<ProductFulfillmentChannel> recordsToUpdate = processAdd(requestToProductFulfilmentChannel(request), existingFulfillmentChannels);

		this.issueMaintenance(request, recordsToUpdate);
	}


	/**
	 * Holds the logic to figure out what to do when adding or updating a ProductFulfillmentChannel.
	 *
	 * @param requestedChange The ProductFulfillmentChannel the user has asked to add or update.
	 * @param existingFulfillmentChannels The list of existing ProductFulfillmentChannels in the DB.
	 * @return A TableUpdateSets with the changes that will need to be made to the DB.
	 */
	protected static TableUpdateSets<ProductFulfillmentChannel> processAdd(ProductFulfillmentChannel requestedChange, List<ProductFulfillmentChannel> existingFulfillmentChannels) {

		TableUpdateSets<ProductFulfillmentChannel> updatesToMake = new TableUpdateSets<>();

		boolean alreadyThere = false;

		// Loop through all the existing channels and see what updates need to be made.
		for (ProductFulfillmentChannel existingFulfillmentChannel : existingFulfillmentChannels) {

			// If it's the same sales/fulfilment channel, just update it.
			if (Objects.equals(requestedChange.getKey(), existingFulfillmentChannel.getKey())) {
				updatesToMake.getUpdates().add(requestedChange);
				alreadyThere = true;
				continue;
			}

			// If they're the same type, don't make any changes.
			if (isSameType(requestedChange.getKey().getFulfillmentChannelCode(),
					existingFulfillmentChannel.getKey().getFulfillmentChannelCode())) {
				continue;
			}

			// If we get here, they're different types (one is sellable and the other is not).

			// If it starts today or later, delete it.
			if (existingFulfillmentChannel.getEffectiveDate().compareTo(InstantUtils.startOfToday()) >= 0) {
				updatesToMake.getDeletes().add(existingFulfillmentChannel);
				continue;
			}

			// If there is an overlap, end it the day before this one starts.
			if (existingFulfillmentChannel.getExpirationDate().compareTo(requestedChange.getEffectiveDate()) >= 0) {

				ZonedDateTime zonedEffectiveDate = requestedChange.getEffectiveDate().atZone(ZoneId.systemDefault());

				existingFulfillmentChannel.setExpirationDate(zonedEffectiveDate.minusDays(1).toInstant())
						.setLastUpdateTime(Instant.now())
						.setLastUpdateUserId(requestedChange.getLastUpdateUserId()); // This will hold the ID of the user who made the request.
				updatesToMake.getUpdates().add(existingFulfillmentChannel);
			}

			// If we get here, there was no overlap and we don't need to do anything.
		}

		// If it wasn't already in the DB, then add it.
		if (!alreadyThere) {
			updatesToMake.getInserts().add(requestedChange);
		}

		return updatesToMake;
	}

	/**
	 * Converts a ExtendedFulfillmentChannelRequest to a ProductFulfillmentChannel.
	 *
	 * @param request The ExtendedFulfillmentChannelRequest to convert.
	 * @return The converted ProductFulfillmentChannel.
	 */
	private static ProductFulfillmentChannel requestToProductFulfilmentChannel(ExtendedFulfillmentChannelRequest request) {

		ProductFulfillmentChannelKey key = new ProductFulfillmentChannelKey().setProductId(request.getProductId())
				.setFulfillmentChannelCode(FulfillmentChannelType.of(request.getFulfillmentChannel().getFulfillmentChannelCode()))
				.setSalesChannelCode(SalesChannelType.of(request.getFulfillmentChannel().getSalesChannelCode()));

		return new ProductFulfillmentChannel().setKey(key)
				.setEffectiveDate(InstantUtils.startOfDate(request.getEffectiveDate()))
				.setExpirationDate(InstantUtils.startOfDate(request.getEndDate()))
				.setLastUpdateUserId(request.getUserId())
				.setLastUpdateTime(Instant.now());
	}

	/**
	 * Does the work of calling the insert, update, and delete functions for a collection of ProductFulfillmentChannel
	 * updates and issuing the events for the changes.
	 *
	 * @param request The user's original ExtendedFulfillmentChannelRequest.
	 * @param recordsToUpdate The collection of changes to make.
	 */
	protected void issueMaintenance(ExtendedFulfillmentChannelRequest request, TableUpdateSets<ProductFulfillmentChannel> recordsToUpdate) {

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

			this.config.getLogger().info(String.format("Ending events for product %d: %d updates and %d deletes.", request.getProductId(),
					recordsToUpdate.getUpdates().size(), recordsToUpdate.getDeletes().size()));

			// Add a PRM2 event
			legacyEvents.add(LegacyEventGenerator.generatePRM2(request.getProductId(), this.config.getProgramName(), request.getUserId(), LegacyEventFunction.UPDATE));

			// Add a PSC2 event.
			this.productLookup.getProductPrimaryUpc(request.getProductId())
					.map(upc -> LegacyEventGenerator.generatePSC2(upc, this.config.getProgramName(), request.getUserId(), LegacyEventFunction.UPDATE))
					.ifPresent(legacyEvents::add);

			this.config.getLegacyEventProcessor().addAndFlush(legacyEvents);
		}
	}

	/**
	 * Handles the actual inserting of records into fulfillment channel.
	 *
	 * @param recordToInsert The ProductFulfillmentChannel object to insert.
	 * @return A list of LegacyEvents that are triggered by the insert.
	 */
	@Transactional
	protected LegacyEvent doInserts(ProductFulfillmentChannel recordToInsert) {

		ProductFulfillmentChannelUpdater productFulfillmentChannelUpdater = new ProductFulfillmentChannelUpdater(List.of(recordToInsert), UpdateType.INSERT);

		int [] inserts = this.config.getJdbcTemplate().batchUpdate(ProductFulfillmentChannelUpdater.INSERT_SQL, productFulfillmentChannelUpdater);

		this.config.getLogger().debug("Performed {} inserts to PROD_FLFL_CHNL.", Arrays.stream(inserts).sum());
		return LegacyEventGenerator.generatePFCH(recordToInsert.getKey().getProductId(), recordToInsert.getKey().getSalesChannelCode().getId(),
				recordToInsert.getKey().getFulfillmentChannelCode().getId(), this.config.getProgramName(), recordToInsert.getLastUpdateUserId(), LegacyEventFunction.ADD);
	}

	/**
	 * Handles the actual updating of records in fulfillment channel.
	 *
	 * @param recordsToUpdate The ProductFulfillmentChannel objects to update.
	 * @return A list of LegacyEvents that are triggered by the updates.
	 */
	@Transactional
	protected List<LegacyEvent> doUpdates(Set<ProductFulfillmentChannel> recordsToUpdate) {

		ProductFulfillmentChannelUpdater productFulfillmentChannelUpdater = new ProductFulfillmentChannelUpdater(recordsToUpdate, UpdateType.UPDATE);

		int[] updates = this.config.getJdbcTemplate().batchUpdate(ProductFulfillmentChannelUpdater.UPDATE_SQL, productFulfillmentChannelUpdater);

		this.config.getLogger().debug("Performed {} updates of PROD_FLFL_CHNL.", Arrays.stream(updates).sum());
		return recordsToUpdate.stream().map(r -> LegacyEventGenerator.generatePFCH(r.getKey().getProductId(), r.getKey().getSalesChannelCode().getId(),
				r.getKey().getFulfillmentChannelCode().getId(), this.config.getProgramName(), r.getLastUpdateUserId(), LegacyEventFunction.UPDATE)).collect(Collectors.toList());
	}

	/**
	 * Handles the actual deleting of records from fulfillment channel.
	 *
	 * @param recordsToDelete The ProductFulfillmentChannel objects to delete.
	 * @return A list of LegacyEvents that are triggered by the deletes.
	 */
	@Transactional
	protected List<LegacyEvent> doDeletes(Set<ProductFulfillmentChannel> recordsToDelete) {

		// The delete trigger is missing from fulfillment channel, so we need to copy the stuff we're deleting into the audits table.
		recordsToDelete.forEach(d -> this.config.getJdbcTemplate().update(AUDIT_FOR_DELETE_SQL,
				JdbcUtils.argsAsArray(d.getKey().getProductId(),
						d.getKey().getSalesChannelCode().getId(),
						d.getKey().getFulfillmentChannelCode().getId())));

		// Now we can do the deletes.
		ProductFulfillmentChannelUpdater productFulfillmentChannelUpdater = new ProductFulfillmentChannelUpdater(recordsToDelete, UpdateType.DELETE);
		int[] deletes = this.config.getJdbcTemplate().batchUpdate(ProductFulfillmentChannelUpdater.DELETE_SQL, productFulfillmentChannelUpdater);

		this.config.getLogger().debug("Performed {} deletes of PROD_FLFL_CHNL.", Arrays.stream(deletes).sum());
		return recordsToDelete.stream().map(r -> LegacyEventGenerator.generatePFCH(r.getKey().getProductId(), r.getKey().getSalesChannelCode().getId(),
				r.getKey().getFulfillmentChannelCode().getId(), this.config.getProgramName(), r.getLastUpdateUserId(), LegacyEventFunction.DELETE)).collect(Collectors.toList());
	}

	/**
	 * Validates an ExtendedFulfillmentChannelRequest to make sure it's OK to process.
	 *
	 * @param fulfillmentChannelUpdateRequest The request to validate.
	 */
	public void validate(ExtendedFulfillmentChannelRequest fulfillmentChannelUpdateRequest) {

		List<String> errors = new LinkedList<>(validateDatesForAdd(fulfillmentChannelUpdateRequest));

		if (Objects.isNull(fulfillmentChannelUpdateRequest.getType())) {
			errors.add("A request type is required.");
		}

		if (Objects.isNull(fulfillmentChannelUpdateRequest.getUserId())) {
			errors.add("User ID is required.");
		}

		if (Objects.isNull(fulfillmentChannelUpdateRequest.getProductId())) {
			errors.add("Product ID is required.");
		} else if (this.fulfillmentChannelLookup.notFound(fulfillmentChannelUpdateRequest.getProductId())) {
			errors.add(String.format("%d is not a valid product ID.", fulfillmentChannelUpdateRequest.getProductId()));
		}

		if (Objects.isNull(fulfillmentChannelUpdateRequest.getFulfillmentChannel())) {
			errors.add("Fulfillment channel is required.");
		} else {
			errors.addAll(this.validateSalesAndFulfillmentCodes(fulfillmentChannelUpdateRequest));
		}

		if (!errors.isEmpty()) {
			throw new ValidationException("Unable to validate product fulfilment channel request.", errors);
		}
	}

	private List<String> validateSalesAndFulfillmentCodes(ExtendedFulfillmentChannelRequest fulfillmentChannelRequest) {

		List<String> errors = new LinkedList<>();

		if (Objects.isNull(fulfillmentChannelRequest.getFulfillmentChannel().getSalesChannelCode())) {
			errors.add("Sales channel code is required.");
		} else {
			try {
				SalesChannelType.of(fulfillmentChannelRequest.getFulfillmentChannel().getSalesChannelCode());
			} catch (Exception e) {
				errors.add(String.format("'%s' is not a valid sales channel.", fulfillmentChannelRequest.getFulfillmentChannel().getSalesChannelCode()));
			}
		}

		if (Objects.isNull(fulfillmentChannelRequest.getFulfillmentChannel().getFulfillmentChannelCode())) {
			errors.add("Fulfillment channel code is required.");
		} else {
			try {
				FulfillmentChannelType fulfillmentChannelType =  FulfillmentChannelType.of(fulfillmentChannelRequest.getFulfillmentChannel().getFulfillmentChannelCode());

				// 08 is no longer allowed.
				if (Objects.equals(FulfillmentChannelType.DO_NOT_DISPLAY, fulfillmentChannelType)) {
					errors.add(String.format("Fulfilment channel type %s is no longer allowed.", FulfillmentChannelType.DO_NOT_DISPLAY.getId().trim()));
				}
			} catch (Exception e) {
				errors.add(String.format("'%s' is not a valid fulfillment channel.", fulfillmentChannelRequest.getFulfillmentChannel().getFulfillmentChannelCode()));
			}
		}

		if (this.fulfillmentChannelLookup.notFound(fulfillmentChannelRequest.getFulfillmentChannel().getSalesChannelCode(),
				fulfillmentChannelRequest.getFulfillmentChannel().getFulfillmentChannelCode())) {
			errors.add(String.format("Sales channel %s and fulfilment channel %s is not a valid combination.",
					fulfillmentChannelRequest.getFulfillmentChannel().getSalesChannelCode(),
					fulfillmentChannelRequest.getFulfillmentChannel().getFulfillmentChannelCode()));
		}

		return errors;
	}

	private static List<String> validateDatesForAdd(ExtendedFulfillmentChannelRequest fulfillmentChannelRequest) {

		List<String> errors = new LinkedList<>();

		if (Objects.equals(FulfillmentChannelRequest.Type.ADD, fulfillmentChannelRequest.getType())) {

			if (Objects.isNull(fulfillmentChannelRequest.getEffectiveDate()) || Objects.isNull(fulfillmentChannelRequest.getEndDate())) {
				errors.add("An effective and end date are required.");
			} else {

				if (fulfillmentChannelRequest.getEffectiveDate().isBefore(DateUtils.today())) {
					errors.add("Effective date must be today or greater.");
				}
				if (fulfillmentChannelRequest.getEndDate().isBefore(fulfillmentChannelRequest.getEffectiveDate())) {
					errors.add("End date must be after the effective date.");
				}
				if (fulfillmentChannelRequest.getEndDate().isAfter(DatabaseConstants.FOREVER_AS_DATE)) {
					errors.add("End date must be on or before 12/31/9999.");
				}
			}

		}

		return errors;
	}

	private static boolean isSameType(FulfillmentChannelType type1, FulfillmentChannelType type2) {

		boolean typeOneNonSellable = isNonSellable(type1);
		boolean typeTwoNonSellable = isNonSellable(type2);

		return typeOneNonSellable == typeTwoNonSellable;
	}

	private static boolean isNonSellable(FulfillmentChannelType type) {

		return Objects.equals(FulfillmentChannelType.DISPLAY_ONLY, type);
	}
}
