package com.heb.pm.core.service.maintenance.ecommerce;

import com.heb.pm.core.event.alerts.AlertStagingClient;
import com.heb.pm.core.exception.ValidationException;
import com.heb.pm.core.service.maintenance.common.MaintenanceConfig;
import com.heb.pm.core.service.maintenance.common.TableUpdateSets;
import com.heb.pm.dao.core.LegacyEventGenerator;
import com.heb.pm.dao.core.entity.ProductScanCodeExtent;
import com.heb.pm.dao.core.entity.codes.ExtendedAttributeType;
import com.heb.pm.dao.core.entity.codes.LegacyEventFunction;
import com.heb.pm.dao.core.entity.codes.StagedAlertType;
import com.heb.pm.dao.core.preparedstatementsetters.ProductScanCodeExtentUpdater;
import com.heb.pm.dao.core.preparedstatementsetters.UpdateType;
import com.heb.pm.dao.core.quicklookup.ProductLookup;
import com.heb.pm.dao.core.quicklookup.SourceSystemLookup;
import com.heb.pm.dao.core.quicklookup.UpcLookup;
import com.heb.pm.util.ValidatorUtils;
import com.heb.pm.util.soap.CheckedSoapException;
import org.apache.commons.lang3.StringUtils;
import org.springframework.transaction.annotation.Transactional;

import java.util.*;
import java.util.stream.Collectors;

import static com.heb.pm.util.ValidatorUtils.notNullOrIllegalArgument;

/**
 * Performs maintenance on the PROD_SCN_CD_EXTENT table.
 *
 * @author d116773
 * @since 1.25.0
 */
public class ProductScanCodeExtentMaintenance {

	// We only want to generate tasks for these types of attributes.
	private static final List<ExtendedAttributeType> TYPES_TO_GENERATE_TASKS_FOR = List.of(
			ExtendedAttributeType.ROMANCE_COPY,
			ExtendedAttributeType.PRODUCT_DESCRIPTION,
			ExtendedAttributeType.EXTENDED_SIZE,
			ExtendedAttributeType.ITEM_SIZE,
			ExtendedAttributeType.UNIT_OF_MEASURE,
			ExtendedAttributeType.ITEM_DESCRIPTION,
			ExtendedAttributeType.INDICATIONS,
			ExtendedAttributeType.INGREDIENTS,
			ExtendedAttributeType.PRODUCT_DETAIL,
			ExtendedAttributeType.GUARANTEED_ANALYSIS,
			ExtendedAttributeType.BRAND_NAME,
			ExtendedAttributeType.PRODUCT_LINE,
			ExtendedAttributeType.DIRECTIONS,
			ExtendedAttributeType.WARNING
	);

	private final transient MaintenanceConfig config;
	private final transient ProductScanCodeExtentLookup lookup;
	private final transient ProductLookup productLookup;
	private final transient UpcLookup upcLookup;
	private final transient SourceSystemLookup sourceSystemLookup;

	/**
	 * Constructs a new ProductScanCodeExtentMaintenance.
	 *
	 * @param config The configuration for this class to use.
	 */
	public ProductScanCodeExtentMaintenance(MaintenanceConfig config) {

		config.requireBasics();

		this.config = config;
		this.lookup = new ProductScanCodeExtentLookup(this.config.getJdbcTemplate());
		this.productLookup = new ProductLookup(this.config.getJdbcTemplate());
		this.upcLookup = new UpcLookup(this.config.getJdbcTemplate());
		this.sourceSystemLookup = new SourceSystemLookup(this.config.getJdbcTemplate());
	}

	/**
	 * Adds or updates a collection of ProductScanCodeExtent records.
	 *
	 * @param toAdd The ProductScanCodeExtent records to process.
	 * @param userId The ID of the user who triggered this change.
	 * @return The number of rows inserted or updated.
	 */
	@Transactional
	public int addProductScanCodeExtend(List<ProductScanCodeExtent> toAdd, String userId) {

		this.config.requireAll();

		notNullOrIllegalArgument(userId, "User ID cannot be null.");
		notNullOrIllegalArgument(toAdd, "List of extended scan code records cannot be null.");

		// Validate all the requests, set any defaults, and collect the distinct UPCs into a set.
		Set<Long> allUpcs = toAdd.stream()
				.map(this::validate)
				.map(p -> this.applyDefaults(p, userId))
				.map(p -> p.getKey().getScanCodeId())
				.collect(Collectors.toSet());

		TableUpdateSets<ProductScanCodeExtent> updates = new TableUpdateSets<>();

		// Collect all the records that need to be inserted and updated.
		allUpcs.stream()
				.map(u -> this.processUpc(toAdd, u, userId))
				.forEach(updates::addAll);

		int rowsUpdated = this.doUpdates(updates.getUpdates());
		rowsUpdated += this.doInserts(updates.getInserts());

		// Trigger legacyEvents.
		this.config.getLegacyEventProcessor().addAndFlush(updates.getEvents());

		// Create any tasks.
		updates.getAlerts().forEach(this::issueAlert);

		return rowsUpdated;
	}

	/**
	 * Validates a ProductScanCodeExtent can be processed.
	 *
	 * @param toValidate The ProductScanCodeExtent to validate.
	 * @return The validated ProductScanCodeExtent object.
	 * @throws ValidationException Any error in validating the object.
	 */
	public ProductScanCodeExtent validate(ProductScanCodeExtent toValidate) {

		List<String> errors = new LinkedList<>();

		if (Objects.isNull(toValidate.getKey().getScanCodeId())) {

			errors.add("UPC is required.");

		} else if (!this.upcLookup.isUpc(toValidate.getKey().getScanCodeId())) {

			errors.add(String.format("%d is not a valid UPC.", toValidate.getKey().getScanCodeId()));
		}

		ValidatorUtils.validateFieldExists(toValidate.getKey()::getProductExtentDataCode, "Type is required.").ifPresent(errors::add);
		ValidatorUtils.validateFieldExists(toValidate::getProductDescriptionText, "Text is required.").ifPresent(errors::add);

		if (Objects.isNull(toValidate.getSourceSystemId())) {

			errors.add("Source system ID is required.");

		} else if (!this.sourceSystemLookup.isSourceSystem(toValidate.getSourceSystemId())) {

			errors.add(String.format("%d is not a valid source system.", toValidate.getSourceSystemId()));
		}

		if (!errors.isEmpty()) {

			throw new ValidationException("Unable to validate extended scan code record.", errors);
		}

		return toValidate;
	}

	/**
	 * Applies default values to a ProductScanCodeExtent object.
	 *
	 * @param productScanCodeExtent The ProductScanCodeExtent to apply default values to.
	 * @param userId The ID of the user who requested this change.
	 * @return The passed in ProductScanCodeExtent but with defaults set.
	 */
	private ProductScanCodeExtent applyDefaults(ProductScanCodeExtent productScanCodeExtent, String userId) {

		return productScanCodeExtent.setLastUpdateUserId(userId);
	}

	/**
	 * Processes the ProductScanCodeExtent records for a single UPC.
	 *
	 * @param allRecordsToAdd All the records to process. These may include records that do not contain the UPC passed as
	 *                        a parameter, but this function will not process them.
	 * @param upc The UPC to process.
	 * @param userId The ID of the user who requested the change.
	 * @return A TableUpdateSets with records to insert and update, a LegacyEvent to process, and, possibly, an alert
	 * to issue to assign a task to a user.
	 */
	protected TableUpdateSets<ProductScanCodeExtent> processUpc(List<ProductScanCodeExtent> allRecordsToAdd, long upc, String userId) {

		// Find all the records for a given UPC.
		List<ProductScanCodeExtent> allForUpc = allRecordsToAdd.stream()
				.filter(p -> Objects.equals(p.getKey().getScanCodeId(), upc))
				.collect(Collectors.toList());

		List<ProductScanCodeExtent> existingRecords = this.lookup.findByUpc(upc);

		// Figure out the records to add and update.
		TableUpdateSets<ProductScanCodeExtent> updates = processAdd(allForUpc, existingRecords);

		// Generate a PSC2 for the UPC.
		updates.getEvents().add(LegacyEventGenerator.generatePSC2(upc, this.config.getProgramName(),
				userId, LegacyEventFunction.UPDATE));

		// Get the product ID for this UPC.
		Long productId = this.upcLookup.getProductId(upc)
				.orElseThrow(() -> new IllegalStateException(String.format("Product for UPC %d no longer exists.", upc)));

		// Get the primary UPC for that products.
		Long productPrimaryUpc = this.productLookup.getProductPrimaryUpc(productId).orElse(0L);

		// Get the list of alert IDs to generate for the inserts.
		Set<Long> alertIds = getAlertIds(updates.getInserts());

		// If this is the product primary UPC, then generate alerts for updates.
		if (Objects.equals(upc, productPrimaryUpc)) {

			alertIds.addAll(getAlertIds(updates.getUpdates()));
		}

		// If there are any to trigger, build the alert event.
		if (!alertIds.isEmpty()) {

			// Get the ebmID for this product to assign an event. If we can't find one, don't assign it to anyone.
			String ebmId = this.productLookup.getEcommerceBusinessManager(productId).orElse(StringUtils.SPACE);

			// Map the record to an alert.
			AlertStagingClient.AlertRequest alertRequest = new AlertStagingClient.AlertRequest()
					.setAlertType(StagedAlertType.PRODUCT_UPDATE)
					.setCreateUser(userId)
					.setAlertKey(productId)
					.setAssignedUser(ebmId);
			alertRequest.getAlerts().addAll(alertIds);

			updates.getAlerts().add(alertRequest);
		}

		return updates;
	}

	/**
	 * Looks at a list of ProductScanCodeExtent records and compares them to a set of existing records
	 * and determines what to do with them. If the records in toAdd are not in existingRecords, they are
	 * added to the inserts list in the returned TableUpdateSets. If the records in toAdd do exist in
	 * existingRecords, they are added tot he updates in the returned TableUpdateSets.
	 *
	 * @param toAdd The records that will be added or updated to the DB.
	 * @param existingRecords The records that already exist in the DB.
	 * @return A TableUpdateSets with inserts and updates to apply to PROD_SCN_CD_EXTEDT.
	 */
	protected static TableUpdateSets<ProductScanCodeExtent> processAdd(List<ProductScanCodeExtent> toAdd,
																List<ProductScanCodeExtent> existingRecords) {

		TableUpdateSets<ProductScanCodeExtent> updates = new TableUpdateSets<>();

		for (ProductScanCodeExtent psc : toAdd) {

			// See if the one we're trying to add is already there or not...
			if (existingRecords.stream().anyMatch(e -> Objects.equals(e.getKey(), psc.getKey()))) {

				// if so, add it to the list to update.
				updates.getUpdates().add(psc);

			} else {

				// If not, add it to the set of inserts.
				updates.getInserts().add(psc);
			}
		}

		return updates;
	}

	/**
	 * Returns the list of alert IDs to generate for a list of ProductScanCodeExtent records.
	 *
	 * @param records The list of ProductScanCodeExtent records to get alert IDs for.
	 * @return The list of alert IDs for the passed in records.
	 */
	protected static Set<Long> getAlertIds(Set<ProductScanCodeExtent> records) {

		Set<Long> alertIds = new HashSet<>();

		// Add all the ones we're supposed to trigger alerts for.
		records.stream()
				.filter(ProductScanCodeExtentMaintenance::generateAlertFor)
				.map(r -> r.getKey().getProductExtentDataCode().getAttributeEquivalent())
				.forEach(alertIds::add);

		return alertIds;
	}

	/**
	 * Calls the service to issue an alert and assign a task to a user.
	 *
	 * @param alertRequest The alert to issue.
	 */
	private void issueAlert(AlertStagingClient.AlertRequest alertRequest) {

		try {

			this.config.getAlertStagingClient().issueAlert(alertRequest);

		} catch (CheckedSoapException e) {

			this.config.getLogger().error(String.format("Unable to issue alerts: '%s'.", e.getLocalizedMessage()));
		}
	}

	/**
	 * Does the actual inserts into PROD_SCN_CD_EXTENT.
	 *
	 * @param toInsert The records to insert.
	 * @return The number of records inserted.
	 */
	private int doInserts(Set<ProductScanCodeExtent> toInsert) {

		ProductScanCodeExtentUpdater updater = new ProductScanCodeExtentUpdater(toInsert, UpdateType.INSERT);
		int[] rowsUpdated = this.config.getJdbcTemplate().batchUpdate(ProductScanCodeExtentUpdater.INSERT_SQL, updater);
		return Arrays.stream(rowsUpdated).sum();
	}

	/**
	 * Does the actual updates into PROD_SCN_CD_EXTENT.
	 *
	 * @param toUpdate The records to update.
	 * @return The number of records updated.
	 */
	private int doUpdates(Set<ProductScanCodeExtent> toUpdate) {

		ProductScanCodeExtentUpdater updater = new ProductScanCodeExtentUpdater(toUpdate, UpdateType.UPDATE);
		int[] rowsUpdated = this.config.getJdbcTemplate().batchUpdate(ProductScanCodeExtentUpdater.UPDATE_SQL, updater);
		return Arrays.stream(rowsUpdated).sum();
	}

	/**
	 * Returns whether or not an alert should be generated for a particular ProductScanCodeExtent.
	 *
	 * @param record The ProductScanCodeExtent to check to see if it should have an alert generate.
	 * @return True if an even should be generated and false otherwise.
	 */
	private static boolean generateAlertFor(ProductScanCodeExtent record) {

		return TYPES_TO_GENERATE_TASKS_FOR.stream().anyMatch(t -> Objects.equals(t, record.getKey().getProductExtentDataCode()));
	}

}
