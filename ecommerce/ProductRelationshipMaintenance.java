package com.heb.pm.core.service.maintenance.ecommerce;

import com.heb.pm.core.event.LegacyEventProcessor;
import com.heb.pm.core.exception.ValidationException;
import com.heb.pm.core.maintenance.ProductRelationshipRequest;
import com.heb.pm.dao.core.DatabaseConstants;
import com.heb.pm.dao.core.LegacyEventGenerator;
import com.heb.pm.dao.core.entity.ItemMasterKey;
import com.heb.pm.dao.core.entity.LegacyEvent;
import com.heb.pm.dao.core.entity.ProductRelationship;
import com.heb.pm.dao.core.entity.ProductRelationshipKey;
import com.heb.pm.dao.core.entity.codes.LegacyEventFunction;
import com.heb.pm.dao.core.entity.codes.ProductRelationshipTypeCode;
import com.heb.pm.dao.core.preparedstatementsetters.ProductRelationshipUpdater;
import com.heb.pm.dao.core.preparedstatementsetters.UpdateType;
import com.heb.pm.dao.core.quicklookup.ProductLookup;
import com.heb.pm.dao.core.quicklookup.UpcLookup;
import com.heb.pm.dao.core.rowmappers.ItemMasterKeyRowMapper;
import com.heb.pm.util.JdbcUtils;
import lombok.Getter;
import org.slf4j.Logger;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Nonnull;
import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Class that manages updating product relationship information.
 *
 * @author d116773
 * @since 1.23.0
 */
// Since this can be called from batch jobs, I want to allow for different loggers. To do this, it is
// passed in as a parameter to the constructor, so the logger can't be static or final.
@SuppressWarnings("PMD.LoggerIsNotStaticFinal")
public class ProductRelationshipMaintenance {

	private static final String VARIANT_ITEM_LOOKUP_SQL = "SELECT ITEM_MASTER.ITM_KEY_TYP_CD, ITEM_MASTER.ITM_ID " +
			"   FROM EMD.ITEM_MASTER INNER JOIN EMD.PROD_ITEM " +
			"   ON  ITEM_MASTER.ITM_KEY_TYP_CD = PROD_ITEM.ITM_KEY_TYP_CD                     " +
			"   AND ITEM_MASTER.ITM_ID = PROD_ITEM.ITM_ID                             " +
			"       WHERE PROD_ITEM.PROD_ID = ?  " +
			"  AND ITEM_MASTER.MRT_SW = 'N'               " +
			"  AND ITEM_MASTER.ASSRTED_ITM_SW = ?         ";

	private static final String ITEM_UPDATE_SQL = "UPDATE EMD.ITEM_MASTER SET ASSRTED_ITM_SW = ?, LST_UPDT_USR_ID = ?, LST_UPDT_TS = ? " +
			"WHERE ITM_KEY_TYP_CD = ? AND ITM_ID = ?";

	private static final ItemMasterKeyRowMapper ITEM_MASTER_KEY_ROW_MAPPER = new ItemMasterKeyRowMapper();

	private final transient JdbcTemplate jdbcTemplate;
	private final transient String jobName;
	private final transient Logger logger;
	private final transient LegacyEventProcessor legacyEventProcessor;
	private final transient ProductRelationshipLookup productRelationshipLookup;
	private final transient ProductLookup productLookup;
	private final transient UpcLookup upcLookup;

	/**
	 * Class used to hold an item master key for items that will be marked as assorted items.
	 *
	 * @author d116773
	 * @since 1.23.0
	 */
	@Getter
	private static final class ItemMasterKeyAndUser extends ItemMasterKey {

		private static final long serialVersionUID = 879891824345484782L;

		private final String updateUser;

		/**
		 * Creates a new ItemMasterKeyAndUser.
		 *
		 * @param toClone An ItemMasterKey to copy into this class.
		 * @param updateUser The user that made the request that will update this item.
		 */
		private ItemMasterKeyAndUser(ItemMasterKey toClone, String updateUser) {
			super();
			this.setItemId(toClone.getItemId());
			this.setItemKeyTypeCode(toClone.getItemKeyTypeCode());
			this.updateUser = updateUser;
		}

		@Override
		// If we don't do this, findbugs flags it as an error.
		@SuppressWarnings("PMD.UselessOverridingMethod")
		public boolean equals(Object o) {
			return super.equals(o);
		}

		@Override
		// If we don't do this, findbugs flags it as an error.
		@SuppressWarnings("PMD.UselessOverridingMethod")
		public int hashCode() {
			return super.hashCode();
		}
	}

	/**
	 * BatchPreparedStatementSetter used to update the assorted item flag on.
	 *
	 * @author d116773
	 * @since 1.23.0
	 */
	private static class ItemMasterUpdater implements BatchPreparedStatementSetter {

		private final transient List<ItemMasterKeyAndUser> recordsToUpdate;
		private final transient String flagValue;

		/**
		 * Constructs a new ItemMasterUpdater.
		 *
		 * @param recordsToUpdate A collection of items to update.
		 * @param turnOnAssorted The value to set on the item's assorted item flag.
		 */
		private ItemMasterUpdater(Collection<? extends ItemMasterKeyAndUser> recordsToUpdate, boolean turnOnAssorted) {

			this.recordsToUpdate = new ArrayList<>(recordsToUpdate);
			this.flagValue = turnOnAssorted ? DatabaseConstants.YES : DatabaseConstants.NO;
		}

		@Override
		public void setValues(@Nonnull PreparedStatement ps, int i) throws SQLException {

			ItemMasterKeyAndUser itemMasterKey = this.recordsToUpdate.get(i);

			ps.setString(1, this.flagValue);
			ps.setString(2, itemMasterKey.getUpdateUser());
			ps.setTimestamp(3, Timestamp.from(Instant.now()));
			ps.setString(4, itemMasterKey.getItemKeyTypeCode().getId());
			ps.setLong(5, itemMasterKey.getItemId());
		}

		@Override
		public int getBatchSize() {

			return this.recordsToUpdate.size();
		}
	}

	/**
	 * Constructs a new ProductRelationshipMaintenance.
	 *
	 * @param jdbcTemplate The JdbcTemplate to use to run queries.
	 * @param jobName The name of the job making the update.
	 * @param logger The Logger to use.
	 * @param legacyEventProcessor The LegacyEventProcessor to use to trigger events with.
	 */
	public ProductRelationshipMaintenance(JdbcTemplate jdbcTemplate, String jobName, Logger logger, LegacyEventProcessor legacyEventProcessor) {

		this.jdbcTemplate = jdbcTemplate;
		this.jobName = jobName;
		this.logger = logger;
		this.legacyEventProcessor = legacyEventProcessor;
		this.productRelationshipLookup = new ProductRelationshipLookup(this.jdbcTemplate);
		this.productLookup = new ProductLookup(this.jdbcTemplate);
		this.upcLookup = new UpcLookup(this.jdbcTemplate);
	}

	/**
	 * Updates the product relationships for a collection of requests.
	 *
	 * @param productRelationshipRequests The collection of requests to process.
	 */
	@Transactional
	public void updateProductRelationship(Collection<? extends ExtendedProductRelationshipRequest> productRelationshipRequests) {

		Set<ProductRelationship> inserts = new HashSet<>();
		Set<ProductRelationship> updates = new HashSet<>();
		Set<ItemMasterKeyAndUser> itemsToUpdate = new HashSet<>();

		for (ExtendedProductRelationshipRequest productRelationshipRequest : productRelationshipRequests) {

			this.logger.info(String.format("Processing product relationship: %s.", productRelationshipRequest));

			this.validate(productRelationshipRequest);

			// See if the relationship already exists.
			Optional<ProductRelationship> productRelationship =
					this.productRelationshipLookup.find(productRelationshipRequest.getProductId(),
							productRelationshipRequest.getRelatedProductId(),
							requestTypeToDbType(productRelationshipRequest.getRelationshipType()));

			if (productRelationship.isPresent()) {

				// If so, update it.
				updates.add(requestToObject(productRelationshipRequest));
			} else {

				// If not, create it and we may need to update item master.
				inserts.add(requestToObject(productRelationshipRequest));
				itemsToUpdate.addAll(this.getItemToUpdate(productRelationshipRequest));
			}
		}

		// Save everything that changed.
		List<LegacyEvent> legacyEvents = this.doUpdates(updates);
		legacyEvents.addAll(this.doInserts(inserts));
		legacyEvents.addAll(this.updateItems(itemsToUpdate, true));

		// Publish the generated events.
		this.legacyEventProcessor.addAndFlush(legacyEvents);
	}

	/**
	 * Performs updates on existing ProductRelationship records.
	 *
	 * @param recordsToUpdate The ProductRelationship records to update.
	 * @return The list of LegacyEvents to trigger because of the updates.
	 */
	@Transactional
	protected List<LegacyEvent> doUpdates(Set<ProductRelationship> recordsToUpdate) {

		ProductRelationshipUpdater productRelationshipUpdater = new ProductRelationshipUpdater(recordsToUpdate, UpdateType.UPDATE);
		int[] rowsUpdated = this.jdbcTemplate.batchUpdate(ProductRelationshipUpdater.UPDATE_SQL, productRelationshipUpdater);
		this.logger.debug(String.format("%,d rows updated.", Arrays.stream(rowsUpdated).sum()));
		return this.generateEvents(recordsToUpdate, LegacyEventFunction.UPDATE);
	}

	/**
	 * Inserts new ProductRelationship records.
	 *
	 * @param recordsToInsert The ProductRelationship records to insert.
	 * @return The list of LegacyEvents to trigger because of the inserts.
	 */
	@Transactional
	protected List<LegacyEvent> doInserts(Set<ProductRelationship> recordsToInsert) {

		ProductRelationshipUpdater productRelationshipUpdater = new ProductRelationshipUpdater(recordsToInsert, UpdateType.INSERT);
		int[] rowsUpdated = this.jdbcTemplate.batchUpdate(ProductRelationshipUpdater.INSERT_SQL, productRelationshipUpdater);
		this.logger.debug(String.format("%,d rows inserted.", Arrays.stream(rowsUpdated).sum()));
		return this.generateEvents(recordsToInsert, LegacyEventFunction.ADD);
	}

	/**
	 * Constructs LegacyEvents for a collection of ProductRelationship changes.
	 *
	 * @param productRelationshipRequests The collection of ProductRelationship changes.
	 * @return The list of LegacyEvents to trigger because of the changes.
	 */
	@Transactional
	protected List<LegacyEvent> generateEvents(Collection<? extends ProductRelationship> productRelationshipRequests, LegacyEventFunction legacyEventFunction) {

		List<LegacyEvent> events = new LinkedList<>();

		for (ProductRelationship productRelationshipRequest : productRelationshipRequests) {

			// Make a PRM2, a PRMM, and a PRRM for each product in the relationship.

			events.add(LegacyEventGenerator.generatePRM2(productRelationshipRequest.getKey().getProductId(), this.jobName,
					productRelationshipRequest.getLastUpdateUserId(), LegacyEventFunction.UPDATE));
			events.add(LegacyEventGenerator.generatePRMM(productRelationshipRequest.getKey().getProductId(), this.jobName,
					productRelationshipRequest.getLastUpdateUserId(), LegacyEventFunction.UPDATE));

			events.add(LegacyEventGenerator.generatePRM2(productRelationshipRequest.getKey().getRelatedProductId(), this.jobName,
					productRelationshipRequest.getLastUpdateUserId(), LegacyEventFunction.UPDATE));
			events.add(LegacyEventGenerator.generatePRMM(productRelationshipRequest.getKey().getRelatedProductId(), this.jobName,
					productRelationshipRequest.getLastUpdateUserId(), LegacyEventFunction.UPDATE));

			events.add(LegacyEventGenerator.generatePRRM(productRelationshipRequest, this.jobName,
					productRelationshipRequest.getLastUpdateUserId(), legacyEventFunction));
		}

		return events;
	}

	/**
	 * Returns a list of items that need to be updated because of a request to change a ProductRelationship.
	 *
	 * @param productRelationshipRequest The request to change a ProductRelationship.
	 * @return A list of items to update. This list may be empty.
	 */
	@Transactional
	protected List<ItemMasterKeyAndUser> getItemToUpdate(ExtendedProductRelationshipRequest productRelationshipRequest) {

		// If it's a variant, then the assorted item switch on item master needs to be set.
		if (Objects.equals(productRelationshipRequest.getRelationshipType(), ProductRelationshipRequest.RelationshipType.VARIANT)) {

			return this.jdbcTemplate.query(VARIANT_ITEM_LOOKUP_SQL, JdbcUtils.argsAsArray(productRelationshipRequest.getProductId(), DatabaseConstants.NO),
					ITEM_MASTER_KEY_ROW_MAPPER).stream()
					.map(i -> new ItemMasterKeyAndUser(i, productRelationshipRequest.getUserId()))
					.collect(Collectors.toList());
		}

		return List.of();
	}

	/**
	 * Updates the assorted item flag for a collection of items.
	 *
	 * @param itemsToUpdate The items to update.
	 * @param turningOnFlag True if you are turning on the flag and false if turning it off.
	 * @return he list of LegacyEvents to trigger because of the updates.
	 */
	@Transactional
	protected List<LegacyEvent> updateItems(Collection<? extends ItemMasterKeyAndUser> itemsToUpdate, boolean turningOnFlag) {

		List<LegacyEvent> events = new LinkedList<>();

		// If any item was updated, generate an ITMM.
		itemsToUpdate.forEach(i -> events.add(LegacyEventGenerator.generateITMM(i, this.jobName, i.getUpdateUser(), LegacyEventFunction.UPDATE)));

		ItemMasterUpdater itemMasterUpdater = new ItemMasterUpdater(itemsToUpdate, turningOnFlag);
		int[] rowsUpdated = this.jdbcTemplate.batchUpdate(ITEM_UPDATE_SQL, itemMasterUpdater);

		this.logger.debug(String.format("%,d items updated.", Arrays.stream(rowsUpdated).sum()));

		return events;
	}

	/**
	 * Validates a ExtendedProductRelationshipRequest. Classes calling the update methods may want to call this on each
	 * update request before calling the update method. The update method will also call this, but will throw an exception
	 * if any of the items in the list fail validation.
	 *
	 * @param productRelationshipRequest The ExtendedProductRelationshipRequest to validate.
	 */
	public void validate(ExtendedProductRelationshipRequest productRelationshipRequest) {

		List<String> errors = new LinkedList<>();

		// Product ID is required and must be a real product.
		if (Objects.isNull(productRelationshipRequest.getProductId())) {
			errors.add("Product ID is required.");
		} else {
			if (!this.productLookup.isProductId(productRelationshipRequest.getProductId())) {
				errors.add(String.format("%d is not a valid product.", productRelationshipRequest.getProductId()));
			}
		}

		// The type of relationship is required.
		if (Objects.isNull(productRelationshipRequest.getRelationshipType())) {
			errors.add("A relationship type is required.");
		}

		// Related product ID is required and must be a real product.
		if (Objects.isNull(productRelationshipRequest.getRelatedProductId())) {
			errors.add("Related product ID is required.");
		} else {
			if (!this.productLookup.isProductId(productRelationshipRequest.getRelatedProductId())) {
				errors.add(String.format("%d is not a valid product.", productRelationshipRequest.getRelatedProductId()));
			}

			if (Objects.nonNull(productRelationshipRequest.getUpc())) {

				this.validateUpc(productRelationshipRequest.getUpc(), productRelationshipRequest.getRelatedProductId())
						.ifPresent(errors::add);
			}
		}

		// User ID is required.
		if (Objects.isNull(productRelationshipRequest.getUserId())) {
			errors.add("User ID is required.");
		}

		// Variants require a UPC but no quantity
		if (Objects.equals(productRelationshipRequest.getRelationshipType(), ProductRelationshipRequest.RelationshipType.VARIANT)) {

			if (Objects.isNull(productRelationshipRequest.getUpc())) {
				errors.add("A UPC is required for a variant relationship.");
			}
			if (Objects.nonNull(productRelationshipRequest.getProductQuantity()) &&
					!Objects.equals(BigDecimal.ZERO, productRelationshipRequest.getProductQuantity()) &&
					!Objects.equals(ProductRelationshipUpdater.UNSET_PRODUCT_QUANTITY, productRelationshipRequest.getProductQuantity())) {
				errors.add("A product quantity cannot be set for a variant relationship.");
			}
		}

		if (!errors.isEmpty()) {
			throw new ValidationException("Unable to validate product relationship request.", errors);
		}
	}

	/**
	 * Validates a UPC in an ExtendedProductRelationshipRequest.
	 *
	 * @param upc The UPC being added to the ProductRelationship.
	 * @param productId The product the UPC should be tied to.
	 * @return If there is an error, a description of the error. If not, empty.
	 */
	private Optional<String> validateUpc(long upc, long productId) {

		// The UPC must be a real UPC and tied to the related product.
		if (!this.upcLookup.isUpc(upc)) {

			return Optional.of(String.format("%d is not a valid UPC.", upc));
		}

		if (!this.upcLookup.isUpc(upc, productId)) {

			return Optional.of(String.format("UPC %d is not tied to product %d.", upc, productId));
		}

		return Optional.empty();
	}

	/**
	 * Converts a ExtendedProductRelationshipRequest to a ProductRelationship.
	 *
	 * @param productRelationshipRequest The ExtendedProductRelationshipRequest to convert.
	 * @return The converted ProductRelationship.
	 */
	private static ProductRelationship requestToObject(ExtendedProductRelationshipRequest productRelationshipRequest) {

		ProductRelationshipKey productRelationshipKey = new ProductRelationshipKey();
		productRelationshipKey.setProductId(productRelationshipRequest.getProductId())
				.setProductRelationshipCode(requestTypeToDbType(productRelationshipRequest.getRelationshipType()))
				.setRelatedProductId(productRelationshipRequest.getRelatedProductId());

		return new ProductRelationship()
				.setKey(productRelationshipKey)
				.setProductQuantity(productRelationshipRequest.getProductQuantity())
				.setLastUpdateUserId(productRelationshipRequest.getUserId())
				.setUpc(productRelationshipRequest.getUpc())
				.setLastUpdateTime(Instant.now());
	}

	/**
	 * Converts a RelationshipType to a ProductRelationshipTypeCode.
	 *
	 * @param relationshipType The RelationshipType to convert.
	 * @return The converted ProductRelationshipTypeCode.
	 */
	private static ProductRelationshipTypeCode requestTypeToDbType(ProductRelationshipRequest.RelationshipType relationshipType) {

		return ProductRelationshipTypeCode.of(relationshipType.getId());
	}

}
