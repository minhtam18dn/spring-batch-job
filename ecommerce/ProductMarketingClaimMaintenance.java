package com.heb.pm.core.service.maintenance.ecommerce;

import com.heb.pm.core.maintenance.ProductMaintenanceRequest;
import com.heb.pm.core.service.maintenance.common.MaintenanceConfig;
import com.heb.pm.dao.core.LegacyEventGenerator;
import com.heb.pm.dao.core.entity.*;
import com.heb.pm.dao.core.entity.codes.LegacyEventFunction;
import com.heb.pm.dao.core.entity.codes.MarketingClaimCode;
import com.heb.pm.dao.core.preparedstatementsetters.ProductMarketingClaimUpdater;
import com.heb.pm.dao.core.preparedstatementsetters.UpdateType;
import com.heb.pm.dao.core.quicklookup.ProductLookup;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.*;
import java.util.function.Function;

/**
 * Manages maintenance or ProductMarketingClaims.
 *
 * @author d116773
 * @since 1.23.0
 */
// Since this can be called from batch jobs, I want to allow for different loggers. To do this, it is
// passed in as a parameter to the constructor, so the logger can't be static or final.
@SuppressWarnings("PMD.LoggerIsNotStaticFinal")
public class ProductMarketingClaimMaintenance {

	private static final long TOTALLY_TEXAS_ATTRIBUTE_ID = 1460L;
	private static final long TOTALLY_TEXAS_ATTRIBUTE_CODE = 116L;
	private static final String TOTALLY_TEXAS_ATTRIBUTE_TEXT = "YES";

	private final transient ProductMarketingClaimLookup productMarketingClaimLookup;
	private final transient ProductLookup productLookup;
	private final transient PrimoPickMaintenance primoPickMaintenance;
	private final transient MaintenanceConfig config;

	/**
	 * Constructs a new ProductMarketingClaimMaintenance.
	 *
	 * @param maintenanceConfig The config for this class.
	 */
	public ProductMarketingClaimMaintenance(MaintenanceConfig maintenanceConfig) {
		this.config = maintenanceConfig;
		this.productMarketingClaimLookup = new ProductMarketingClaimLookup(this.config.getJdbcTemplate());
		this.productLookup = new ProductLookup(this.config.getJdbcTemplate());
		this.primoPickMaintenance = new PrimoPickMaintenance(maintenanceConfig, this);
	}

	/**
	 * Looks through a ProductMaintenanceRequest to find any attributes that this class manages and does the
	 * appropriate updates.
	 *
	 * @param productId The ID of the product to do the update on.
	 * @param maintenanceRequest The user's maintenance request.
	 */
	@Transactional
	public void handleMarketingClaims(long productId, ProductMaintenanceRequest maintenanceRequest) {

		if (Objects.equals(Boolean.TRUE, maintenanceRequest.getGoLocal())) {
			this.makeGoLocal(List.of(productId), maintenanceRequest.getUserId());
		}

		if (Objects.equals(Boolean.FALSE, maintenanceRequest.getGoLocal())) {
			this.removeGoLocal(List.of(productId), maintenanceRequest.getUserId());
		}

		if (Objects.equals(Boolean.TRUE, maintenanceRequest.getTotallyTexas())) {
			this.makeTotallyTexas(List.of(productId), maintenanceRequest.getUserId());
		}

		if (Objects.equals(Boolean.FALSE, maintenanceRequest.getTotallyTexas())) {
			this.removeTotallyTexas(List.of(productId), maintenanceRequest.getUserId());
		}

		if (Objects.equals(Boolean.TRUE, maintenanceRequest.getOwnBrand())) {
			this.makeOwnBrand(List.of(productId), maintenanceRequest.getUserId());
		}

		if (Objects.equals(Boolean.FALSE, maintenanceRequest.getOwnBrand())) {
			this.removeOwnBrand(List.of(productId), maintenanceRequest.getUserId());
		}

		if (Objects.equals(Boolean.TRUE, maintenanceRequest.getSelectIngredients())) {
			this.makeSelectIngredient(List.of(productId), maintenanceRequest.getUserId());
		}

		if (Objects.equals(Boolean.FALSE, maintenanceRequest.getSelectIngredients())) {
			this.removeSelectIngredients(List.of(productId), maintenanceRequest.getUserId());
		}

		if (Objects.equals(Boolean.TRUE, maintenanceRequest.getDistinctive())) {
			this.makeDistinctive(List.of(productId), maintenanceRequest.getUserId());
		}

		if (Objects.equals(Boolean.FALSE, maintenanceRequest.getDistinctive())) {
			this.removeDistinctive(List.of(productId), maintenanceRequest.getUserId());
		}

		if (Objects.nonNull(maintenanceRequest.getPrimoPick())) {
			ExtendedPrimoPickRequest extendedPrimoPickRequest =  ExtendedPrimoPickRequest.of(productId, maintenanceRequest.getPrimoPick());

			this.primoPickMaintenance.handlePrimoPick(List.of(extendedPrimoPickRequest), maintenanceRequest.getUserId());
		}
	}

	/**
	 * Makes a list of products Select Ingredients. If a product in the list is already select ingredients,
	 * then it is untouched.
	 *
	 * @param productIds The list of products to make select ingredients.
	 * @param userId The ID of the user making the request.
	 */
	@Transactional
	public void makeSelectIngredient(List<Long> productIds, String userId) {

		Set<ProductMarketingClaim> insertedRecords = this.handleSimpleInsert(productIds, userId, MarketingClaimCode.SELECT_INGREDIENTS);
		this.config.getLegacyEventProcessor().addAndFlush(generateEvents(insertedRecords, LegacyEventFunction.ADD));
	}

	/**
	 * Removes Select Ingredients from a list of products.
	 *
	 * @param productIds The list of products to update.
	 * @param userId The user who triggered the update.
	 */
	@Transactional
	public void removeSelectIngredients(List<Long> productIds, String userId) {

		Set<ProductMarketingClaim> removedRecords = this.handleSimpleDelete(productIds, userId, MarketingClaimCode.SELECT_INGREDIENTS);
		this.config.getLegacyEventProcessor().addAndFlush(generateEvents(removedRecords, LegacyEventFunction.DELETE));
	}

	/**
	 * Makes a list of products Own Brand. If a product in the list is already Own Brand,
	 * then it is untouched.
	 *
	 * @param productIds The list of products to make Own Brand.
	 * @param userId The ID of the user making the request.
	 */
	@Transactional
	public void makeOwnBrand(List<Long> productIds, String userId) {

		Set<ProductMarketingClaim> insertedRecords = this.handleSimpleInsert(productIds, userId, MarketingClaimCode.OWN_BRAND);
		this.config.getLegacyEventProcessor().addAndFlush(generateEvents(insertedRecords, LegacyEventFunction.ADD));
	}

	/**
	 * Removes Own Brand from a list of products.
	 *
	 * @param productIds The list of products to update.
	 * @param userId The user who triggered the update.
	 */
	@Transactional
	public void removeOwnBrand(List<Long> productIds, String userId) {

		Set<ProductMarketingClaim> removedRecords = this.handleSimpleDelete(productIds, userId, MarketingClaimCode.OWN_BRAND);
		this.config.getLegacyEventProcessor().addAndFlush(generateEvents(removedRecords, LegacyEventFunction.DELETE));
	}

	/**
	 * Makes a list of products Go Local. If a product in the list is already Go Local,
	 * then it is untouched.
	 *
	 * @param productIds The list of products to make Go Local.
	 * @param userId The ID of the user making the request.
	 */
	@Transactional
	public void makeGoLocal(List<Long> productIds, String userId) {

		Set<ProductMarketingClaim> insertedRecords = this.handleSimpleInsert(productIds, userId, MarketingClaimCode.GO_LOCAL);
		this.config.getLegacyEventProcessor().addAndFlush(generateEvents(insertedRecords, LegacyEventFunction.ADD));
	}

	/**
	 * Removes Go Local from a list of products.
	 *
	 * @param productIds The list of products to update.
	 * @param userId The user who triggered the update.
	 */
	@Transactional
	public void removeGoLocal(List<Long> productIds, String userId) {

		Set<ProductMarketingClaim> removedRecords = this.handleSimpleDelete(productIds, userId, MarketingClaimCode.GO_LOCAL);
		this.config.getLegacyEventProcessor().addAndFlush(generateEvents(removedRecords, LegacyEventFunction.DELETE));
	}

	/**
	 * Makes a list of products Distinctive.
	 *
	 * @param productIds The list of products to make Distinctive.
	 * @param userId The user who triggered the update.
	 */
	@Transactional
	public void makeDistinctive(List<Long> productIds, String userId) {

		Set<ProductMarketingClaim> insertedRecords = this.handleSimpleInsert(productIds, userId, MarketingClaimCode.DISTINCTIVE);
		this.config.getLegacyEventProcessor().addAndFlush(generateEvents(insertedRecords, LegacyEventFunction.ADD));
	}

	/**
	 * Removes Distinctive from a list of products.
	 *
	 * @param productIds The list of products to update.
	 * @param userId The user who triggered the update.
	 */
	@Transactional
	public void removeDistinctive(List<Long> productIds, String userId) {

		boolean anyArePrimoPick = !productIds.stream().allMatch(this::validateRemoveDistinctiveRequest);
		if (anyArePrimoPick) {
			throw new IllegalArgumentException("You cannot remove Distinctive from a product that is a Primo-Pick.");
		}

		Set<ProductMarketingClaim> removedRecords = this.handleSimpleDelete(productIds, userId, MarketingClaimCode.DISTINCTIVE);
		this.config.getLegacyEventProcessor().addAndFlush(generateEvents(removedRecords, LegacyEventFunction.DELETE));
	}

	/**
	 * Determines if it is OK to remove distinctive from a product.
	 *
	 * @param productId The product to check.
	 * @return True if it is OK to remove distinctive from the product and false otherwise.
	 */
	@Transactional
	public boolean validateRemoveDistinctiveRequest(long productId) {

		// You can only remove distinctive if it's not a primo pick.
		ProductMarketingClaimKey key = ProductMarketingClaimKey.of(productId, MarketingClaimCode.PRIMO_PICK);

		Optional<ProductMarketingClaim> primoPick = this.productMarketingClaimLookup.findById(key);
		if (primoPick.isEmpty()) {
			return true;
		}

		return !primoPick.get().isActivePrimoPick();
	}

	/**
	 * Makes a list of products Totally Texas. If a product in the list is already Totally Texas,
	 * then it is untouched.
	 *
	 * @param productIds The list of products to make Totally Texas.
	 * @param userId The ID of the user making the request.
	 */
	@Transactional
	public void makeTotallyTexas(List<Long> productIds, String userId) {

		Set<ProductMarketingClaim> insertedMarketingClaims =
				this.handleSimpleInsert(productIds, userId, MarketingClaimCode.TOTALLY_TEXAS);

		// Get the events from adding the marketing claims.
		List<LegacyEvent> legacyEvents = new LinkedList<>(generateEvents(insertedMarketingClaims, LegacyEventFunction.ADD));

		// Handle adding the master data extended attributes.
		legacyEvents.addAll(this.handleTotallyTexasAttributes(insertedMarketingClaims, userId, true));

		// Save off the events.
		this.config.getLegacyEventProcessor().addAndFlush(legacyEvents);
	}

	/**
	 * Removes Totally Texas from a list of products.
	 *
	 * @param productIds The list of products to update.
	 * @param userId The user who triggered the update.
	 */
	@Transactional
	public void removeTotallyTexas(List<Long> productIds, String userId) {

		Set<ProductMarketingClaim> removedMarketingClaims =
				this.handleSimpleDelete(productIds, userId, MarketingClaimCode.TOTALLY_TEXAS);

		// Get the events from adding the marketing claims.
		List<LegacyEvent> legacyEvents = new LinkedList<>(generateEvents(removedMarketingClaims, LegacyEventFunction.DELETE));

		// Handle adding the master data extended attributes.
		legacyEvents.addAll(this.handleTotallyTexasAttributes(removedMarketingClaims, userId, false));

		// Save off the events.
		this.config.getLegacyEventProcessor().addAndFlush(legacyEvents);
	}

	/**
	 * Updates the mst_dta_extn_attr changes when making a list of products Totall Texas or removing that claim..
	 *
	 * @param productMarketingClaims The ProductMarketingClaim to pull the updates from.
	 * @param userId The user requesting this change.
	 * @param isInsert Is this making stuff Totally Texas or removing the claim?
	 */
	@Transactional
	protected List<LegacyEvent> handleTotallyTexasAttributes(Set<ProductMarketingClaim> productMarketingClaims, String userId, boolean isInsert) {

		List<MasterDataExtendedAttribute> masterDataExtendedAttributes = new LinkedList<>();

		productMarketingClaims.stream().map(pmc -> this.totallyTexasToMasterDataExtendedAttribute(pmc, SourceSystem.PRODUCT_MAINTENANCE_SOURCE_SYSTEM)).forEach(masterDataExtendedAttributes::add);
		productMarketingClaims.stream().map(pmc -> this.totallyTexasToMasterDataExtendedAttribute(pmc, SourceSystem.ECOMMERCE_SOURCE_SYSTEM)).forEach(masterDataExtendedAttributes::add);

		MasterDataExtendedAttributeMaintenance masterDataExtendedAttributeMaintenance = new MasterDataExtendedAttributeMaintenance(this.config);

		if (isInsert) {
			masterDataExtendedAttributeMaintenance.addMasterDataExtendedAttributes(masterDataExtendedAttributes, userId);
		} else {
			masterDataExtendedAttributeMaintenance.removeMasterDataExtendedAttributes(masterDataExtendedAttributes, userId);
		}

		List<LegacyEvent> legacyEvents = new LinkedList<>();

		// Since we're changing source 13, add a PRM2.
		productMarketingClaims.stream()
				.map(m -> LegacyEventGenerator.generatePRM2(m.getKey().getProdId(), this.config.getProgramName(), m.getLastUpdateUserId(), LegacyEventFunction.UPDATE))
				.forEach(legacyEvents::add);

		return legacyEvents;
	}

	private MasterDataExtendedAttribute totallyTexasToMasterDataExtendedAttribute(ProductMarketingClaim productMarketingClaim, long sourceSystemId) {

		long keyId;
		String keyType;

		if (sourceSystemId == SourceSystem.ECOMMERCE_SOURCE_SYSTEM) {

			keyId = productMarketingClaim.getKey().getProdId();
			keyType = MasterDataExtendedAttributeKey.KEY_TYPE_PRODUCT;
		} else {

			keyId = this.productLookup.getProductPrimaryUpc(productMarketingClaim.getKey().getProdId())
					.orElseThrow(() -> new IllegalArgumentException(String.format("Unable to find primary UPC for %d.", productMarketingClaim.getKey().getProdId())));
			keyType = MasterDataExtendedAttributeKey.KEY_TYPE_UPC;
		}

		MasterDataExtendedAttributeKey key = new MasterDataExtendedAttributeKey();
		key.setKeyId(keyId)
				.setAttributeId(TOTALLY_TEXAS_ATTRIBUTE_ID)
				.setKeyType(keyType)
				.setSequenceNumber(0L)
				.setSourceSystemId(sourceSystemId);

		MasterDataExtendedAttribute attribute = new MasterDataExtendedAttribute();
		attribute.setKey(key)
				.setAttributeCodeId(TOTALLY_TEXAS_ATTRIBUTE_CODE)
				.setTextValue(TOTALLY_TEXAS_ATTRIBUTE_TEXT)
				.setLastUpdateUserId(productMarketingClaim.getLastUpdateUserId())
				.setCreateUserId(productMarketingClaim.getLastUpdateUserId())
				.setCreateTime(Instant.now())
				.setLastUpdateTime(Instant.now())
				.setPrimarySourceSystemId(SourceSystem.PRODUCT_MAINTENANCE_SOURCE_SYSTEM);

		return attribute;
	}


	/**
	 * Some of the claims have basically no rules. This method can handle those.
	 *
	 * @param productIds The list of products to add a marketing claim to.
	 * @param userId The ID of the user adding the claim.
	 * @param marketingClaimCode The marketing claim code they want to add.
	 */
	@Transactional
	protected Set<ProductMarketingClaim> handleSimpleInsert(List<Long> productIds, String userId, MarketingClaimCode marketingClaimCode) {

		return this.handleInserts(productIds,
				p -> this.productLookup.isProductId(p) ? Optional.empty() : Optional.of(String.format("Product %d not found.", p)),
				p -> ProductMarketingClaim.ofDefaults(ProductMarketingClaimKey.of(p, marketingClaimCode), userId));
	}

	/**
	 * Extracts out the work of an insert.
	 *
	 * @param thingsToAdd The list of things to add.
	 * @param validator A validator to apply to each.
	 * @param mapper Maps the elements in thingsToAdd to a ProductMarketingClaim.
	 * @param <T> The type in thingsToAdd.
	 * @return The inserted ProductMarketingClaims.
	 */
	@Transactional
	protected <T> Set<ProductMarketingClaim> handleInserts(List<T> thingsToAdd, Function<T, Optional<String>> validator, Function<T, ProductMarketingClaim> mapper) {

		Set<T> uniqueIds = new HashSet<>(thingsToAdd);

		Set<ProductMarketingClaim> toInsert = new HashSet<>();

		for (T value : uniqueIds) {

			Optional<String> validationMessage = validator.apply(value);
			if (validationMessage.isPresent()) {
				throw new IllegalArgumentException(validationMessage.get());
			}

			ProductMarketingClaim productMarketingClaim = mapper.apply(value);

			// There is no reason to update this record if it's already present.
			if (this.productMarketingClaimLookup.findById(productMarketingClaim.getKey()).isEmpty()) {
				toInsert.add(productMarketingClaim);
			}
		}

		ProductMarketingClaimUpdater updater = new ProductMarketingClaimUpdater(toInsert, UpdateType.INSERT);

		int[] rowsInserted = this.config.getJdbcTemplate().batchUpdate(ProductMarketingClaimUpdater.INSERT_SQL, updater);
		this.config.getLogger().info(String.format("%,d product marketing claims inserted.", Arrays.stream(rowsInserted).sum()));

		return toInsert;
	}

	/**
	 * Some of the claims have basically no rules. This method can handle those.
	 *
	 * @param productIds The list of products to add a marketing claim to.
	 * @param userId The ID of the user adding the claim.
	 * @param marketingClaimCode The marketing claim code they want to add.
	 */
	@Transactional
	protected Set<ProductMarketingClaim> handleSimpleDelete(List<Long> productIds, String userId, MarketingClaimCode marketingClaimCode) {

		Set<Long> uniqueIds = new HashSet<>(productIds);

		Set<ProductMarketingClaim> toRemove = new HashSet<>();

		for (Long productId : uniqueIds) {

			ProductMarketingClaim productMarketingClaim =
					ProductMarketingClaim.ofDefaults(ProductMarketingClaimKey.of(productId, marketingClaimCode), userId);

			// There is no reason to update this record if it's already present.
			if (this.productMarketingClaimLookup.findById(productMarketingClaim.getKey()).isPresent()) {
				toRemove.add(productMarketingClaim);
			}
		}

		ProductMarketingClaimUpdater updater = new ProductMarketingClaimUpdater(toRemove, UpdateType.DELETE);

		int[] rowsDeleted = this.config.getJdbcTemplate().batchUpdate(ProductMarketingClaimUpdater.DELETE_SQL, updater);
		this.config.getLogger().info(String.format("%,d product marketing claims inserted.", Arrays.stream(rowsDeleted).sum()));

		return toRemove;
	}

	/**
	 * Generates LegacyEvents for changes in ProductMarketingClaims.
	 *
	 * @param productMarketingClaims The list of ProductMarketingClaims to generate events for.
	 * @param legacyEventFunction The function for the events.
	 * @return A list of LegacyEvents to save.
	 */
	@Transactional
	protected List<LegacyEvent> generateEvents(Set<ProductMarketingClaim> productMarketingClaims, LegacyEventFunction legacyEventFunction) {

		List<LegacyEvent> legacyEvents = new LinkedList<>();
		productMarketingClaims.stream()
				.map(p -> LegacyEventGenerator.generatePMCM(p.getKey().getProdId(), p.getKey().getMarketingClaimCode().getId(),
						this.config.getProgramName(), p.getLastUpdateUserId(), legacyEventFunction))
				.forEach(legacyEvents::add);

		return legacyEvents;
	}
}
