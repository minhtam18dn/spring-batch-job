package com.heb.pm.core.service.maintenance.ecommerce;

import com.heb.pm.core.exception.ValidationException;
import com.heb.pm.core.maintenance.ProductCustomHierarchyRequest;
import com.heb.pm.core.service.maintenance.common.MaintenanceConfig;
import com.heb.pm.core.service.maintenance.common.TableUpdateSets;
import com.heb.pm.dao.core.DatabaseConstants;
import com.heb.pm.dao.core.LegacyEventGenerator;
import com.heb.pm.dao.core.entity.*;
import com.heb.pm.dao.core.entity.codes.GenericEntityType;
import com.heb.pm.dao.core.entity.codes.HierarchyContextCode;
import com.heb.pm.dao.core.entity.codes.LegacyEventFunction;
import com.heb.pm.dao.core.preparedstatementsetters.EntityDescriptionUpdater;
import com.heb.pm.dao.core.preparedstatementsetters.GenericEntityUpdater;
import com.heb.pm.dao.core.preparedstatementsetters.UpdateType;
import com.heb.pm.dao.core.quicklookup.ProductGroupLookup;
import com.heb.pm.dao.core.quicklookup.ProductLookup;
import com.heb.pm.dao.core.rowmappers.LongResultSetExtractor;
import com.heb.pm.util.InstantUtils;
import com.heb.pm.util.JdbcUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.heb.pm.util.ValidatorUtils.notNullOrIllegalArgument;

/**
 * To keep from looking up the same stuff multiple times, a lot of the logic
 * for processing a ProductCustomHierarchyRequest is moved into this class so it
 * can keep state across functions.
 *
 * @author d116773
 * @since 1.27.0
 */
/* default */ class CustomHierarchyMaintenanceImpl {

	private static final long DEFAULT_SEQUENCE_NUMBER = 1L;
	private static final Long MINIMUM_ENTITY_ID = 1L;

	private static final int ONLY_ONE = 1;

	private static final String PARENT_IS_LOWEST_LEVEL_CHECK_SQL = "SELECT DISTINCT PARNT_ENTY_ID " +
			"FROM EMD.ENTY_RLSHP ER, EMD.ENTY E WHERE ER.PARNT_ENTY_ID = ? AND ER.CHILD_ENTY_ID = E.ENTY_ID A" +
			"ND E.ENTY_TYP_CD NOT IN ('PGRP ', 'PROD ')";

	private static final String PRODUCT_IN_HIERARCHY_CHECK_SQL = "SELECT PARNT_ENTY_ID FROM EMD.ENTY_RLSHP " +
			"WHERE CHILD_ENTY_ID = ? AND HIER_CNTXT_CD = ?";

	private static final String PRODUCT_ALREADY_IN_NODE_CHECK_SQL = "SELECT PARNT_ENTY_ID FROM EMD.ENTY_RLSHP " +
			"WHERE PARNT_ENTY_ID = ? AND CHILD_ENTY_ID = ? AND HIER_CNTXT_CD = ?";

	private static final String IS_DEFAULT_PARENT_CHECK_SQL = "SELECT PARNT_ENTY_ID FROM EMD.ENTY_RLSHP " +
			"WHERE PARNT_ENTY_ID = ? AND CHILD_ENTY_ID = ? AND HIER_CNTXT_CD = ? AND DFLT_PARNT_SW = 'Y'";

	private static final String HIERARCHY_CONTEXT_CHECK_SQL = "SELECT HIER_CNTXT_CD FROM EMD.HIER_CNTXT WHERE HIER_CNTXT_CD = ?";

	private static final String MAX_ENTITY_ID_SQL = "SELECT MAX(ENTY_ID) MEI FROM EMD.ENTY";

	private final transient MaintenanceConfig config;

	private final transient ProductLookup productLookup;
	private final transient ProductGroupLookup productGroupLookup;
	private final transient EntityLookup entityLookup;
	private final transient ExtendedProductCustomHierarchyRequest request;

	private transient GenericEntityType childEntityType;
	private transient GenericEntity entityForChild;
	private transient boolean createdNewChild;

	/**
	 * Constructs a new CustomHierarchyMaintenanceImpl.
	 *
	 * @param config The config to use.
	 * @param request The request this class will process.
	 */
	protected CustomHierarchyMaintenanceImpl(MaintenanceConfig config, ExtendedProductCustomHierarchyRequest request) {

		notNullOrIllegalArgument(config, "Config cannot be null.");
		notNullOrIllegalArgument(request, "Request cannot be null");

		config.requireBasics();

		this.config = config;

		this.productLookup = new ProductLookup(this.config.getJdbcTemplate());
		this.productGroupLookup = new ProductGroupLookup(this.config.getJdbcTemplate());
		this.entityLookup = new EntityLookup(this.config.getJdbcTemplate());

		this.request = request;
	}

	/**
	 * Performs validation and the logic to process the request as an add.
	 *
	 * @return The rows to insert and the events to generate for this request.
	 */
	protected TableUpdateSets<EntityRelationship> addProductToCustomHierarchy() {

		this.validateAdd();

		TableUpdateSets<EntityRelationship> updates = new TableUpdateSets<>();

		GenericEntity childEntity = this.getEntityForChild(true).orElseThrow(() -> new IllegalStateException("Child entity not created."));

		// If it's already there, no need to add it again. If we made a new entity, there's no reason to check.
		if (!this.createdNewChild && this.alreadyInNode(this.request.getParentHierarchyLevel(), childEntity.getId(), this.getHierarchyContextCode())) {
			return updates;
		}

		// If it's already in the hierarchy, this needs to be a non-default parent.
		boolean existsInHierarchy = this.alreadyInHierarchy(childEntity.getId(), this.getHierarchyContextCode());

		EntityRelationshipKey key = new EntityRelationshipKey().setParentEntityId(this.request.getParentHierarchyLevel())
				.setChildEntityId(childEntity.getId())
				.setHierarchyContextCode(this.getHierarchyContextCode());

		EntityRelationship entityRelationship = new EntityRelationship().setKey(key)
				.setDefaultParentSwitch(!existsInHierarchy)
				.setDisplaySwitch(Boolean.TRUE)
				.setSequenceNumber(DEFAULT_SEQUENCE_NUMBER)
				.setCreateUserId(this.request.getUserId())
				.setLastUpdateUserId(this.request.getUserId())
				.setEffectiveDate(InstantUtils.startOfToday())
				.setExpirationDate(DatabaseConstants.FOREVER)
				.setActiveSwitch(DatabaseConstants.YES);

		updates.getInserts().add(entityRelationship);

		// Add an ENRM event
		updates.getEvents().add(LegacyEventGenerator.generateENRM(key, this.config.getProgramName(), this.request.getUserId(), LegacyEventFunction.ADD));

		// Add an ENYM event if needed.
		if (this.createdNewChild) {
			updates.getEvents().add(LegacyEventGenerator.generateENYM(childEntity.getId(), this.config.getProgramName(), this.request.getUserId(), LegacyEventFunction.ADD));
		}

		return updates;
	}

	/**
	 * Performs validation and the logic to process the request as a remove.
	 *
	 * @return The rows to delete and the events to generate for this request.
	 */
	protected TableUpdateSets<EntityRelationship> removeProductFromCustomHierarchy() {

		this.validateRemove();

		TableUpdateSets<EntityRelationship> updates = new TableUpdateSets<>();

		Optional<GenericEntity> childEntityWrapper = this.getEntityForChild(false);

		// If there's no child entity, then there's nothing to remove.
		if (childEntityWrapper.isEmpty()) {
			return updates;
		}

		GenericEntity childEntity = childEntityWrapper.get();

		boolean existsAtLevel = this.alreadyInNode(this.request.getParentHierarchyLevel(),  childEntity.getId(), this.getHierarchyContextCode());

		// If it's not in the hierarchy at this level, there's nothing to remove.
		if (!existsAtLevel) {
			return updates;
		}

		EntityRelationshipKey key =  new EntityRelationshipKey().setParentEntityId(this.request.getParentHierarchyLevel())
				.setChildEntityId(childEntity.getId())
				.setHierarchyContextCode(this.getHierarchyContextCode());
		EntityRelationship relationship = new EntityRelationship().setKey(key); // We only need the key.

		updates.getDeletes().add(relationship);

		// Add an ENRM event
		updates.getEvents().add(LegacyEventGenerator.generateENRM(key, this.config.getProgramName(), this.request.getUserId(), LegacyEventFunction.DELETE));

		return updates;
	}

	/**
	 * Creates a new ENTY record for a product or product group.
	 *
	 * @return The GenericEntity that was just inserted.
	 */
	protected GenericEntity createNewEntity() {

		this.config.getLogger().info(String.format("Creating new entity for %d.", this.request.getId()));

		// Get the new entity id.
		Long entityId = this.config.getJdbcTemplate().query(MAX_ENTITY_ID_SQL, new LongResultSetExtractor("MEI"));

		// If there are none...
		if (Objects.isNull(entityId)) {

			// Start at 1.
			entityId = MINIMUM_ENTITY_ID;
		} else {

			// If not, take the previous max and add 1.
			entityId++;
		}

		GenericEntity genericEntity = new GenericEntity().setId(entityId)
				.setEntityType(this.getChildEntityType())
				.setDisplayNumber(this.request.getId())
				.setCreateUserId(this.request.getUserId())
				.setDisplayText(StringUtils.SPACE);

		GenericEntityUpdater genericEntityUpdater = new GenericEntityUpdater(List.of(genericEntity), UpdateType.INSERT);

		this.config.getJdbcTemplate().batchUpdate(GenericEntityUpdater.INSERT_SQL, genericEntityUpdater);

		EntityDescriptionKey descriptionKey = new EntityDescriptionKey()
				.setEntityId(entityId)
				.setHierarchyContextCode(this.getHierarchyContextCode());

		EntityDescription description = new EntityDescription().setKey(descriptionKey)
				.setShortDescription(MasterDataExtendedAttributeKey.KEY_TYPE_PRODUCT)
				.setLongDescription(StringUtils.SPACE)
				.setCreateUserId(this.request.getUserId());

		EntityDescriptionUpdater descriptionUpdater = new EntityDescriptionUpdater(List.of(description), UpdateType.INSERT);
		this.config.getJdbcTemplate().batchUpdate(EntityDescriptionUpdater.INSERT_SQL, descriptionUpdater);

		this.createdNewChild = true;

		return genericEntity;
	}

	/**
	 * Returns the GenericEntityType that matches the request.
	 *
	 * @return The GenericEntityType that matches the request.
	 */
	protected GenericEntityType getChildEntityType() {

		if (Objects.isNull(this.childEntityType)) {
			this.childEntityType = Objects.equals(ProductCustomHierarchyRequest.Type.PRODUCT_GROUP, this.request.getIdType()) ?
					GenericEntityType.PRODUCT_GROUP : GenericEntityType.PRODUCT;
		}

		return this.childEntityType;
	}


	/**
	 * Returns the GenericEntity that ties to the ENTY record for the product or group being processed.
	 *
	 * @param createIfNotFound Whether or not to create the ENTY record if it doesn't exist. True will make the record
	 *                         and false will return empty.
	 * @return The GenericEntity tied to the child in the request or empty.
	 */
	protected Optional<GenericEntity> getEntityForChild(boolean createIfNotFound) {

		if (Objects.nonNull(this.entityForChild)) {
			return Optional.of(this.entityForChild);
		}

		Optional<GenericEntity> lookupChild = this.entityLookup.findByTypeAndExternalId(this.getChildEntityType(), this.request.getId());

		if (lookupChild.isPresent()) {

			this.entityForChild = lookupChild.get();
		} else {

			if (createIfNotFound) {
				this.entityForChild = this.createNewEntity();
			}
		}

		return Optional.ofNullable(this.entityForChild);
	}

	/**
	 * Does the validations when adding a new mapping.
	 *
	 * @throws ValidationException If validation fails.
	 */
	protected void validateAdd() {

		List<String> errors = this.validate();

		this.validateHierarchyLevelForAdd().ifPresent(errors::add);

		// There's some extra checks for MAT.
		this.validateMatEntry().ifPresent(errors::add);

		if (!errors.isEmpty()) {
			throw new ValidationException("Unable to validate custom hierarchy maintenance request.", errors);
		}
	}

	/**
	 * Does the validations when removing a mapping.
	 *
	 * @throws ValidationException If validation fails.
	 */
	protected void validateRemove() {

		List<String> errors = this.validate();

		if (!errors.isEmpty()) {
			throw new ValidationException("Unable to validate custom hierarchy maintenance request.", errors);
		}

		// We should only do this validation if it passes the above as it relies on
		// parts of that being OK.
		this.validateDefaultParent().ifPresent(errors::add);

		if (!errors.isEmpty()) {
			throw new ValidationException("Unable to validate custom hierarchy maintenance request.", errors);
		}
	}

	/**
	 * Performs the most basic validations.
	 *
	 * @return A list of error messages. This list may be empty, but will not be null.
	 */
	protected List<String> validate() {

		List<String> errors = new LinkedList<>();

		if (Objects.isNull(this.request.getUserId())) {
			errors.add("User ID is required.");
		}

		// Make sure the request contains a valid product or product group.
		this.validateProductId().ifPresent(errors::add);

		// Validate the hierarchy context.
		this.validateHierarchy().ifPresent(errors::add);

		// Make sure the hierarchy level exists and is a lowest level or empty.
		this.validateHierarchyLevel();

		return errors;
	}

	/**
	 * When removing a record, makes sure that the default parent is not being removed if other children exist.
	 *
	 * @return An error message or empty.
	 */
	protected Optional<String> validateDefaultParent() {

		Optional<GenericEntity> childEntityWrapper = this.getEntityForChild(false);

		// If there's no child entity, then we won't remove anything, so nothing to validate.
		if (childEntityWrapper.isEmpty()) {
			return Optional.empty();
		}

		// See how many records for this child are in the hierarchy.
		int numberInHierarchy = JdbcUtils.runCountQuery(this.config.getJdbcTemplate(), PRODUCT_IN_HIERARCHY_CHECK_SQL,
				JdbcUtils.argsAsArray(childEntityWrapper.get().getId(), this.getHierarchyContextCode().getId()));

		// If it's not in the hierarchy, or there's only one, you can remove it.
		if (numberInHierarchy <= ONLY_ONE) {
			return Optional.empty();
		}

		// If we get here, there's more than one entry in the hierarchy, and we can't remove the default parent.
		if (JdbcUtils.runCountQuery(this.config.getJdbcTemplate(), IS_DEFAULT_PARENT_CHECK_SQL,
				JdbcUtils.argsAsArray(this.request.getParentHierarchyLevel(), childEntityWrapper.get().getId(), this.getHierarchyContextCode().getId()))  > 0) {

			return Optional.of("You cannot remove the default parent if there are non-defaults.");
		}

		return Optional.empty();
	}

	/**
	 * Makes sure the hierarchy ID is valid.
	 *
	 * @return An error message or empty.
	 */
	protected Optional<String> validateHierarchy() {

		if (Objects.isNull(this.request.getHierarchyContext())) {
			return Optional.of("A hierarchy context is required.");
		}

		String formattedContext = this.getHierarchyContextCode().getId();

		if (JdbcUtils.runCountQuery(this.config.getJdbcTemplate(), HIERARCHY_CONTEXT_CHECK_SQL, JdbcUtils.argsAsArray(formattedContext)) == 0) {
			return Optional.of(String.format("%s is not a valid hierarchy context.", this.request.getHierarchyContext()));
		}

		return Optional.empty();
	}

	/**
	 * Makes sure the level of the hierarchy the user is adding the product to is valid.
	 *
	 * @return An error message or empty.
	 */
	protected Optional<String> validateHierarchyLevel() {

		// We need a value.
		if (Objects.isNull(this.request.getParentHierarchyLevel())) {
			return Optional.of("A parent hierarchy level is required.");
		}

		// This is a problem, but it'll be caught elsewhere. Returning
		// empty will keep from having duplicate error messages.
		if (Objects.isNull(this.request.getHierarchyContext())) {
			return Optional.empty();
		}

		// Make sure it exists.
		Optional<GenericEntity> parentEntityWrapper = this.entityLookup.findById(request.getParentHierarchyLevel());
		if (parentEntityWrapper.isEmpty()) {
			return Optional.of(String.format("%d is not a valid hierarchy level.", this.request.getParentHierarchyLevel()));
		}

		// Make sure it's a custom hierarchy level.
		GenericEntity parentEntity = parentEntityWrapper.get();
		if (!Objects.equals(GenericEntityType.CUSTOM_HIERARCHY_LEVEL, parentEntity.getEntityType())) {
			return Optional.of(String.format("%d is not a custom hierarchy level.", this.request.getParentHierarchyLevel()));
		}

		return Optional.empty();
	}

	/**
	 * Does additional hierarchy level validation when adding products to the hierarchy.
	 *
	 * @return An error message or empty.
	 */
	protected Optional<String> validateHierarchyLevelForAdd() {

		// Make sure it only contains products and groups or is empty.
		if (JdbcUtils.runCountQuery(this.config.getJdbcTemplate(), PARENT_IS_LOWEST_LEVEL_CHECK_SQL,
				JdbcUtils.argsAsArray(this.request.getParentHierarchyLevel())) != 0) {
			return Optional.of(String.format("%d contains non-product or product-group children.", request.getParentHierarchyLevel()));
		}

		return Optional.empty();
	}

	/**
	 * Validates the product or group ID the user is adding.
	 *
	 * @return An error message or empty.
	 */
	protected Optional<String> validateProductId() {

		if (Objects.isNull(this.request.getId()) || Objects.isNull(this.request.getIdType())) {
			return Optional.of("An ID and type are required.");
		}

		if (Objects.equals(ProductCustomHierarchyRequest.Type.PRODUCT, this.request.getIdType())) {
			if (!this.productLookup.isProductId(this.request.getId())) {
				return Optional.of(String.format("%d is not a valid product ID.", this.request.getId()));
			}
		} else {
			if (!this.productGroupLookup.isProductGroupId(this.request.getId())) {
				return Optional.of(String.format("%d is not a valid product group ID.", this.request.getId()));
			}
		}

		return Optional.empty();
	}

	/**
	 * There are additional validations if the user is adding the product to the MAT hierarchy.
	 *
	 * @return An error message or empty.
	 */
	protected Optional<String> validateMatEntry() {

		// We only need to do these checks for MAT.
		if (!Objects.equals(HierarchyContextCode.MASTER_ATTRIBUTE_TAXONOMY, this.getHierarchyContextCode())) {

			return Optional.empty();
		}

		if (Objects.equals(this.request.getIdType(), ProductCustomHierarchyRequest.Type.PRODUCT_GROUP)) {
			return Optional.of("Only products can be added to the MAT hierarchy.");
		}

		// If there's not an entity, then we know this is OK.
		Optional<GenericEntity> childEntity = this.getEntityForChild(false);
		if (childEntity.isEmpty()) {
			return Optional.empty();
		}

		// It can only be in the MAT hierarchy once.
		if (this.alreadyInHierarchy(childEntity.get().getId(), this.getHierarchyContextCode())) {
			return Optional.of(String.format("Product %d already exists in the MAT hierarchy.", this.request.getId()));
		}

		return Optional.empty();
	}

	/**
	 * Returns whether or not a product or group is already somewhere in the hierarchy.
	 *
	 * @param childEntityId The ENTY_ID of the product or group.
	 * @param hierarchyContextCode The hierarchy context to check.
	 * @return True if the product or group is already in the hierarchy and false otherwise.
	 */
	protected boolean alreadyInHierarchy(long childEntityId, HierarchyContextCode hierarchyContextCode) {

		return JdbcUtils.runCountQuery(this.config.getJdbcTemplate(),
				PRODUCT_IN_HIERARCHY_CHECK_SQL,
				JdbcUtils.argsAsArray(childEntityId, hierarchyContextCode.getId())) > 0;
	}

	/**
	 * Returns whether or not a child is already at a given location in the hierarchy.
	 *
	 * @param parentEntityId The hierarchy location to check.
	 * @param childEntityId The ENTY_ID of the product or group.
	 * @param hierarchyContextCode The hierarchy context to check.
	 * @return True if the child is already in the requested hierarchy level and false otherwise.
	 */
	protected boolean alreadyInNode(long parentEntityId, long childEntityId, HierarchyContextCode hierarchyContextCode) {

		return JdbcUtils.runCountQuery(this.config.getJdbcTemplate(),
				PRODUCT_ALREADY_IN_NODE_CHECK_SQL,
				JdbcUtils.argsAsArray(parentEntityId, childEntityId, hierarchyContextCode.getId()))  > 0;
	}

	/**
	 * Converts the hierarchy context string in the request to a HierarchyContextCode.
	 *
	 * @return The HierarchyContextCode corresponding to the code in the request.
	 */
	protected HierarchyContextCode getHierarchyContextCode() {

		return HierarchyContextCode.of(this.request.getHierarchyContext());
	}
}
