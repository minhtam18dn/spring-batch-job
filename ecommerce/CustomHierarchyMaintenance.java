package com.heb.pm.core.service.maintenance.ecommerce;

import com.heb.pm.core.exception.ValidationException;
import com.heb.pm.core.service.maintenance.common.MaintenanceConfig;
import com.heb.pm.core.service.maintenance.common.TableUpdateSets;
import com.heb.pm.dao.core.entity.EntityRelationship;
import com.heb.pm.dao.core.preparedstatementsetters.EntityRelationshipUpdater;
import com.heb.pm.dao.core.preparedstatementsetters.UpdateType;
import org.springframework.transaction.annotation.Transactional;

import java.util.*;

/**
 * Handles maintaining custom hierarchies.
 *
 * @author d116773
 * @since 1.27.0
 */
public class CustomHierarchyMaintenance {

	private final transient MaintenanceConfig config;

	/**
	 * Constructs a new CustomHierarchyMaintenance.
	 *
	 * @param config The config for the class to use.
	 */
	public CustomHierarchyMaintenance(MaintenanceConfig config) {
		this.config = config;
	}

	/**
	 * Can be called by external classes to perform a validation on a ExtendedProductCustomHierarchyRequest to insert.
	 *
	 * @param request The ExtendedProductCustomHierarchyRequest to validate.
	 * @throws ValidationException Any validation errors.
	 */
	public void validateAdd(ExtendedProductCustomHierarchyRequest request) {

		this.doValidation(request,  false);
	}

	/**
	 * Can be called by external classes to perform a validation on a ExtendedProductCustomHierarchyRequest to delete.
	 *
	 * @param request The ExtendedProductCustomHierarchyRequest to validate.
	 * @throws ValidationException Any validation errors.
	 */
	public void validateRemove(ExtendedProductCustomHierarchyRequest request) {

		this.doValidation(request, true);
	}

	/**
	 * Adds a collection of products to a custom hierarchy.
	 *
	 * @param addRequests The list of ExtendedProductCustomHierarchyRequests to process.
	 * @return The number of rows inserted.
	 * @throws ValidationException Any validation errors.
	 */
	@Transactional
	public int addProductsToCustomHierarchy(List<ExtendedProductCustomHierarchyRequest> addRequests) {

		this.config.requireForProcessing();

		if (this.containsSameProductMoreThanOnce(addRequests)) {
			return this.addRequestsOneAtATime(addRequests);
		} else {
			return this.addRequestsInBunch(addRequests);
		}
	}

	/**
	 * Removes a list of products from a custom hierarchy.
	 *
	 * @param removeRequests The list of ExtendedProductCustomHierarchyRequests to process.
	 * @return The number of rows deleted.
	 * @throws ValidationException Any validation errors.
	 */
	@Transactional
	public int removeProductsFromCustomHierarchy(List<ExtendedProductCustomHierarchyRequest> removeRequests) {

		this.config.requireForProcessing();

		TableUpdateSets<EntityRelationship> deletes = new TableUpdateSets<>();

		removeRequests.stream()
				.map(this::mapToDeletes)
				.forEach(deletes::addAll);

		// Delete the records.
		int rowsDeleted = this.doDeletes(deletes.getDeletes());

		// Publish any events needed.
		this.config.getLegacyEventProcessor().addAndFlush(deletes.getEvents());

		return rowsDeleted;
	}

	/**
	 * Delegate function for doing the validations only.
	 *
	 * @param request The request to validate.
	 * @param isRemove Is this a remove?
	 */
	private void doValidation(ExtendedProductCustomHierarchyRequest request, boolean isRemove) {

		if (Objects.isNull(request)) {
			throw new ValidationException("Unable to validate request.", "Request cannot be null.");
		}

		this.config.requireBasics();

		CustomHierarchyMaintenanceImpl validator = new CustomHierarchyMaintenanceImpl(this.config, request);

		if (isRemove) {
			validator.validateRemove();
		} else {
			validator.validateAdd();
		}
	}

	/**
	 * Adds products to custom hierarchy levels. This function will process them one at a time
	 * and should be called if the same product (or ID) is in the list more than once.
	 *
	 * @param addRequests The list of ExtendedProductCustomHierarchyRequests to process.
	 * @return The number of rows inserted.
	 */
	private int addRequestsOneAtATime(List<ExtendedProductCustomHierarchyRequest> addRequests) {

		int rowsInserted = 0;

		for (ExtendedProductCustomHierarchyRequest request : addRequests) {

			TableUpdateSets<EntityRelationship> inserts = this.mapToInserts(request);

			rowsInserted += this.doInserts(inserts.getInserts());
			this.config.getLegacyEventProcessor().addAndFlush(inserts.getEvents());
		}

		return rowsInserted;
	}

	/**
	 * Adds products to custom hierarchy levels. This function is more efficient than
	 * addRequestsOneAtATime, but can only handle instances where each request has a unique product or group.
	 *
	 * @param addRequests The list of ExtendedProductCustomHierarchyRequests to process.
	 * @return The number of rows inserted.
	 */
	private int addRequestsInBunch(List<ExtendedProductCustomHierarchyRequest> addRequests) {

		TableUpdateSets<EntityRelationship> inserts = new TableUpdateSets<>();

		addRequests.stream()
				.map(this::mapToInserts)
				.forEach(inserts::addAll);

		// Insert the records.
		int rowsInserted = this.doInserts(inserts.getInserts());

		// Publish any events needed.
		this.config.getLegacyEventProcessor().addAndFlush(inserts.getEvents());

		return rowsInserted;
	}

	/**
	 * Checks a list of requests to see if a product (or group) is in the list more than once. If they are, then
	 * we need to process them one at a time instead of in a group since we have the default parent thing to deal with.
	 *
	 * @param addRequests The list of requests to check.
	 * @return True if any product (or group) ID is in the list more than once.
	 */
	private boolean containsSameProductMoreThanOnce(List<ExtendedProductCustomHierarchyRequest> addRequests) {

		Set<Long> productsViewed = new HashSet<>();
		for (ExtendedProductCustomHierarchyRequest request : addRequests) {
			if (productsViewed.contains(request.getId())) {
				return true;
			}
			productsViewed.add(request.getId());
		}

		return false;
	}

	/**
	 * Does the actual inserts into ENTY_RLSHP.
	 *
	 * @param recordsToInsert The EntityRelationships to insert.
	 * @return The number of rows inserted.
	 */
	private int doInserts(Collection<EntityRelationship> recordsToInsert) {

		if (recordsToInsert.isEmpty()) {
			return 0;
		}

		EntityRelationshipUpdater inserter = new EntityRelationshipUpdater(recordsToInsert, UpdateType.INSERT);
		int[] rowsInserted = this.config.getJdbcTemplate().batchUpdate(EntityRelationshipUpdater.INSERT_SQL, inserter);
		return Arrays.stream(rowsInserted).sum();
	}

	/**
	 * Does the actual deletes from ENTY_RLSHP.
	 *
	 * @param recordsToDelete The EntityRelationships to delete.
	 * @return The number of rows inserted.
	 */
	private int doDeletes(Collection<EntityRelationship> recordsToDelete) {

		if (recordsToDelete.isEmpty()) {
			return 0;
		}

		EntityRelationshipUpdater deleter = new EntityRelationshipUpdater(recordsToDelete, UpdateType.DELETE);
		int[] rowsDeleted = this.config.getJdbcTemplate().batchUpdate(EntityRelationshipUpdater.DELETE_SQL, deleter);
		return Arrays.stream(rowsDeleted).sum();
	}


	/**
	 * Maps a ExtendedProductCustomHierarchyRequest to a TableUpdateSets with the EntityRelationships to insert.
	 *
	 * @param addRequest The ExtendedProductCustomHierarchyRequest to process.
	 * @return The TableUpdateSets to insert.
	 */
	private TableUpdateSets<EntityRelationship> mapToInserts(ExtendedProductCustomHierarchyRequest addRequest) {

		CustomHierarchyMaintenanceImpl implClass = new CustomHierarchyMaintenanceImpl(this.config, addRequest);
		return implClass.addProductToCustomHierarchy();
	}

	/**
	 * Maps a ExtendedProductCustomHierarchyRequest to a TableUpdateSets with the EntityRelationships to delete.
	 *
	 * @param deleteRequest The ExtendedProductCustomHierarchyRequest to process.
	 * @return The TableUpdateSets to delete.
	 */
	private TableUpdateSets<EntityRelationship> mapToDeletes(ExtendedProductCustomHierarchyRequest deleteRequest) {

		CustomHierarchyMaintenanceImpl implClass = new CustomHierarchyMaintenanceImpl(this.config, deleteRequest);
		return implClass.removeProductFromCustomHierarchy();
	}
}
