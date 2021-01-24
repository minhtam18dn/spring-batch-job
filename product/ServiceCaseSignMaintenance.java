package com.heb.pm.core.service.maintenance.product;

import com.heb.pm.core.exception.ValidationException;
import com.heb.pm.core.maintenance.ProductMaintenanceRequest;
import com.heb.pm.core.service.maintenance.common.MaintenanceConfig;
import com.heb.pm.core.service.maintenance.common.TableUpdateSets;
import com.heb.pm.dao.core.LegacyEventGenerator;
import com.heb.pm.dao.core.entity.LegacyEvent;
import com.heb.pm.dao.core.entity.ProductDescription;
import com.heb.pm.dao.core.entity.codes.DescriptionType;
import com.heb.pm.dao.core.entity.codes.LanguageType;
import com.heb.pm.dao.core.entity.codes.LegacyEventFunction;
import com.heb.pm.dao.core.quicklookup.ProductLookup;
import org.apache.commons.collections4.CollectionUtils;
import org.springframework.lang.NonNull;
import org.springframework.lang.Nullable;
import org.springframework.transaction.annotation.Transactional;

import java.util.*;

/**
 * Class that manages product data related to service case signs.
 *
 * This includes:
 * 1. Service case callout description
 * 2. Approved service case description
 * 3. Proposed service case description
 *
 * @author m314029
 * @since 1.25.0
 */
// Since this can be called from batch jobs, I want to allow for different loggers. To do this, it is
// passed in as a parameter to the constructor, so the logger can't be static or final.
@SuppressWarnings("PMD.LoggerIsNotStaticFinal")
public class ServiceCaseSignMaintenance {

	private static final String PRODUCT_NOT_FOUND_ERROR = "%d is not a product.";
	private static final String DESCRIPTION_CHANGE_NOT_FOUND_ERROR = "At least one description change is required.";
	private static final String UPDATING_CALLOUT_DESCRIPTION_TEXT = "Updating service case callout of product {} to '{}'.";
	private static final String UPDATING_ROMANCE_DESCRIPTION_TEXT = "Updating service case (romance) description of product {} to '{}'.";
	private static final String UPDATING_PROPOSED_DESCRIPTION_TEXT = "Updating service case (proposed) description of product {} to '{}'.";

	private final transient ProductDescriptionMaintenance productDescriptionMaintenance;
	private final transient ProductLookup productLookup;
	private final transient MaintenanceConfig config;
	private final transient TagTypeMaintenance tagTypeMaintenance;
	private final transient ServiceCaseSignLookup serviceCaseSignLookup;

	/**
	 * Constructs a new ServiceCaseSignMaintenance.
	 *
	 * @param config The configuration for this class to use.
	 */
	public ServiceCaseSignMaintenance(MaintenanceConfig config) {
		this.config = config;
		this.productLookup = new ProductLookup(config.getJdbcTemplate());
		this.productDescriptionMaintenance = new ProductDescriptionMaintenance(config);
		this.tagTypeMaintenance = new TagTypeMaintenance(config);
		this.serviceCaseSignLookup = new ServiceCaseSignLookup(config.getJdbcTemplate());
	}

	/**
	 * Generate the legacy events used for changes to a service case sign.
	 *
	 * @param productId Product ID to be modified.
	 * @param programName Program to assign making the change.
	 * @param lastUpdatedUser Who is requesting the change.
	 * @param function Function to apply to legacy event.
	 * @return List of created events.
	 */
	private static List<LegacyEvent> generateEventsForExtendedServiceCaseSignRequest(
			@NonNull Long productId, @NonNull String programName, @NonNull String lastUpdatedUser, @NonNull LegacyEventFunction function) {
		List<LegacyEvent> toReturn = new ArrayList<>();
		toReturn.add(LegacyEventGenerator.generatePRMM(productId, programName, lastUpdatedUser, function));
		toReturn.add(LegacyEventGenerator.generatePRM2(productId, programName, lastUpdatedUser, function));
		return toReturn;
	}

	/**
	 * Handles processing a ExtendedServiceCaseSignRequest.
	 *
	 * @param request The user's request.
	 */
	@Transactional
	public void updateServiceCaseSign(@Nullable ExtendedServiceCaseSignRequest request) {
		if (Objects.isNull(request)) {
			return;
		}

		this.validateRequest(request);

		boolean tagChange = this.handleTagMaintenance(request.getProductId(), request.getTagType(), request.getUserId());
		TableUpdateSets<ProductDescription> updates = this.processServiceCaseSign(request);

		// the tag updates already creates PRMM and PRM2 events, so only generate if needed
		if (!tagChange && updates.hasTableChanges()) {
			updates.getEvents().addAll(generateEventsForExtendedServiceCaseSignRequest(
					request.getProductId(), this.config.getProgramName(), request.getUserId(), LegacyEventFunction.UPDATE));
			this.config.getLegacyEventProcessor().addAndFlush(updates.getEvents());
		}
		this.productDescriptionMaintenance.handleAllChanges(updates);
	}

	/**
	 * Handles processing multiple ExtendedServiceCaseSignRequest.
	 *
	 * @param requests The user's requests.
	 */
	@Transactional
	public void updateServiceCaseSigns(@Nullable List<ExtendedServiceCaseSignRequest> requests) {

		if (CollectionUtils.isEmpty(requests)) {
			return;
		}

		TableUpdateSets<ProductDescription> updates = new TableUpdateSets<>();
		boolean tagChange;
		for (ExtendedServiceCaseSignRequest request : requests) {
			if (Objects.isNull(request)) {
				continue;
			}
			this.validateRequest(request);
			tagChange = this.handleTagMaintenance(request.getProductId(), request.getTagType(), request.getUserId());
			TableUpdateSets<ProductDescription> currentUpdates = this.processServiceCaseSign(request);

			// the tag updates already creates PRMM and PRM2 events, so only generate if needed
			if (!tagChange && currentUpdates.hasTableChanges()) {
				currentUpdates.getEvents().addAll(generateEventsForExtendedServiceCaseSignRequest(
						request.getProductId(), this.config.getProgramName(), request.getUserId(), LegacyEventFunction.UPDATE));
			}
			updates.addAll(currentUpdates);
		}
		if (updates.hasEvents()) {
			this.config.getLegacyEventProcessor().addAndFlush(updates.getEvents());
		}
		this.productDescriptionMaintenance.handleAllChanges(updates);
	}

	/**
	 * Holds the logic of what to do with all the existing and new service case sign description events.
	 *
	 * @param request The user's request.
	 * @return Any updates after processing user request.
	 */
	private TableUpdateSets<ProductDescription> processServiceCaseSign(@NonNull ExtendedServiceCaseSignRequest request) {

		// if there are no changes, then do nothing.
		if (Objects.isNull(request.getDescription()) &&
				Objects.isNull(request.getProposedDescription()) &&
				Objects.isNull(request.getCalloutDescription())) {
			return new TableUpdateSets<>();
		}

		List<ProductDescription> existingDescriptions = this.serviceCaseSignLookup.findAllByProductIdAndLanguageType(
				request.getProductId(), LanguageType.ENGLISH);
		return this.findDifferences(request, existingDescriptions);
	}

	/**
	 * Find differences between existing service case sign descriptions and any values set in the user's request, then
	 * returns any updates to perform.
	 *
	 * @param request Request from the user.
	 * @param existingDescriptions Any existing service case sign descriptions.
	 * @return Any updates for service case sign descriptions.
	 */
	private TableUpdateSets<ProductDescription> findDifferences(@NonNull ExtendedServiceCaseSignRequest request,
																@NonNull List<ProductDescription> existingDescriptions) {
		TableUpdateSets<ProductDescription> updateSets = new TableUpdateSets<>();
		Optional<ProductDescription> serviceCaseCalloutDescription = Optional.empty();
		Optional<ProductDescription> serviceCaseDescription = Optional.empty();
		Optional<ProductDescription> serviceCaseProposedDescription = Optional.empty();
		for (ProductDescription productDescription : existingDescriptions) {
			switch (productDescription.getKey().getDescriptionType()) {
				case SERVICE_CASE_CALLOUT: {
					serviceCaseCalloutDescription = Optional.of(productDescription);
					break;
				}
				case SIGN_ROMANCE: {
					serviceCaseDescription = Optional.of(productDescription);
					break;
				}
				case SIGN_ROMANCE_PROPOSED: {
					serviceCaseProposedDescription = Optional.of(productDescription);
					break;
				}
				default: {
					// Description is not a service case sign type.
					break;
				}
			}
		}

		updateSets.addAll(processServiceCaseSignCalloutDescription(request, serviceCaseCalloutDescription.orElse(null)));
		updateSets.addAll(processServiceCaseSignDescription(request, serviceCaseDescription.orElse(null)));
		updateSets.addAll(processServiceCaseSignProposedDescription(request, serviceCaseProposedDescription.orElse(null)));

		return updateSets;
	}

	/**
	 * Validates an ExtendedServiceCaseSignRequest to make sure it's OK to process.
	 *
	 * @param productDescriptionRequest The request to validate.
	 */
	public void validateRequest(@Nullable ExtendedServiceCaseSignRequest productDescriptionRequest) {

		if (Objects.isNull(productDescriptionRequest)) {
			return;
		}

		List<String> errors = new LinkedList<>();

		if (Objects.isNull(productDescriptionRequest.getUserId())) {
			errors.add("User ID is required.");
		}

		if (Objects.isNull(productDescriptionRequest.getProductId())) {
			errors.add("Product ID is required.");
		} else if (!this.productLookup.isProductId(productDescriptionRequest.getProductId())) {
			errors.add(String.format(PRODUCT_NOT_FOUND_ERROR, productDescriptionRequest.getProductId()));
		}

		if (Objects.isNull(productDescriptionRequest.getDescription()) &&
				Objects.isNull(productDescriptionRequest.getProposedDescription()) &&
				Objects.isNull(productDescriptionRequest.getCalloutDescription())) {
			errors.add(DESCRIPTION_CHANGE_NOT_FOUND_ERROR);
		}

		if (!errors.isEmpty()) {
			throw new ValidationException("Unable to validate product description request.", errors);
		}
	}

	/**
	 * Processes a work request for a single product's service case callout. If the service case callout description is
	 * present, it will update the product accordingly.
	 *
	 * @param request The request for the change.
	 * @param existingProductDescription The existing service case callout description.
	 * @return All updates to service case callout.
	 */
	private TableUpdateSets<ProductDescription> processServiceCaseSignCalloutDescription(
			@NonNull ExtendedServiceCaseSignRequest request, @Nullable ProductDescription existingProductDescription) {

		if (Objects.nonNull(request.getCalloutDescription())) {

			this.config.getLogger().info(UPDATING_CALLOUT_DESCRIPTION_TEXT, request.getProductId(), request.getCalloutDescription());

			TableUpdateSets<ProductDescription> updatesToMake = new TableUpdateSets<>();
			if (Objects.nonNull(existingProductDescription)) {
				existingProductDescription.setDescription(request.getCalloutDescription());
				existingProductDescription.setLstUpdateUserId(request.getUserId());
				updatesToMake.getUpdates().add(existingProductDescription);
			} else {
				ProductDescription newProductDescription = ProductDescription.of(
						request.getProductId(), LanguageType.ENGLISH.getId(),
						DescriptionType.SERVICE_CASE_CALLOUT);
				newProductDescription
						.setDescription(request.getCalloutDescription())
						.setLstUpdateUserId(request.getUserId());
				updatesToMake.getInserts().add(newProductDescription);
			}
			return updatesToMake;
		} else {
			return new TableUpdateSets<>();
		}
	}

	/**
	 * Processes a work request for a single product's service case description. If the service case description is
	 * present, it will update the product accordingly.
	 *
	 * @param request The request for the change.
	 * @param existingProductDescription The existing service case description.
	 * @return All updates to service case description.
	 */
	private TableUpdateSets<ProductDescription> processServiceCaseSignDescription(
			@NonNull ExtendedServiceCaseSignRequest request, @Nullable ProductDescription existingProductDescription) {

		if (Objects.nonNull(request.getDescription())) {

			this.config.getLogger().info(UPDATING_ROMANCE_DESCRIPTION_TEXT, request.getProductId(), request.getDescription());

			TableUpdateSets<ProductDescription> updatesToMake = new TableUpdateSets<>();
			if (Objects.nonNull(existingProductDescription)) {
				existingProductDescription.setDescription(request.getDescription());
				existingProductDescription.setLstUpdateUserId(request.getUserId());
				updatesToMake.getUpdates().add(existingProductDescription);
			} else {
				ProductDescription newProductDescription = ProductDescription.of(
						request.getProductId(), LanguageType.ENGLISH.getId(),
						DescriptionType.SIGN_ROMANCE);
				newProductDescription
						.setDescription(request.getDescription())
						.setLstUpdateUserId(request.getUserId());
				updatesToMake.getInserts().add(newProductDescription);
			}
			return updatesToMake;
		} else {
			return new TableUpdateSets<>();
		}
	}

	/**
	 * Processes a work request for a single product's service case proposed description. If the service case
	 * proposed description is present, it will update the product accordingly.
	 *
	 * @param request The request for the change.
	 * @param existingProductDescription The existing service case proposed description.
	 * @return All updates to service case proposed description.
	 */
	private TableUpdateSets<ProductDescription> processServiceCaseSignProposedDescription(
			@NonNull ExtendedServiceCaseSignRequest request, @Nullable ProductDescription existingProductDescription) {

		if (Objects.nonNull(request.getProposedDescription())) {

			this.config.getLogger().info(UPDATING_PROPOSED_DESCRIPTION_TEXT, request.getProductId(), request.getProposedDescription());
			TableUpdateSets<ProductDescription> updatesToMake = new TableUpdateSets<>();
			if (Objects.nonNull(existingProductDescription)) {
				existingProductDescription.setDescription(request.getProposedDescription());
				existingProductDescription.setLstUpdateUserId(request.getUserId());
				updatesToMake.getUpdates().add(existingProductDescription);
			} else {
				ProductDescription newProductDescription = ProductDescription.of(
						request.getProductId(), LanguageType.ENGLISH.getId(),
						DescriptionType.SIGN_ROMANCE_PROPOSED);
				newProductDescription
						.setDescription(request.getProposedDescription())
						.setLstUpdateUserId(request.getUserId());
				updatesToMake.getInserts().add(newProductDescription);
			}
			return updatesToMake;
		} else {
			return new TableUpdateSets<>();
		}
	}

	/**
	 * Processes a work request for a single product. If the tag type is present, it will update the product accordingly.
	 * This method returns whether there was a tag change or not, to assist in not creating duplicate legacy events.
	 *
	 * @param productId The ID of the product to do the update on.
	 * @param tagType The tag type to set.
	 * @return Whether there was a tag type change.
	 */
	@Transactional
	protected boolean handleTagMaintenance(long productId, String tagType, String user) {

		if (Objects.nonNull(tagType)) {
			ProductMaintenanceRequest productMaintenanceRequest = new ProductMaintenanceRequest()
					.setTagType(tagType)
					.setUserId(user);
			this.tagTypeMaintenance.handleTagMaintenance(productId, productMaintenanceRequest);
			return true;
		} else {
			return false;
		}
	}
}
