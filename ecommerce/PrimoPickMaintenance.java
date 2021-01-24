package com.heb.pm.core.service.maintenance.ecommerce;

import com.heb.pm.core.event.LegacyEventProcessor;
import com.heb.pm.core.exception.ValidationException;
import com.heb.pm.core.model.PrimoPick;
import com.heb.pm.core.service.maintenance.common.MaintenanceConfig;
import com.heb.pm.core.service.maintenance.common.TableUpdateSets;
import com.heb.pm.dao.core.DatabaseConstants;
import com.heb.pm.dao.core.LegacyEventGenerator;
import com.heb.pm.dao.core.entity.*;
import com.heb.pm.dao.core.entity.codes.*;
import com.heb.pm.dao.core.preparedstatementsetters.ProductDescriptionUpdater;
import com.heb.pm.dao.core.preparedstatementsetters.ProductMarketingClaimUpdater;
import com.heb.pm.dao.core.preparedstatementsetters.UpdateType;
import com.heb.pm.dao.core.quicklookup.ProductLookup;
import com.heb.pm.util.DateUtils;
import com.heb.pm.util.JdbcUtils;
import com.heb.pm.util.MappingUtils;
import org.slf4j.Logger;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.annotation.Transactional;

import java.util.*;

/**
 * Extracts Primo-Pick specific functions from the ProductMarketingClaimMaintenance class.
 *
 * @author d116773
 * @since 1.23.0
 */
// Since this can be called from batch jobs, I want to allow for different loggers. To do this, it is
// passed in as a parameter to the constructor, so the logger can't be static or final.
@SuppressWarnings("PMD.LoggerIsNotStaticFinal")
public class PrimoPickMaintenance {

	private static final String DESCRIPTION_LENGTH_SQL = "SELECT DES_LN FROM EMD.DES_TYP WHERE DES_TYP_CD = ?";

	private final transient JdbcTemplate jdbcTemplate;
	private final transient ProductMarketingClaimLookup productMarketingClaimLookup;
	private final transient ProductLookup productLookup;
	private final transient Logger logger;
	private final transient String jobName;
	private final transient LegacyEventProcessor legacyEventProcessor;
	private final transient ProductMarketingClaimMaintenance productMarketingClaimMaintenance;
	private final transient ProductDescriptionLookup productDescriptionLookup;

	private transient int maximumProposedDescriptionSize;
	private transient int maximumDescriptionSize;

	/**
	 * Constructs a new PrimoPickMaintenance.
	 *
	 * @param config The config for this class.
	 */
	public PrimoPickMaintenance(MaintenanceConfig config) {

		this(config, new ProductMarketingClaimMaintenance(config));
	}

	/**
	 * Constructs a new PrimoPickMaintenance.
	 *
	 * @param config The config for this class.
	 * @param productMarketingClaimMaintenance A ProductMarketingClaimMaintenance to mark things as distinctive if needed.
	 */
	public PrimoPickMaintenance(MaintenanceConfig config,
											ProductMarketingClaimMaintenance productMarketingClaimMaintenance) {

		this.jdbcTemplate = config.getJdbcTemplate();
		this.logger = config.getLogger();
		this.jobName = config.getProgramName();
		this.productMarketingClaimLookup = new ProductMarketingClaimLookup(this.jdbcTemplate);
		this.productLookup = new ProductLookup(this.jdbcTemplate);
		this.legacyEventProcessor = config.getLegacyEventProcessor();
		this.productMarketingClaimMaintenance = productMarketingClaimMaintenance;
		this.productDescriptionLookup = new ProductDescriptionLookup(this.jdbcTemplate);
	}

	/**
	 * Processes a list of primo pick requests.
	 *
	 * @param primoPickRequests The list of primo pick requests to process.
	 * @param userId The ID of the user making the request.
	 */
	@Transactional
	public void handlePrimoPick(List<ExtendedPrimoPickRequest> primoPickRequests, String userId) {

		TableUpdateSets<ProductMarketingClaim> tableUpdates = new TableUpdateSets<>();
		List<ProductDescription> newDescriptions = new LinkedList<>();
		List<ProductDescription> descriptionsToUpdate = new LinkedList<>();
		List<LegacyEvent> legacyEvents = new LinkedList<>();
		List<Long> productsToMakeDistinctive = new LinkedList<>();

		for (ExtendedPrimoPickRequest primoPickRequest : primoPickRequests) {

			this.validatePrimoPick(primoPickRequest);

			if (primoPickRequest.isMakeDistinctive()) {
				productsToMakeDistinctive.add(primoPickRequest.getProductId());
			}

			ProductMarketingClaim marketingClaim = requestToMarketingClaim(primoPickRequest, userId);

			// See if there is already one in the table.
			Optional<ProductMarketingClaim> primoPick = this.productMarketingClaimLookup.findById(marketingClaim.getKey());

			if (primoPick.isPresent()) {

				// If one is there, update it.
				ProductMarketingClaim existingRecord = primoPick.get();

				MappingUtils.overlayExisting(existingRecord::getStatus, existingRecord::setStatus, marketingClaim.getStatus());
				MappingUtils.overlayExisting(existingRecord::getStatusChangeReason, existingRecord::setStatusChangeReason, marketingClaim.getStatusChangeReason());

				// NEVER is effectively null, so don't overwrite it.
				if (!Objects.equals(DatabaseConstants.NEVER_AS_DATE, marketingClaim.getPrimoPickEffectiveDate())) {
					MappingUtils.overlayExisting(existingRecord::getPrimoPickEffectiveDate, existingRecord::setPrimoPickEffectiveDate, marketingClaim.getPrimoPickEffectiveDate());
				}

				MappingUtils.overlayExisting(existingRecord::getPrimoPickExpirationDate, existingRecord::setPrimoPickExpirationDate, marketingClaim.getPrimoPickExpirationDate());

				existingRecord.setLastUpdateUserId(userId);

				tableUpdates.getUpdates().add(existingRecord);

				legacyEvents.add(LegacyEventGenerator.generatePMCM(existingRecord.getKey().getProdId(),
						existingRecord.getKey().getMarketingClaimCode().getId(), this.jobName,
						existingRecord.getLastUpdateUserId(), LegacyEventFunction.UPDATE));
			} else {

				// If we're saying to end it, and it's not there, then just continue.
				if (Objects.equals(PrimoPick.Status.ENDED, primoPickRequest.getStatus())) {
					continue;
				}

				// If not, insert it.
				tableUpdates.getInserts().add(marketingClaim);

				legacyEvents.add(LegacyEventGenerator.generatePMCM(marketingClaim.getKey().getProdId(),
						marketingClaim.getKey().getMarketingClaimCode().getId(), this.jobName,
						marketingClaim.getLastUpdateUserId(), LegacyEventFunction.ADD));
			}


			// Add the appropriate type of description.
			for (ProductDescription productDescription : requestToProductDescriptions(primoPickRequest, userId)) {
				if (this.productDescriptionLookup.findById(productDescription.getKey()).isPresent()) {
					descriptionsToUpdate.add(productDescription);
				} else {
					newDescriptions.add(productDescription);
				}
			}

			// Add product-level events.
			legacyEvents.add(LegacyEventGenerator.generatePRM2(primoPickRequest.getProductId(), this.jobName, userId, LegacyEventFunction.UPDATE));
			legacyEvents.add(LegacyEventGenerator.generatePRMM(primoPickRequest.getProductId(), this.jobName, userId, LegacyEventFunction.UPDATE));
		}

		// Do the database updates and save off the generated events.
		this.doUpdates(tableUpdates, newDescriptions, descriptionsToUpdate, legacyEvents, productsToMakeDistinctive, userId);
	}


	private void doUpdates(TableUpdateSets<ProductMarketingClaim> tableUpdates,	List<ProductDescription> newDesciptions,
						   List<ProductDescription> descriptionsToUpdate, List<LegacyEvent> legacyEvents, List<Long> productsToMakeDistinctive, String userId) {

		int rowsUpdated = 0;
		int rowsInserted = 0;

		// Make the ones that need to be distinctive as such.
		if (!productsToMakeDistinctive.isEmpty()) {
			this.productMarketingClaimMaintenance.makeDistinctive(productsToMakeDistinctive, userId);
		}

		// Save the inserts.
		if (!tableUpdates.getInserts().isEmpty()) {

			ProductMarketingClaimUpdater update = new ProductMarketingClaimUpdater(tableUpdates.getInserts(), UpdateType.INSERT);
			int[] rows = this.jdbcTemplate.batchUpdate(ProductMarketingClaimUpdater.INSERT_SQL, update);
			rowsInserted += Arrays.stream(rows).sum();
		}

		// Save the updates.
		if (!tableUpdates.getUpdates().isEmpty()) {

			ProductMarketingClaimUpdater update = new ProductMarketingClaimUpdater(tableUpdates.getUpdates(), UpdateType.UPDATE);
			int[] rows = this.jdbcTemplate.batchUpdate(ProductMarketingClaimUpdater.UPDATE_SQL, update);
			rowsUpdated += Arrays.stream(rows).sum();
		}

		// Save any new descriptions.
		if (!newDesciptions.isEmpty()) {

			ProductDescriptionUpdater productDescriptionUpdater = new ProductDescriptionUpdater(newDesciptions, UpdateType.INSERT);
			int[] rows = this.jdbcTemplate.batchUpdate(ProductDescriptionUpdater.INSERT_SQL, productDescriptionUpdater);
			rowsInserted += Arrays.stream(rows).sum();
		}

		// Update any needed descriptions.
		if (!descriptionsToUpdate.isEmpty()) {

			ProductDescriptionUpdater productDescriptionUpdater = new ProductDescriptionUpdater(descriptionsToUpdate, UpdateType.UPDATE);
			int[] rows = this.jdbcTemplate.batchUpdate(ProductDescriptionUpdater.UPDATE_SQL, productDescriptionUpdater);
			rowsUpdated += Arrays.stream(rows).sum();
		}

		// Save the legacy events.
		if (!legacyEvents.isEmpty()) {
			this.legacyEventProcessor.addAndFlush(legacyEvents);
		}

		this.logger.info(String.format("%,d Primo Pick rows inserted and %,d rows updated.", rowsInserted, rowsUpdated));
	}

	/**
	 * Creates any product descriptions that are needed from the request.
	 *
	 * @param request The ExtendedPrimoPickRequest to convert.
	 * @param userId The ID of the user who submitted the request.
	 * @return A ProductDescription containing the primo pick approved description.
	 */
	private static List<ProductDescription> requestToProductDescriptions(ExtendedPrimoPickRequest request, String userId) {

		List<ProductDescription> productDescriptions = new LinkedList<>();

		if (Objects.nonNull(request.getProposedDescription())) {

			ProductDescriptionKey productDescriptionKey = new ProductDescriptionKey()
					.setProductId(request.getProductId())
					.setDescriptionType(DescriptionType.PRIMO_PICK_PROPOSED)
					.setLanguageType(LanguageType.ENGLISH.getId());

			productDescriptions.add(new ProductDescription().setKey(productDescriptionKey)
					.setLstUpdateUserId(userId)
					.setDescription(request.getProposedDescription()));
		}

		if (Objects.nonNull(request.getDescription())) {

			ProductDescriptionKey productDescriptionKey = new ProductDescriptionKey()
					.setProductId(request.getProductId())
					.setDescriptionType(DescriptionType.PRIMO_PICK)
					.setLanguageType(LanguageType.ENGLISH.getId());

			productDescriptions.add(new ProductDescription().setKey(productDescriptionKey)
					.setLstUpdateUserId(userId)
					.setDescription(request.getDescription()));
		}

		return productDescriptions;
	}

	/**
	 * Converts a ExtendedPrimoPickRequest to a ProductMarketingClaim.
	 *
	 * @param request The ExtendedPrimoPickRequest to convert.
	 * @param userId The ID of the user who submitted the request.
	 * @return The converted ProductMarketingClaim.
	 */
	private static ProductMarketingClaim requestToMarketingClaim(ExtendedPrimoPickRequest request, String userId) {

		ProductMarketingClaimKey key = ProductMarketingClaimKey.of(request.getProductId(), MarketingClaimCode.PRIMO_PICK);

		return ProductMarketingClaim.of(key, userId)
				.setStatusChangeReason(request.getRejectReason())
				.setStatus(requestToStatusClaimCode(request.getStatus()))
				.setPrimoPickExpirationDate(request.getExpirationDate())
				.setPrimoPickEffectiveDate(request.getEffectiveDate())
				.setLastUpdateUserId(userId);
	}

	/**
	 * Converts a PrimoPick.Status to a MarketingClaimStatusCode.
	 *
	 * @param status The PrimoPick.Status to convert.
	 * @return The converted MarketingClaimStatusCode.
	 */
	private static MarketingClaimStatusCode requestToStatusClaimCode(PrimoPick.Status status) {

		if (Objects.isNull(status)) {
			return null;
		}

		switch (status) {
			case ACTIVE:
				return MarketingClaimStatusCode.APPROVED;
			case REQUESTED:
				return MarketingClaimStatusCode.SUBMITTED;
			case ENDED:
				return MarketingClaimStatusCode.APPROVED; // Ending it leaves it in approved, it's just changing the end date.
			default:
				return MarketingClaimStatusCode.REJECTED;
		}
	}


	/**
	 * Validates an ExtendedPrimoPickRequest.
	 *
	 * @param primoPick The ExtendedPrimoPickRequest to validate.
	 * @throws ValidationException Any error validating the Primo-Pick.
	 */
	@Transactional
	public void validatePrimoPick(ExtendedPrimoPickRequest primoPick) {

		List<String> errors = new LinkedList<>();

		if (!this.productLookup.isProductId(primoPick.getProductId())) {
			errors.add(String.format("%d is not a valid product ID.", primoPick.getProductId()));
		}

		if (Objects.isNull(primoPick.getStatus())) {
			errors.add(String.format("No status given for product %d.", primoPick.getProductId()));
		}

		errors.addAll(this.validateRequestPrimoPick(primoPick));
		errors.addAll(this.validateApprovePrimoPick(primoPick));
		errors.addAll(this.validateEndDate(primoPick));
		errors.addAll(this.validateNotEnd(primoPick));

		if (!errors.isEmpty()) {
			throw new ValidationException("Unable to validate Primo-Pick.", errors);
		}
	}

	private List<String> validateNotEnd(ExtendedPrimoPickRequest primoPickRequest) {

		List<String> errors = new LinkedList<>();

		if (Objects.equals(PrimoPick.Status.ENDED, primoPickRequest.getStatus())) {
			return errors;
		}

		if (Objects.isNull(primoPickRequest.getEffectiveDate())) {
			errors.add(String.format("Product %d does not have an effective date.", primoPickRequest.getProductId()));
		} else if (!Objects.equals(DatabaseConstants.NEVER_AS_DATE, primoPickRequest.getEffectiveDate()) &&
				primoPickRequest.getEffectiveDate().isBefore(DateUtils.today())) {
			errors.add(String.format("Product %d's effective date is before today.", primoPickRequest.getProductId()));
		}

		return errors;
	}

	private List<String> validateRequestPrimoPick(ExtendedPrimoPickRequest primoPickRequest) {

		List<String> errors = new LinkedList<>();

		if (!Objects.equals(PrimoPick.Status.REQUESTED, primoPickRequest.getStatus())) {
			return errors;
		}

		// You need a primo-pick description
		if (Objects.isNull(primoPickRequest.getProposedDescription())) {
			errors.add(String.format("Product %d's requires a proposed Primo-Pick description.", primoPickRequest.getProductId()));
		} else if (primoPickRequest.getProposedDescription().length() > this.getMaximumProposedDescriptionSize()) {
			errors.add(String.format("Primo-Pick proposed descriptions must be %,d characters or fewer.", this.getMaximumProposedDescriptionSize()));
		}

		return errors;
	}

	private List<String> validateApprovePrimoPick(ExtendedPrimoPickRequest primoPickRequest) {

		List<String> errors = new LinkedList<>();

		if (!Objects.equals(PrimoPick.Status.ACTIVE, primoPickRequest.getStatus())) {
			return errors;
		}

		// You can only make it primo pick if it's distinctive. The flag means we're going to make it distinctive
		// as part of the process.
		if (!primoPickRequest.isMakeDistinctive()) {
			ProductMarketingClaimKey key = ProductMarketingClaimKey.of(primoPickRequest.getProductId(), MarketingClaimCode.DISTINCTIVE);

			if (this.productMarketingClaimLookup.findById(key).isEmpty()) {
				errors.add(String.format("Product %d is not Distinctive.", primoPickRequest.getProductId()));
			}
		}

		// You need a primo-pick description
		if (Objects.isNull(primoPickRequest.getDescription())) {
			errors.add(String.format("Product %d's requires a Primo-Pick description.", primoPickRequest.getProductId()));
		} else if (primoPickRequest.getDescription().length() > this.getMaximumDescriptionSize()) {
			errors.add(String.format("Primo-Pick descriptions must be %,d characters or fewer.", this.getMaximumDescriptionSize()));
		}

		return errors;
	}

	private List<String> validateEndDate(ExtendedPrimoPickRequest primoPickRequest) {

		List<String> errors = new LinkedList<>();

		if (Objects.isNull(primoPickRequest.getExpirationDate())) {
			errors.add(String.format("Product %d does not have an expiration date.", primoPickRequest.getProductId()));
		} else if (primoPickRequest.getExpirationDate().isAfter(DatabaseConstants.FOREVER_AS_DATE)) {
			errors.add(String.format("Product %d's expiration date is after 12/31/9999.", primoPickRequest.getProductId()));
		}

		return errors;
	}

	private int getMaximumProposedDescriptionSize() {

		if (this.maximumProposedDescriptionSize == 0) {
			this.maximumProposedDescriptionSize = this.jdbcTemplate.queryForObject(DESCRIPTION_LENGTH_SQL,
					JdbcUtils.argsAsArray(DescriptionType.PRIMO_PICK_PROPOSED.getId()),
					Integer.class);
		}
		return this.maximumProposedDescriptionSize;
	}

	private int getMaximumDescriptionSize() {

		if (this.maximumDescriptionSize == 0) {
			this.maximumDescriptionSize = this.jdbcTemplate.queryForObject(DESCRIPTION_LENGTH_SQL,
					JdbcUtils.argsAsArray(DescriptionType.PRIMO_PICK.getId()),
					Integer.class);
		}
		return this.maximumDescriptionSize;

	}
}
