package com.heb.pm.core.service.maintenance.ecommerce;

import com.heb.pm.core.event.alerts.AlertStagingClient;
import com.heb.pm.core.exception.ValidationException;
import com.heb.pm.core.service.maintenance.common.MaintenanceConfig;
import com.heb.pm.dao.core.DatabaseConstants;
import com.heb.pm.dao.core.LegacyEventGenerator;
import com.heb.pm.dao.core.entity.*;
import com.heb.pm.dao.core.entity.codes.LegacyEventFunction;
import com.heb.pm.dao.core.entity.codes.StagedAlertType;
import com.heb.pm.dao.core.preparedstatementsetters.MasterDataExtendedAttributeUpdater;
import com.heb.pm.dao.core.preparedstatementsetters.UpdateType;
import com.heb.pm.dao.core.quicklookup.ProductLookup;
import com.heb.pm.dao.core.quicklookup.UpcLookup;
import com.heb.pm.dao.core.rowmappers.*;
import com.heb.pm.util.JdbcUtils;
import com.heb.pm.util.ValidatorUtils;
import com.heb.pm.util.soap.CheckedSoapException;
import org.apache.commons.lang3.StringUtils;
import org.springframework.transaction.annotation.Transactional;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static com.heb.pm.util.ValidatorUtils.notNullOrIllegalArgument;

/**
 * Maintains MST_DTA_EXTN_ATTR.
 *
 * @author d116773
 * @since 1.24.0
 */
// This is a big class, but it is all related to maintaining one thing that is
// complex. I feel pulling the cde out will make it harder rather than easier
// to maintain.
@SuppressWarnings("PMD.GodClass")
public class MasterDataExtendedAttributeMaintenance {

	private static final long MINIMUM_SEQUENCE_NUMBER = 0L;

	private static final String ATTRIBUTE_LOOKUP_SQL = AttributeRowMapper.SELECT_SQL + " WHERE ATTR_ID = ?";

	private static final String ATTRIBUTE_CODE_LOOKUP_SQL = AttributeCodeRowMapper.SELECT_SQL + " WHERE ATTR_CD_ID = ?";

	private static final String SINGLE_ROW_LOOKUP_SQL = MasterDataExtendedAttributeRowMapper.SELECT_SQL +
			" WHERE ATTR_ID = ? AND KEY_ID = ? AND ITM_PROD_KEY_CD = ? AND DTA_SRC_SYS = ?";

	private static final String MAX_SEQUENCE_NUMBER_SQL = "SELECT MAX(SEQ_NBR) MSN " +
			"FROM EMD.MST_DTA_EXTN_ATTR " +
			"WHERE  ATTR_ID = ? AND KEY_ID = ? AND ITM_PROD_KEY_CD = ? AND DTA_SRC_SYS = ?";

	private static final MasterDataExtendedAttributeRowMapper ROW_MAPPER = new MasterDataExtendedAttributeRowMapper();
	private static final SingleRowResultSetExtractor<MasterDataExtendedAttribute> MDEA_SINGLE_ROW_RESULT_SET_EXTRACTOR = new SingleRowResultSetExtractor<>(ROW_MAPPER);

	private static final AttributeRowMapper ATTRIBUTE_ROW_MAPPER = new AttributeRowMapper();
	private static final SingleRowResultSetExtractor<Attribute> ATTRIBUTE_SINGLE_ROW_RESULT_SET_EXTRACTOR = new SingleRowResultSetExtractor<>(ATTRIBUTE_ROW_MAPPER);

	private static final AttributeCodeRowMapper ATTRIBUTE_CODE_ROW_MAPPER = new AttributeCodeRowMapper();
	private static final SingleRowResultSetExtractor<AttributeCode> ATTRIBUTE_CODE_SINGLE_ROW_RESULT_SET_EXTRACTOR = new SingleRowResultSetExtractor<>(ATTRIBUTE_CODE_ROW_MAPPER);

	private static final LongRowMapper SEQUENCE_NUMBER_ROW_MAPPER = new LongRowMapper("MSN");

	private final transient MaintenanceConfig config;
	private final transient Map<Long, Attribute> attributeLookupMap = new ConcurrentHashMap<>();
	private final transient Map<Long, AttributeCode> attributeCodeLookupMap = new ConcurrentHashMap<>();
	private final transient UpcLookup upcLookup;
	private final transient ProductLookup productLookup;

	/**
	 * Constructs a new MasterDataExtendedAttributeMaintenance.
	 *
	 * @param config The class's config.
	 */
	public MasterDataExtendedAttributeMaintenance(MaintenanceConfig config) {

		config.requireBasics();
		this.config = config;
		this.upcLookup = new UpcLookup(this.config.getJdbcTemplate());
		this.productLookup = new ProductLookup(this.config.getJdbcTemplate());
	}

	/**
	 * Performs a validation on a MasterDataExtendedAttribute.
	 *
	 * @param masterDataExtendedAttribute The MasterDataExtendedAttribute to validate.
	 * @throws ValidationException Any error in validation.
	 */
	public void validate(MasterDataExtendedAttribute masterDataExtendedAttribute) {

		MasterDataExtendedAttributeKey key = masterDataExtendedAttribute.getKey();

		List<String> errors = this.validateKey(key);

		// At least one of the values has to be set.
		if (Objects.isNull(masterDataExtendedAttribute.getAttributeCodeId()) && Objects.isNull(masterDataExtendedAttribute.getTextValue()) &&
				Objects.isNull(masterDataExtendedAttribute.getAttributeValueNumber()) && Objects.isNull(masterDataExtendedAttribute.getAttributeValueTime()) &&
				Objects.isNull(masterDataExtendedAttribute.getAttributeValueDate())) {
			errors.add("There is no value set for this extended attribute.");
		}

		// Make sure the attribute is valid.
		if (Objects.nonNull(key.getAttributeId())) {
			validateAttributeId(key.getAttributeId()).ifPresent(errors::add);
			validateValueMatchesAttribute(masterDataExtendedAttribute).ifPresent(errors::add);
		}

		// If it's a code, make sure that's valid.
		if (Objects.nonNull(masterDataExtendedAttribute.getAttributeCodeId())) {
			validateAttributeCodeId(masterDataExtendedAttribute.getAttributeCodeId()).ifPresent(errors::add);
		}

		if (!errors.isEmpty()) {
			throw new ValidationException("Unable to validate extended attribute.", errors);
		}
	}

	/**
	 * Adds a collection of records to MST_DTA_EXTN_ATTR.
	 *
	 * @param masterDataExtendedAttributes The collection of records to add.
	 * @param userId The ID of the user adding the records.
	 * @return The number of rows changed. This may include deletes.
	 */
	@Transactional
	public int addMasterDataExtendedAttributes(List<MasterDataExtendedAttribute> masterDataExtendedAttributes, String userId) {

		this.config.requireForProcessing();

		notNullOrIllegalArgument(userId, "User ID is required.");

		Set<MasterDataExtendedAttribute> toAdd = new HashSet<>();
		Set<MasterDataExtendedAttribute> toUpdate = new HashSet<>();

		List<LegacyEvent> legacyEvents = new LinkedList<>();

		this.applySequences(masterDataExtendedAttributes);

		for (MasterDataExtendedAttribute masterDataExtendedAttribute : masterDataExtendedAttributes) {

			// Validation is done in applySequences.

			// If it's a code, update the ATTR_VAL_TXT field from the ATTR_CD table.
			this.overlayAttributeCodeText(masterDataExtendedAttribute);

			MasterDataExtendedAttributeKey key = masterDataExtendedAttribute.getKey();

			// If it's not in the map, then it would have failed validation.
			boolean isMultiValuedAttribute = this.attributeLookupMap.get(key.getAttributeId()).getMultipleValueSwitch();

			// If it's a single-value attribute.
			if (!isMultiValuedAttribute) {

				// See if one is already in the DB.
				Optional<MasterDataExtendedAttribute> existingAttribute = this.findByKeyExceptSequence(key);

				// Update it if it is.
				if (existingAttribute.isPresent()) {
					MasterDataExtendedAttribute unwrapped = existingAttribute.get();

					unwrapped.setTextValue(masterDataExtendedAttribute.getTextValue())
							.setAttributeCodeId(masterDataExtendedAttribute.getAttributeCodeId())
							.setAttributeValueNumber(masterDataExtendedAttribute.getAttributeValueNumber())
							.setAttributeValueDate(masterDataExtendedAttribute.getAttributeValueDate())
							.setAttributeValueTime(masterDataExtendedAttribute.getAttributeValueTime())
							.setPrimarySourceSystemId(masterDataExtendedAttribute.getPrimarySourceSystemId())
							.setLastUpdateUserId(userId);

					toUpdate.add(unwrapped);
				} else {

					// If not, then we'll insert a new one.
					masterDataExtendedAttribute.setCreateUserId(userId).setLastUpdateUserId(userId);

					toAdd.add(masterDataExtendedAttribute);
				}

			}  else {

				// Anything not multivalued is just an insert.
				masterDataExtendedAttribute.setCreateUserId(userId).setLastUpdateUserId(userId);

				toAdd.add(masterDataExtendedAttribute);
			}
		}

		int rowsUpdated = this.handleUpdate(toUpdate);
		int rowsAdded = this.handleAdd(toAdd);
		this.config.getLogger().info(String.format("%,d extended attribute rows inserted and %,d updated.", rowsAdded, rowsUpdated));

		// Add application alerts if the objects are all set. Since this was added later, I'm
		// being defensive about issuing them in case the alert staging client is not set.
		if (Objects.nonNull(this.config.getAlertStagingClient())) {

			this.getAlertRequests(toAdd, toUpdate, userId).forEach(this::issueAlert);

		} else {
			this.config.getLogger().warn("Warning, Alert Staging Client not set.");
		}

		// Save off the events.
		toAdd.stream().map(m -> LegacyEventGenerator.generateMDEA(m.getKey().getAttributeId(), m.getKey().getKeyId(),
					m.getKey().getKeyType(), m.getKey().getSourceSystemId(), this.config.getProgramName(), userId, LegacyEventFunction.ADD))
				.forEach(legacyEvents::add);

		toUpdate.stream().map(m -> LegacyEventGenerator.generateMDEA(m.getKey().getAttributeId(), m.getKey().getKeyId(),
				m.getKey().getKeyType(), m.getKey().getSourceSystemId(), this.config.getProgramName(), userId, LegacyEventFunction.UPDATE))
				.forEach(legacyEvents::add);

		this.config.getLegacyEventProcessor().addAndFlush(legacyEvents);

		// Return the number of rows modified.
		return rowsAdded + rowsUpdated;
	}

	/**
	 * Calls the AlertStagingClient to issue an AlertRequest.
	 *
	 * @param alertRequest The AlertRequest to issue.
	 */
	private void issueAlert(AlertStagingClient.AlertRequest alertRequest) {

		try {
			this.config.getAlertStagingClient().issueAlert(alertRequest);
		} catch (CheckedSoapException e) {
			this.config.getLogger().error(String.format("Error while creating alerts: %s.", e.getLocalizedMessage()));
		}

	}

	/**
	 * Takes a list of records being inserted and updated and maps them to AlertRequests.
	 *
	 * @param inserts The list of records being inserted.
	 * @param updates The list of records being updated.
	 * @param userId The ID of the user who kiced off the request.
	 * @return A list of AlertRequests. The list may be empty, but will not be null.
	 */
	private List<AlertStagingClient.AlertRequest> getAlertRequests (Set<MasterDataExtendedAttribute> inserts, Set<MasterDataExtendedAttribute> updates, String userId) {

		Map<Long, Set<Long>> alertValues = new ConcurrentHashMap<>(); // Will have product ID as key and a list of attribute IDs as the value.

		this.addAttributesToMap(inserts, alertValues);
		this.addAttributesToMap(updates, alertValues);

		List<AlertStagingClient.AlertRequest> alertRequests = new LinkedList<>();

		for (Map.Entry<Long, Set<Long>> alertValue : alertValues.entrySet()) {

			AlertStagingClient.AlertRequest alertRequest = new AlertStagingClient.AlertRequest()
					.setAlertType(StagedAlertType.PRODUCT_UPDATE)
					.setAlertKey(alertValue.getKey())
					.setCreateUser(userId)
					.setAssignedUser(this.productLookup.getEcommerceBusinessManager(alertValue.getKey()).orElse(StringUtils.SPACE));

			alertRequest.getAlerts().addAll(alertValue.getValue());
			alertRequests.add(alertRequest);
		}

		return alertRequests;
	}

	/**
	 * Given a set of MasterDataExtendedAttributes and a map with product IDs and attribute IDs, goes through
	 * the set, tries to look up the product ID, sees if an attribute type changed that we want to issue
	 * an alert for, and adds that product and attribute ID to the map.
	 *
	 * @param records The set of MasterDataExtendedAttributes to look at.
	 * @param alertValues The Map to add the product ID and attribtue IDs to.
	 */
	private void addAttributesToMap(Set<MasterDataExtendedAttribute> records, Map<Long, Set<Long>> alertValues) {

		for (MasterDataExtendedAttribute record : records) {

			if (changedAlertType(record)) {

				Optional<Long> productId = this.getProductForAttributeValue(record);

				if (productId.isPresent()) {

					if (alertValues.containsKey(productId.get())) {
						alertValues.get(productId.get()).add(record.getKey().getAttributeId());
					} else {

						Set<Long> alertIds = new HashSet<>();
						alertIds.add(record.getKey().getAttributeId());
						alertValues.put(productId.get(), alertIds);
					}
				}
			}
		}
	}

	/**
	 * Looks up a product ID for a MasterDataExtendedAttribute.
	 *
	 * @param attribute The MasterDataExtendedAttribute to lookup the product ID for.
	 * @return The product ID ties to the MasterDataExtendedAttribute or empty if it can't be found.
	 */
	private Optional<Long> getProductForAttributeValue(MasterDataExtendedAttribute attribute) {

		if (Objects.equals(attribute.getKey().getKeyType(), MasterDataExtendedAttributeKey.KEY_TYPE_PRODUCT)) {
			return Optional.of(attribute.getKey().getKeyId());
		}

		if (Objects.equals(attribute.getKey().getKeyType(), MasterDataExtendedAttributeKey.KEY_TYPE_UPC)) {
			return this.upcLookup.getProductId(attribute.getKey().getKeyId());
		}

		return Optional.empty();
	}

	/**
	 * Returns whether or not a MasterDataExtendedAttribute contains a value in one of the types we issue
	 * alerts for.
	 *
	 * @param attribute The MasterDataExtendedAttribute to check.
	 * @return True if we issue an alert for that type and false otherwise.
	 */
	private static boolean changedAlertType(MasterDataExtendedAttribute attribute) {
		return Objects.nonNull(attribute.getTextValue()) || Objects.nonNull(attribute.getAttributeValueNumber()) ||
				Objects.nonNull(attribute.getAttributeCodeId());
	}
	/**
	 * Figures out what should be the sequence number for all the records in the list. If it is mult-valued, it will
	 * take the max existing one and keep incrementing. If it is a single-valued attribute, it sets it to 0.
	 *
	 * For the purposes of performance, the validation was moved into this method.
	 *
	 * @param masterDataExtendedAttributes The list of MasterDataExtendedAttributes to apply the sequences to.
	 */
	private void applySequences(List<MasterDataExtendedAttribute> masterDataExtendedAttributes) {

		Map<MasterDataExtendedAttributeKey, Long> sequenceNumberMap = new ConcurrentHashMap<>();

		for (MasterDataExtendedAttribute masterDataExtendedAttribute : masterDataExtendedAttributes) {

			MasterDataExtendedAttributeKey key = masterDataExtendedAttribute.getKey();

			// Moved here because we need the attributes in the map below and this ensures they are there. I didn't really
			// want it here as it's not conceptually right, but I didn't want to go through the list 3 times.
			this.validate(masterDataExtendedAttribute);

			// If it's not in the map, then it would have failed validation.
			boolean isMultiValuedAttribute = this.attributeLookupMap.get(key.getAttributeId()).getMultipleValueSwitch();

			// If it's multi-valued, figure out the new sequence number.
			long sequenceNumber = MINIMUM_SEQUENCE_NUMBER;

			if (isMultiValuedAttribute) {

				// See if it's already in the map.
				Long lastSequence = sequenceNumberMap.get(masterDataExtendedAttribute.getKey());
				if (Objects.nonNull(lastSequence)) {

					// If so, use that plus 1.
					sequenceNumber = lastSequence + 1;
				} else {

					// If not, get the max from the DB and add one.
					Long maxSequenceNumber = this.config.getJdbcTemplate().query(MAX_SEQUENCE_NUMBER_SQL,
							JdbcUtils.argsAsArray(key.getAttributeId(), key.getKeyId(), key.getKeyType(), key.getSourceSystemId()),
							new SingleRowResultSetExtractor<>(SEQUENCE_NUMBER_ROW_MAPPER));

					sequenceNumber = Objects.isNull(maxSequenceNumber) ? MINIMUM_SEQUENCE_NUMBER : maxSequenceNumber + 1;
				}

				// Save the next sequence to the hash table.
				sequenceNumberMap.put(copyKey(masterDataExtendedAttribute.getKey()), sequenceNumber);

				masterDataExtendedAttribute.getKey().setSequenceNumber(sequenceNumber);
			}

			masterDataExtendedAttribute.getKey().setSequenceNumber(sequenceNumber);
		}
	}

	private static MasterDataExtendedAttributeKey copyKey(MasterDataExtendedAttributeKey toCopy) {

			return new MasterDataExtendedAttributeKey().setKeyId(toCopy.getKeyId())
					.setKeyType(toCopy.getKeyType())
					.setAttributeId(toCopy.getAttributeId())
					.setSequenceNumber(toCopy.getSequenceNumber())
					.setSourceSystemId(toCopy.getSourceSystemId());
	}

	/**
	 * Removes a collection of rows from MST_DTA_EXTN_ATTR.
	 *
	 * @param masterDataExtendedAttributes The collection of rows to remove.
	 * @param userId The ID of the user who requested removing the rows.
	 * @return The number of records deleted.
	 */
	@Transactional
	public int removeMasterDataExtendedAttributes(List<MasterDataExtendedAttribute> masterDataExtendedAttributes, String userId) {

		this.config.requireForProcessing();

		Set<MasterDataExtendedAttribute> toRemove = new HashSet<>();
		List<LegacyEvent> legacyEvents = new LinkedList<>();

		for (MasterDataExtendedAttribute masterDataExtendedAttribute : masterDataExtendedAttributes) {

			MasterDataExtendedAttributeKey key = masterDataExtendedAttribute.getKey();

			List<String> errors = this.validateKey(key);
			// This has an extra validation of requiring a sequence number.
			ValidatorUtils.validateFieldExists(key::getSequenceNumber, "Sequence number cannot be empty.").ifPresent(errors::add);

			if (!errors.isEmpty()) {
				throw new ValidationException("Unable to validate extended attribute record.", errors);
			}

			Optional<MasterDataExtendedAttribute> existingAttribute = this.findByKeyExceptSequence(key);
			if (existingAttribute.isPresent()) {
				toRemove.add(existingAttribute.get());
				legacyEvents.add(LegacyEventGenerator.generateMDEA(key.getAttributeId(), key.getKeyId(), key.getKeyType(), key.getSourceSystemId(),
						this.config.getProgramName(), userId, LegacyEventFunction.DELETE));
			}
		}

		int rowsDeleted = this.handleDelete(toRemove);
		this.config.getLogger().info(String.format("%,d rows removed.", rowsDeleted));

		this.config.getLegacyEventProcessor().addAndFlush(legacyEvents);

		return rowsDeleted;
	}

	/**
	 * Performs the inserts into the table.
	 *
	 * @param toAdd The collection of rows to insert.
	 * @return The number of rows affected.
	 */
	@Transactional
	protected int handleAdd(Set<MasterDataExtendedAttribute> toAdd) {

		MasterDataExtendedAttributeUpdater updater = new MasterDataExtendedAttributeUpdater(toAdd, UpdateType.INSERT);

		int[] rowsUpdate = this.config.getJdbcTemplate().batchUpdate(MasterDataExtendedAttributeUpdater.INSERT_SQL, updater);
		return Arrays.stream(rowsUpdate).sum();
	}

	/**
	 * Performs the deletes from the table.
	 *
	 * @param toDelete The collection of rows to delete.
	 * @return The number of rows affected.
	 */
	@Transactional
	protected int handleDelete(Set<MasterDataExtendedAttribute> toDelete) {

		MasterDataExtendedAttributeUpdater updater = new MasterDataExtendedAttributeUpdater(toDelete, UpdateType.DELETE);

		int[] rowsUpdate = this.config.getJdbcTemplate().batchUpdate(MasterDataExtendedAttributeUpdater.DELETE_SQL, updater);
		return Arrays.stream(rowsUpdate).sum();
	}

	/**
	 * Performs updates to the table.
	 *
	 * @param toUpdate The collection of rows to update.
	 * @return The number of rows affected.
	 */
	@Transactional
	protected int handleUpdate(Set<MasterDataExtendedAttribute> toUpdate) {

		MasterDataExtendedAttributeUpdater updater = new MasterDataExtendedAttributeUpdater(toUpdate, UpdateType.UPDATE);

		int[] rowsUpdate = this.config.getJdbcTemplate().batchUpdate(MasterDataExtendedAttributeUpdater.UPDATE_SQL, updater);
		return Arrays.stream(rowsUpdate).sum();
	}

	private Optional<MasterDataExtendedAttribute> findByKeyExceptSequence(MasterDataExtendedAttributeKey key) {

		return Optional.ofNullable(this.config.getJdbcTemplate().query(SINGLE_ROW_LOOKUP_SQL,
				JdbcUtils.argsAsArray(key.getAttributeId(), key.getKeyId(), key.getKeyType(), key.getSourceSystemId()),
				MDEA_SINGLE_ROW_RESULT_SET_EXTRACTOR));
	}

	private Optional<String> validateAttributeId(Long attributeId) {

		// See if we've already looked up this attribute.
		Attribute attribute = this.attributeLookupMap.get(attributeId);

		// If not, then find it and add it to the map.
		if (Objects.isNull(attribute)) {

			attribute = this.config.getJdbcTemplate().query(ATTRIBUTE_LOOKUP_SQL, JdbcUtils.argsAsArray(attributeId), ATTRIBUTE_SINGLE_ROW_RESULT_SET_EXTRACTOR);

			if (Objects.nonNull(attribute)) {
				this.attributeLookupMap.put(attributeId, attribute);
			}
		}

		if (Objects.isNull(attribute)) {
			return Optional.of(String.format("Attribute %d does not exist.", attributeId));
		} else {
			return Optional.empty();
		}
	}

	private Optional<String> validateAttributeCodeId(Long attributeCodeId) {

		// The code may not be set.
		if (Objects.isNull(attributeCodeId)) {
			return Optional.empty();
		}

		// See if we've already looked up this code.
		AttributeCode attributeCode = this.attributeCodeLookupMap.get(attributeCodeId);

		// If not, then find it and add it to the map.
		if (Objects.isNull(attributeCode)) {

			attributeCode = this.config.getJdbcTemplate().query(ATTRIBUTE_CODE_LOOKUP_SQL, JdbcUtils.argsAsArray(attributeCodeId), ATTRIBUTE_CODE_SINGLE_ROW_RESULT_SET_EXTRACTOR);
			if (Objects.nonNull(attributeCode)) {

				this.attributeCodeLookupMap.put(attributeCodeId, attributeCode);
			}
		}

		if (Objects.isNull(attributeCode)) {
			return Optional.of(String.format("Attribute code %d does not exist.", attributeCodeId));
		} else {
			return Optional.empty();
		}
	}

	/**
	 * Validates that the value provided for the record matches the type of the attribute.
	 *
	 * @param masterDataExtendedAttribute The MasterDataExtendedAttribute to validate.
	 * @return An error message if there is one or empty.
	 */
	private Optional<String> validateValueMatchesAttribute(MasterDataExtendedAttribute masterDataExtendedAttribute) {

		Attribute attribute = this.attributeLookupMap.get(masterDataExtendedAttribute.getKey().getAttributeId());
		if (Objects.isNull(attribute)) {
			return Optional.empty(); // this will be caught by the attribute check.
		}

		switch (attribute.getAttributeDomainCode()) {
			case DATE:
				if (Objects.isNull(masterDataExtendedAttribute.getAttributeValueDate())) {
					return Optional.of("This attribute is of type date, but no date was provided.");
				}
				break;
			case DECIMAL:
			case INTEGER:
				if (Objects.isNull(masterDataExtendedAttribute.getAttributeValueNumber())) {
					return Optional.of("This attribute is of a numeric type, but no number was provided.");
				}
				break;
			case TIMESTAMP:
				if (Objects.isNull(masterDataExtendedAttribute.getAttributeValueTime())) {
					return Optional.of("This attribute if of type timestamp, but no timestamp was provided.");
				}
				break;
			case BOOLEAN:
				if (Objects.isNull(masterDataExtendedAttribute.getTextValue())) {
					return Optional.of("This attribute if of type Boolean, but no value was provided.");
				}
				if (!Objects.equals(DatabaseConstants.YES, masterDataExtendedAttribute.getTextValue()) &&
						!Objects.equals(DatabaseConstants.NO, masterDataExtendedAttribute.getTextValue())) {
					return Optional.of("This attribute if of type Boolean, but TRUE or FALSE was not provided.");
				}
				break;
			case STRING:
				return validateTextAttribute(attribute, masterDataExtendedAttribute);
			case IMAGE:
				return Optional.of("Images are not supported.");
		}

		return Optional.empty();
	}

	/**
	 * Validates a text attribute.
	 *
	 * @param attribute The Attribute being set.
	 * @param masterDataExtendedAttribute The value being set.
	 * @return An error message if validation fails or empty.
	 */
	private Optional<String> validateTextAttribute(Attribute attribute, MasterDataExtendedAttribute masterDataExtendedAttribute) {

		// If it's a code, then check attributeCodeId.
		if (Objects.equals(DatabaseConstants.YES, attribute.getAttributeValueListSwitch())) {
			return Objects.isNull(masterDataExtendedAttribute.getAttributeCodeId()) ?
					Optional.of("This attribute is of type code, but no code was provided.") : Optional.empty();
		}

		// If not, check textValue.
		return Objects.isNull(masterDataExtendedAttribute.getTextValue()) ?
				Optional.of("This attribute is of type string, but no string was provided.") : Optional.empty();
	}

	/**
	 * For codes, the legacy code copied the text value from the ATTR_CD table into the ATTR_VAL_TXT field in
	 * MST_DTA_EXTN_ATTR. If the attribute is a code, this will look-up the value in ATTR_CD in ATTR and update
	 * the text field of the MasterDataExtendedAttribute passed in with the ATTR_VAL_CD value from ATTR_CD.
	 *
	 * @param masterDataExtendedAttribute The MasterDataExtendedAttribute to update if the type of the attribute
	 *                                    is a code.
	 */
	private void overlayAttributeCodeText(MasterDataExtendedAttribute masterDataExtendedAttribute) {

		Attribute attribute = this.attributeLookupMap.get(masterDataExtendedAttribute.getKey().getAttributeId());

		if (!Objects.equals(attribute.getAttributeValueListSwitch(), DatabaseConstants.YES)) {
			return;
		}

		AttributeCode attributeCode = this.attributeCodeLookupMap.get(masterDataExtendedAttribute.getAttributeCodeId());
		masterDataExtendedAttribute.setTextValue(attributeCode.getAttributeValueCode());
	}

	private List<String> validateKey(MasterDataExtendedAttributeKey key) {

		List<String> errors = new LinkedList<>();

		ValidatorUtils.validateFieldExists(key::getAttributeId, "Attribute ID cannot be empty.").ifPresent(errors::add);
		ValidatorUtils.validateFieldExists(key::getKeyId, "Key ID cannot be empty.").ifPresent(errors::add);
		ValidatorUtils.validateFieldExists(key::getKeyType, "Key type cannot be empty.").ifPresent(errors::add);
		ValidatorUtils.validateFieldExists(key::getSourceSystemId, "Source system ID cannot be empty.").ifPresent(errors::add);

		return errors;
	}
}
