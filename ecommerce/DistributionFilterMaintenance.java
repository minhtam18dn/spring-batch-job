package com.heb.pm.core.service.maintenance.ecommerce;

import com.heb.pm.core.event.LegacyEventProcessor;
import com.heb.pm.core.maintenance.ProductMaintenanceRequest;
import com.heb.pm.core.service.maintenance.common.MaintenanceConfig;
import com.heb.pm.dao.core.LegacyEventGenerator;
import com.heb.pm.dao.core.converters.StringToDistributionKeyTypeCodeConverter;
import com.heb.pm.dao.core.entity.DistributionFilter;
import com.heb.pm.dao.core.entity.DistributionFilterKey;
import com.heb.pm.dao.core.entity.LegacyEvent;
import com.heb.pm.dao.core.entity.SourceSystem;
import com.heb.pm.dao.core.entity.codes.DistributionKeyTypeCode;
import com.heb.pm.dao.core.entity.codes.LegacyEventFunction;
import com.heb.pm.dao.core.preparedstatementsetters.DistributionFilterUpdater;
import com.heb.pm.dao.core.preparedstatementsetters.UpdateType;
import com.heb.pm.util.JdbcUtils;
import org.slf4j.Logger;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;

/**
 * Service to handle maintenance of distribution filters.
 *
 * @author d116773
 * @since 1.24.0
 */
// Since this can be called from batch jobs, I want to allow for different loggers. To do this, it is
// passed in as a parameter to the constructor, so the logger can't be static or final.
@SuppressWarnings("PMD.LoggerIsNotStaticFinal")
public class DistributionFilterMaintenance {


	private static final String AUDIT_INSERT_SQL = "INSERT INTO EMD.DSTRB_FLTR_AUD " +
			"(TRG_SYSTEM_ID, DSTRB_KEY_TYP_CD, ATTR_VAL_NBR, AUD_REC_CRE8_TS, ACT_CD, LST_UPDT_UID ) " +
			"SELECT TRG_SYSTEM_ID, DSTRB_KEY_TYP_CD, ATTR_VAL_NBR, SYSDATE, 'PURGE', ? " +
			"FROM EMD.DSTRB_FLTR WHERE TRG_SYSTEM_ID = ? AND DSTRB_KEY_TYP_CD = ? AND ATTR_VAL_NBR = ?";

	private static final StringToDistributionKeyTypeCodeConverter TYPE_CODE_CONVERTER =
			new StringToDistributionKeyTypeCodeConverter();

	private final transient JdbcTemplate jdbcTemplate;
	private final transient String jobName;
	private final transient Logger logger;
	private final transient LegacyEventProcessor legacyEventProcessor;
	private final transient DistributionFilterLookup distributionFilterLookup;

	/**
	 * Constructs a new DistributionFilterMaintenance.
	 *
	 * @param maintenanceConfig The configuration for this class.
	 */
	public DistributionFilterMaintenance(MaintenanceConfig maintenanceConfig) {

		this.jdbcTemplate = maintenanceConfig.getJdbcTemplate();
		this.jobName = maintenanceConfig.getProgramName();
		this.logger = maintenanceConfig.getLogger();
		this.legacyEventProcessor = maintenanceConfig.getLegacyEventProcessor();
		this.distributionFilterLookup = new DistributionFilterLookup(this.jdbcTemplate);
	}

	/**
	 * Handles turning on or off third party sellable for a product.
	 *
	 * @param productId The ID of the product to make or remove third-party-sellable from.
	 * @param productMaintenanceRequest The user's maintenance request.
	 */
	@Transactional
	public void handleThirdPartySellable(long productId, ProductMaintenanceRequest productMaintenanceRequest) {

		if (Objects.equals(Boolean.TRUE, productMaintenanceRequest.getThirdPartySellable())) {

			this.logger.info(String.format("Making product %d third-party-sellable.", productId));

			this.enableDistributionFilter(
					List.of(distributionFilterOf(productId, productMaintenanceRequest.getUserId())));
		}

		if (Objects.equals(Boolean.FALSE, productMaintenanceRequest.getThirdPartySellable())) {

			this.logger.info(String.format("Making product %d not third-party-sellable.", productId));

			this.removeDistributionFilter(
					List.of(distributionFilterOf(productId, productMaintenanceRequest.getUserId())));
		}
	}

	private static DistributionFilter distributionFilterOf(long productId, String userId) {

		DistributionFilterKey key = DistributionFilterKey.of(SourceSystem.INSTACART_SOURCE_SYSTEM,
				DistributionKeyTypeCode.PRODUCT, BigDecimal.valueOf(productId));

		return DistributionFilter.of(key, userId);
	}

	/**
	 * Inserts a list of DistributionFilters.
	 * @param distributionFilters The list DistributionFilters to insert.
	 */
	@Transactional
	public void enableDistributionFilter(List<? extends DistributionFilter> distributionFilters) {

		List<DistributionFilter> toAdd = new LinkedList<>();
		List<LegacyEvent> legacyEvents = new LinkedList<>();

		for (DistributionFilter distributionFilter : distributionFilters) {

			if (this.distributionFilterLookup.findById(distributionFilter.getKey()).isEmpty()) {
				toAdd.add(distributionFilter);
				legacyEvents.add(LegacyEventGenerator.generateDFMM(distributionFilter.getKey(),
						this.jobName, distributionFilter.getLastUpdateUser(), LegacyEventFunction.ADD));
			}
		}

		if (toAdd.isEmpty()) {
			this.logger.debug("No records to add.");
			return;
		}

		DistributionFilterUpdater updater = new DistributionFilterUpdater(toAdd, UpdateType.INSERT);
		int[] rowsAdded = this.jdbcTemplate.batchUpdate(DistributionFilterUpdater.INSERT_SQL, updater);

		this.legacyEventProcessor.addAndFlush(legacyEvents);

		this.logger.info(String.format("%,d distribution filters added.", Arrays.stream(rowsAdded).sum()));
	}

	/**
	 * Removes a list of DistributionFilters.
	 *
	 * @param distributionFilters The list of DistributionFilters to remove.
	 */
	@Transactional
	public void removeDistributionFilter(List<? extends DistributionFilter> distributionFilters) {

		List<DistributionFilter> toRemove = new LinkedList<>();
		List<LegacyEvent> legacyEvents = new LinkedList<>();

		for (DistributionFilter distributionFilter : distributionFilters) {

			if (this.distributionFilterLookup.findById(distributionFilter.getKey()).isPresent()) {

				// There is no trigger on the delete from this table, so we need to add it to the audits
				// by hand.
				this.jdbcTemplate.update(AUDIT_INSERT_SQL,
						JdbcUtils.argsAsArray(
								distributionFilter.getLastUpdateUser(),
								distributionFilter.getKey().getTargetSourceSystemId(),
								TYPE_CODE_CONVERTER.convertToDatabaseColumn(distributionFilter.getKey().getDistributionKeyTypeCode()),
								distributionFilter.getKey().getAttributeValueNumber()));

				toRemove.add(distributionFilter);

				legacyEvents.add(LegacyEventGenerator.generateDFMM(distributionFilter.getKey(),
						this.jobName, distributionFilter.getLastUpdateUser(), LegacyEventFunction.DELETE));
			}
		}


		if (toRemove.isEmpty()) {
			this.logger.debug("No records to add.");
			return;
		}

		DistributionFilterUpdater updater = new DistributionFilterUpdater(toRemove, UpdateType.DELETE);
		int[] rowsDeleted = this.jdbcTemplate.batchUpdate(DistributionFilterUpdater.DELETE_SQL, updater);

		this.legacyEventProcessor.addAndFlush(legacyEvents);

		this.logger.info(String.format("%,d distribution filters deleted.", Arrays.stream(rowsDeleted).sum()));
	}
}
