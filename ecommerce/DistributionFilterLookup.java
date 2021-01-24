package com.heb.pm.core.service.maintenance.ecommerce;

import com.heb.pm.dao.core.converters.StringToDistributionKeyTypeCodeConverter;
import com.heb.pm.dao.core.entity.DistributionFilter;
import com.heb.pm.dao.core.entity.DistributionFilterKey;
import com.heb.pm.dao.core.rowmappers.DistributionFilterRowMapper;
import com.heb.pm.dao.core.rowmappers.SingleRowResultSetExtractor;
import com.heb.pm.util.JdbcUtils;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.Optional;

/**
 * Class to do lookups of DistributionFilter records.
 *
 * @author d116773
 * @since 1.24.0
 */
public class DistributionFilterLookup {

	private static final DistributionFilterRowMapper ROW_MAPPER = new DistributionFilterRowMapper();
	private static final SingleRowResultSetExtractor<DistributionFilter> SINGLE_ROW_RESULT_SET_EXTRACTOR =
			new SingleRowResultSetExtractor<>(ROW_MAPPER);

	private static final StringToDistributionKeyTypeCodeConverter TYPE_CODE_CONVERTER =
			new StringToDistributionKeyTypeCodeConverter();

	private static final String SINGLE_ROW_LOOKUP = DistributionFilterRowMapper.SELECT_SQL +
			" WHERE TRG_SYSTEM_ID = ? AND DSTRB_KEY_TYP_CD = ? AND ATTR_VAL_NBR = ?";

	private final transient JdbcTemplate jdbcTemplate;

	/**
	 * Constructs a new DistributionFilterLookup.
	 *
	 * @param jdbcTemplate The JdbcTemplate to use to run queries with.
	 */
	public DistributionFilterLookup(JdbcTemplate jdbcTemplate) {
		this.jdbcTemplate = jdbcTemplate;
	}

	/**
	 * Finds a DistributionFilter with a given ID. Will return empty if not found.
	 *
	 * @param key They key of the DistributionFilter to find.
	 * @return The DistributionFilter that matches key or empty.
	 */
	public Optional<DistributionFilter> findById(DistributionFilterKey key) {

		return Optional.ofNullable(this.jdbcTemplate.query(SINGLE_ROW_LOOKUP,
				JdbcUtils.argsAsArray(key.getTargetSourceSystemId(),
						TYPE_CODE_CONVERTER.convertToDatabaseColumn(key.getDistributionKeyTypeCode()),
						key.getAttributeValueNumber()),
				SINGLE_ROW_RESULT_SET_EXTRACTOR));
	}
}
