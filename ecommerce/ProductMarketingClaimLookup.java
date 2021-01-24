package com.heb.pm.core.service.maintenance.ecommerce;

import com.heb.pm.dao.core.entity.ProductMarketingClaim;
import com.heb.pm.dao.core.entity.ProductMarketingClaimKey;
import com.heb.pm.dao.core.rowmappers.ProductMarketingClaimRowMapper;
import com.heb.pm.dao.core.rowmappers.SingleRowResultSetExtractor;
import com.heb.pm.util.JdbcUtils;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.Optional;

/**
 * Looks up ProductMarketingClaims.
 *
 * @author d116773
 * @since 1.23.0
 */
public class ProductMarketingClaimLookup {

	private static final ProductMarketingClaimRowMapper ROW_MAPPER = new ProductMarketingClaimRowMapper();
	private static final SingleRowResultSetExtractor<ProductMarketingClaim> SINGLE_ROW_RESULT_SET_EXTRACTOR =
			new SingleRowResultSetExtractor<>(ROW_MAPPER);


	private static final String SINGLE_ROW_LOOKUP = ProductMarketingClaimRowMapper.SELECT_SQL + " WHERE PROD_ID = ? AND MKT_CLM_CD = ?";

	private final transient JdbcTemplate jdbcTemplate;

	/**
	 * Constructs a new ProductMarketingClaimLookup.
	 *
	 * @param jdbcTemplate The JdbcTemplate to use to run queries.
	 */
	public ProductMarketingClaimLookup(JdbcTemplate jdbcTemplate) {
		this.jdbcTemplate = jdbcTemplate;
	}

	/**
	 * Returns a ProductMarketingClaim that matches a key or empty.
	 *
	 * @param key The key to lookup.
	 * @return A ProductMarketingClaim that matches the key or empty.
	 */
	public Optional<ProductMarketingClaim> findById(ProductMarketingClaimKey key) {

		return Optional.ofNullable(this.jdbcTemplate.query(SINGLE_ROW_LOOKUP,
				JdbcUtils.argsAsArray(key.getProdId(), key.getMarketingClaimCode().getId()),
				SINGLE_ROW_RESULT_SET_EXTRACTOR));
	}
}
