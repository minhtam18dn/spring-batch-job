package com.heb.pm.core.service.maintenance.ecommerce;

import com.heb.pm.dao.core.entity.ProductDescription;
import com.heb.pm.dao.core.entity.ProductDescriptionKey;
import com.heb.pm.dao.core.rowmappers.ProudctDescriptionRowMapper;
import com.heb.pm.dao.core.rowmappers.SingleRowResultSetExtractor;
import com.heb.pm.util.JdbcUtils;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.Optional;

/**
 * Handles looking up ProductDescriptions.
 *
 * @author d116773
 * @since 1.23.0
 */
public class ProductDescriptionLookup {

	private static final String SINGLE_ROW_LOOKUP = ProudctDescriptionRowMapper.SELECT_SQL + " WHERE PROD_ID = ? AND DES_TYP_CD = ? AND LANG_TYP_CD = ?";

	private static final ProudctDescriptionRowMapper PROUDCT_DESCRIPTION_ROW_MAPPER = new ProudctDescriptionRowMapper();
	private static final SingleRowResultSetExtractor<ProductDescription> SINGLE_ROW_RESULT_SET_EXTRACTOR = new SingleRowResultSetExtractor<>(PROUDCT_DESCRIPTION_ROW_MAPPER);

	private final transient JdbcTemplate jdbcTemplate;

	/**
	 * Constructs a new ProductDescriptionLookup.
	 *
	 * @param jdbcTemplate The JdbcTemplate to use to run queries with.
	 */
	public ProductDescriptionLookup(JdbcTemplate jdbcTemplate) {
		this.jdbcTemplate = jdbcTemplate;
	}

	/**
	 * Returns a ProductDescription matching a key if it exists and empty otherwise.
	 *
	 * @param key The key to look for.
	 * @return The ProductDescription that matches key or empty.
	 */
	public Optional<ProductDescription> findById(ProductDescriptionKey key) {

		return Optional.ofNullable(this.jdbcTemplate.query(SINGLE_ROW_LOOKUP,
				JdbcUtils.argsAsArray(key.getProductId(), key.getDescriptionType().getId(), key.getLanguageType()),
				SINGLE_ROW_RESULT_SET_EXTRACTOR));
	}
}
