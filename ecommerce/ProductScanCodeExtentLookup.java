package com.heb.pm.core.service.maintenance.ecommerce;

import com.heb.pm.dao.core.entity.ProductScanCodeExtent;
import com.heb.pm.dao.core.entity.ProductScanCodeExtentKey;
import com.heb.pm.dao.core.rowmappers.ProductScanCodeExtentRowMapper;
import com.heb.pm.dao.core.rowmappers.SingleRowResultSetExtractor;
import com.heb.pm.util.JdbcUtils;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.List;
import java.util.Optional;

import static com.heb.pm.util.ValidatorUtils.notNullOrIllegalArgument;

/**
 * Lookup class for records in PROD_SCN_CD_EXTEND.
 *
 * @author d116773
 * @since 1.25.0
 */
public class ProductScanCodeExtentLookup {

	private static final String SELECT_BY_KEY_SQL = ProductScanCodeExtentRowMapper.SELECT_SQL +
			" WHERE SCN_CD_ID = ? AND PROD_EXT_DTA_CD = ?";

	private static final String SELECT_BY_UPC_SQL = ProductScanCodeExtentRowMapper.SELECT_SQL +
			" WHERE SCN_CD_ID = ?";

	private static final ProductScanCodeExtentRowMapper ROW_MAPPER = new ProductScanCodeExtentRowMapper();
	private static final SingleRowResultSetExtractor<ProductScanCodeExtent> SINGLE_ROW_RESULT_SET_EXTRACTOR =
			new SingleRowResultSetExtractor<>(ROW_MAPPER);


	private final transient JdbcTemplate jdbcTemplate;

	/**
	 * Constructs a new ProductScanCodeExtentLookup.
	 *
	 * @param jdbcTemplate The JdbcTemplate to use to run queries.
	 */
	public ProductScanCodeExtentLookup(JdbcTemplate jdbcTemplate) {
		this.jdbcTemplate = jdbcTemplate;
	}

	/**
	 * Returns a single row from PROD_SCN_CD_EXTEND with a given key. If none is found, will return empy.
	 *
	 * @param key They key to lookup.
	 * @return The ProductScanCodeExtent that matches key or empty.
	 * @throws IllegalArgumentException If the key is null or contains any null values.
	 */
	public Optional<ProductScanCodeExtent> findById(ProductScanCodeExtentKey key) {

		notNullOrIllegalArgument(key, "Key is required.");
		notNullOrIllegalArgument(key.getScanCodeId(), "UPC is required.");
		notNullOrIllegalArgument(key.getProductExtentDataCode(), "Type is required.");

		return Optional.ofNullable(this.jdbcTemplate.query(SELECT_BY_KEY_SQL,
				JdbcUtils.argsAsArray(key.getScanCodeId(), key.getProductExtentDataCode().getId()),
				SINGLE_ROW_RESULT_SET_EXTRACTOR));
	}

	/**
	 * Returns all rows from PROD_SCN_CD_EXTEND for a given UPC. The list may be empty but will not be null.
	 *
	 * @param upc The UPC to look up.
	 * @return A list of ProductScanCodeExtents for a given UPC.
	 * @throws IllegalArgumentException If upc is null.
	 */
	public List<ProductScanCodeExtent> findByUpc(Long upc) {

		notNullOrIllegalArgument(upc, "UPC is required.");
		return this.jdbcTemplate.query(SELECT_BY_UPC_SQL,
				JdbcUtils.argsAsArray(upc),
				ROW_MAPPER);
	}
}
