package com.heb.pm.core.service.maintenance.product;

import com.heb.pm.dao.core.converters.StringToConsumerPurchaseChoiceConverter;
import com.heb.pm.dao.core.entity.GoodsProduct;
import com.heb.pm.dao.core.entity.ProductShippingHandling;
import com.heb.pm.dao.core.rowmappers.ProductShippingHandlingRowMapper;
import com.heb.pm.dao.core.rowmappers.SingleRowResultSetExtractor;
import com.heb.pm.dao.oracle.BigDecimalDoubleScaledConverter;
import com.heb.pm.dao.oracle.BigDecimalQuadrupleScaledConverter;
import com.heb.pm.util.JdbcUtils;
import com.heb.pm.util.jpa.SwitchToBooleanConverter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

import java.util.Optional;

/**
 * Lookup for product dimension data.
 *
 * @author d116773
 * @since 1.25.0
 */
public class DimensionsLookup {

	private static final String DIMENSIONS_SELECT_SQL = "SELECT PROD_ID, RETL_UNT_LN, RETL_UNT_WD, RETL_UNT_HT, RETL_UNT_WT, " +
			"CONSM_PRCH_CHC_CD, WT_SW, LRGE_PROD_SHPN_SW " +
			"FROM EMD.GOODS_PROD WHERE PROD_ID = ?";
	private static final String SHIPPING_HANDLING_SELECT_SQL = ProductShippingHandlingRowMapper.SELECT_SQL + " WHERE PROD_ID = ?";

	private static final BigDecimalDoubleScaledConverter DIMENSION_CONVERTER = new BigDecimalDoubleScaledConverter();
	private static final BigDecimalQuadrupleScaledConverter BIG_DECIMAL_QUADRUPLE_SCALED_CONVERTER = new BigDecimalQuadrupleScaledConverter();

	private static final SwitchToBooleanConverter BOOLEAN_CONVERTER = new SwitchToBooleanConverter();
	private static final StringToConsumerPurchaseChoiceConverter PURCHASE_CHOICE_CONVERTER = new StringToConsumerPurchaseChoiceConverter();

	private static final ProductShippingHandlingRowMapper SHIPPING_HANDLING_ROW_MAPPER = new ProductShippingHandlingRowMapper();
	private static final SingleRowResultSetExtractor<ProductShippingHandling> SHIPPING_HANDLING_SINGLE_ROW_RESULT_SET_EXTRACTOR = new SingleRowResultSetExtractor(SHIPPING_HANDLING_ROW_MAPPER);

	private final transient JdbcTemplate jdbcTemplate;

	private static final RowMapper<GoodsProduct> GOODS_PRODUCT_ROW_MAPPER = (rs, rowNum) ->
			new GoodsProduct().setProdId(rs.getLong("PROD_ID"))
					.setRetailUnitLength(DIMENSION_CONVERTER.convertToEntityAttribute(rs.getBigDecimal("RETL_UNT_LN")))
					.setRetailUnitWidth(DIMENSION_CONVERTER.convertToEntityAttribute(rs.getBigDecimal("RETL_UNT_WD")))
					.setRetailUnitHeight(DIMENSION_CONVERTER.convertToEntityAttribute(rs.getBigDecimal("RETL_UNT_HT")))
					.setRetailUnitWeight(BIG_DECIMAL_QUADRUPLE_SCALED_CONVERTER.convertToEntityAttribute(rs.getBigDecimal("RETL_UNT_WT")))
					.setEcommerceSoldBy(PURCHASE_CHOICE_CONVERTER.convertToEntityAttribute(rs.getString("CONSM_PRCH_CHC_CD")))
					.setLargeProductShippingSwitch(BOOLEAN_CONVERTER.convertToEntityAttribute(rs.getString("LRGE_PROD_SHPN_SW")))
					.setSoldBy(rs.getString("CONSM_PRCH_CHC_CD"))
					.setSoldByWeightSwitch(rs.getString("WT_SW"));

	private static final SingleRowResultSetExtractor<GoodsProduct> SINGLE_GOODS_PRODUCT_EXTRACTOR = new SingleRowResultSetExtractor<>(GOODS_PRODUCT_ROW_MAPPER);

	/**
	 * Constructs a new DimensionsLookup.
	 *
	 * @param jdbcTemplate The JdbcTemplate to use to run queries.
	 */
	public DimensionsLookup(JdbcTemplate jdbcTemplate) {
		this.jdbcTemplate = jdbcTemplate;
	}

	/**
	 * Returns a GoodsProduct with dimensional data populated.
	 *
	 * @param productId The product ID to lookup.
	 * @return That product's GoodsProduct with dimensional data or empty.
	 */
	public Optional<GoodsProduct> findById(long productId) {

		return Optional.ofNullable(this.jdbcTemplate.query(DIMENSIONS_SELECT_SQL, JdbcUtils.argsAsArray(productId),
				SINGLE_GOODS_PRODUCT_EXTRACTOR));
	}

	/**
	 * Returns a PROD_SHPNG_HNDLG record for a product.
	 *
	 * @param productId The product ID to look up.
	 * @return The product's PROD_SHPNG_HNDLG data or empty.
	 */
	public Optional<ProductShippingHandling> findShippingHandlingByProduct(long productId) {

		return Optional.ofNullable(this.jdbcTemplate.query(SHIPPING_HANDLING_SELECT_SQL, JdbcUtils.argsAsArray(productId),
				SHIPPING_HANDLING_SINGLE_ROW_RESULT_SET_EXTRACTOR));
	}
}
