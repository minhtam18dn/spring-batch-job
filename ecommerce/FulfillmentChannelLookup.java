package com.heb.pm.core.service.maintenance.ecommerce;

import com.heb.pm.core.exception.NotFoundException;
import com.heb.pm.dao.core.entity.ProductFulfillmentChannel;
import com.heb.pm.dao.core.quicklookup.ProductLookup;
import com.heb.pm.dao.core.rowmappers.ProductFulfillmentChannelRowMapper;
import com.heb.pm.dao.core.rowmappers.SingleRowResultSetExtractor;
import com.heb.pm.util.JdbcUtils;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Optional;

/**
 * All the lookups for FulfillmentChannelService are delegated to this class.
 *
 * @author m314029
 * @since 1.24.0
 */
/* default */ class FulfillmentChannelLookup {

	private static final String PRODUCT_NOT_FOUND_ERROR = "%d is not a product ID.";
	private static final String FULFILLMENT_CHANNEL_NOT_FOUND_ERROR = "Fulfillment channel with sales channel: '%s', " +
			"and fulfillment channel: '%s' not found.";
	private static final String MULTIPLE_FULFILLMENT_CHANNEL_NOT_FOUND_ERROR = "Fulfillment channel with sales channel: '%s', " +
			"not found.";

	private static final String FULFILLMENT_CHANNEL_EXISTS_SQL = "SELECT SALS_CHNL_CD, FLFL_CHNL_CD FROM EMD.FLFL_CHNL " +
			"WHERE SALS_CHNL_CD = ? and FLFL_CHNL_CD = ?";

	private static final String MULTIPLE_FULFILLMENT_CHANNEL_EXISTS_SQL = "SELECT SALS_CHNL_CD, FLFL_CHNL_CD FROM EMD.FLFL_CHNL " +
			"WHERE SALS_CHNL_CD = ?";

	private static final String SELECT_ALL_FOR_PRODUCT_AND_SALES_CHANNEL_AND_FULFILLMENT_CHANNEL_SQL =
			ProductFulfillmentChannelRowMapper.SELECT_SQL +	" WHERE PROD_ID = ? AND SALS_CHNL_CD = ? " +
					"AND FLFL_CHNL_CD = ?";

	private static final String SELECT_ALL_FOR_PRODUCT_AND_SALES_CHANNEL_SQL =
			ProductFulfillmentChannelRowMapper.SELECT_SQL +	" WHERE PROD_ID = ? AND SALS_CHNL_CD = ?";

	private static final String SELECT_ALL_FOR_PRODUCT_SQL = ProductFulfillmentChannelRowMapper.SELECT_SQL +
			" WHERE PROD_ID = ?";

	private static final ProductFulfillmentChannelRowMapper PRODUCT_ONLINE_ROW_MAPPER = new ProductFulfillmentChannelRowMapper();

	private static final SingleRowResultSetExtractor<ProductFulfillmentChannel> SINGLE_ROW_RESULT_SET_EXTRACTOR = new SingleRowResultSetExtractor<>(PRODUCT_ONLINE_ROW_MAPPER);

	private final transient JdbcTemplate jdbcTemplate;
	private final transient ProductLookup productLookup;

	/**
	 * Constructs a new FulfillmentChannelLookup.
	 *
	 * @param jdbcTemplate The class to use to run queries.
	 */
	protected FulfillmentChannelLookup(JdbcTemplate jdbcTemplate) {
		this.jdbcTemplate = jdbcTemplate;
		this.productLookup = new ProductLookup(this.jdbcTemplate);
	}

	/**
	 * Returns true if an ID is a product ID.
	 *
	 * @param productId The ID to check.
	 * @return True if an ID is not a product ID and false otherwise.
	 */
	public boolean notFound(Long productId) {

		return !this.productLookup.isProductId(productId);
	}

	/**
	 * Returns true if a fulfillment channel does not exist.
	 *
	 * @param salesChannel The sales channel to check.
	 * @param fulfillmentChannel The fulfillment channel to check.
	 * @return True if a fulfillment channel does not exist and false otherwise.
	 */
	public boolean notFound(String salesChannel, String fulfillmentChannel) {

		return JdbcUtils.runCountQuery(this.jdbcTemplate, FULFILLMENT_CHANNEL_EXISTS_SQL, JdbcUtils.argsAsArray(salesChannel, fulfillmentChannel)) == 0;
	}

	/**
	 * Returns true if no fulfillment channels exist tied to given sales channel.
	 *
	 * @param salesChannel The sales channel to check.
	 * @return True if a fulfillment channel does not exist and false otherwise.
	 */
	protected boolean notFound(String salesChannel) {

		return JdbcUtils.runCountQuery(this.jdbcTemplate, MULTIPLE_FULFILLMENT_CHANNEL_EXISTS_SQL, JdbcUtils.argsAsArray(salesChannel)) == 0;
	}

	/**
	 * Returns a list of ProductFulfillmentChannel for a given product.
	 *
	 * It only returns current and future events.
	 *
	 * This will throw an error if the product does not exist and return an empty list if the product
	 * exists but there is no product online information.
	 *
	 * @param productId The ID of product to look for information for.
	 * @return The list of ProductFulfillmentChannel for the supplied product ID.
	 */
	public List<ProductFulfillmentChannel> getByProductId(Long productId) throws NotFoundException {

		if (this.notFound(productId)) {
			throw new NotFoundException(String.format(PRODUCT_NOT_FOUND_ERROR, productId));
		}

		return this.jdbcTemplate.query(SELECT_ALL_FOR_PRODUCT_SQL, JdbcUtils.argsAsArray(productId), PRODUCT_ONLINE_ROW_MAPPER);
	}

	/**
	 * Reads the ProductFulfillmentChannel record for a given product tied to given sales and fulfillment channel. If one is not found, it returns empty.
	 *
	 * @param productId The ID of the product to look for ProductFulfillmentChannel records for.
	 * @param salesChannel The ID of the sales channel to look for ProductFulfillmentChannel records for.
	 * @param fulfillmentChannel The ID of the fulfillment channel to look for ProductFulfillmentChannel records for.
	 * @return The ProductFulfillmentChannel records for a given product for a given fulfillment channel or empty.
	 */
	@Transactional
	protected Optional<ProductFulfillmentChannel> readByProductAndSalesAndFulfillmentChannel(Long productId, String salesChannel, String fulfillmentChannel) throws NotFoundException {


		if (this.notFound(productId)) {
			throw new NotFoundException(String.format(PRODUCT_NOT_FOUND_ERROR, productId));
		}

		if (this.notFound(salesChannel, fulfillmentChannel)) {
			throw new NotFoundException(String.format(FULFILLMENT_CHANNEL_NOT_FOUND_ERROR, salesChannel, fulfillmentChannel));
		}

		return Optional.ofNullable(this.jdbcTemplate.query(SELECT_ALL_FOR_PRODUCT_AND_SALES_CHANNEL_AND_FULFILLMENT_CHANNEL_SQL,
				JdbcUtils.argsAsArray(productId, salesChannel, fulfillmentChannel), SINGLE_ROW_RESULT_SET_EXTRACTOR));
	}

	/**
	 * Reads all the ProductFulfillmentChannel records for a given product tied a given sales channel.
	 *
	 * @param productId The ID of the product to look for ProductFulfillmentChannel records for.
	 * @param salesChannel The ID of the sales channel to look for ProductFulfillmentChannel records for.
	 * @return All the ProductFulfillmentChannel records for a given product for a given sales channel.
	 */
	@Transactional
	protected List<ProductFulfillmentChannel> readListByProductAndSalesChannel(Long productId, String salesChannel) throws NotFoundException {


		if (this.notFound(productId)) {
			throw new NotFoundException(String.format(PRODUCT_NOT_FOUND_ERROR, productId));
		}

		if (this.notFound(salesChannel)) {
			throw new NotFoundException(String.format(MULTIPLE_FULFILLMENT_CHANNEL_NOT_FOUND_ERROR, salesChannel));
		}

		return this.jdbcTemplate.query(SELECT_ALL_FOR_PRODUCT_AND_SALES_CHANNEL_SQL,
				JdbcUtils.argsAsArray(productId, salesChannel),
				PRODUCT_ONLINE_ROW_MAPPER);
	}
}
