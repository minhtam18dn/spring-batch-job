package com.heb.pm.core.service.maintenance.ecommerce;

import com.heb.pm.core.exception.NotFoundException;
import com.heb.pm.dao.core.entity.ProductOnline;
import com.heb.pm.dao.core.quicklookup.ProductLookup;
import com.heb.pm.dao.core.rowmappers.ProductOnlineRowMapper;
import com.heb.pm.util.JdbcUtils;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

/**
 * All the lookups for ProductOnlineService are delegated to this class.
 *
 * @author d116773
 * @since 1.22.0
 */
/* default */ class ProductOnlineLookup {

	private static final String NOT_FOUND_ERROR = "%d is neither a product ID nor a product group ID.";

	private static final String ID_IS_PRODUCT_GROUP_SQL = "SELECT CUST_PROD_GRP_ID FROM EMD.CUST_PROD_GRP WHERE CUST_PROD_GRP_ID = ?";

	private static final String SALES_CHANNEL_EXISTS_SQL = "SELECT SALS_CHNL_CD FROM EMD.SALS_CHNL WHERE SALS_CHNL_CD = ?";

	private static final String SELECT_ALL_FOR_PRODUCT_AND_SALES_CHANNEL_SQL = ProductOnlineRowMapper.SELECT_SQL +
			" WHERE PROD_ID = ? AND SALS_CHNL_CD = ? AND EXPRN_DT > TRUNC(SYSDATE)";

	private static final String SELECT_ALL_FOR_PRODUCT_SQL = ProductOnlineRowMapper.SELECT_SQL +
			" WHERE PROD_ID = ? AND EXPRN_DT > TRUNC(SYSDATE)";

	private static final ProductOnlineRowMapper PRODUCT_ONLINE_ROW_MAPPER = new ProductOnlineRowMapper();

	private final transient JdbcTemplate jdbcTemplate;
	private final transient ProductLookup productLookup;

	/**
	 * Constructs a new ProductOnlineLookup.
	 *
	 * @param jdbcTemplate The class to use to run queries.
	 */
	protected ProductOnlineLookup(JdbcTemplate jdbcTemplate) {
		this.jdbcTemplate = jdbcTemplate;
		this.productLookup = new ProductLookup(this.jdbcTemplate);
	}

	/**
	 * Returns true if an ID is a product ID nor a product group ID.
	 *
	 * @param productId The ID to check.
	 * @return True if an ID is neither a product ID nor a product group ID and false otherwise.
	 */
	protected boolean notFound(Long productId) {

		return !this.productLookup.isProductId(productId) && !isProductGroupId(productId);
	}

	/**
	 * Returns true if an ID is a product group ID .
	 *
	 * @param productId The ID to check.
	 * @return True if an ID is a product group ID and false otherwise.
	 */
	protected boolean isProductGroupId(Long productId) {

		return JdbcUtils.runCountQuery(this.jdbcTemplate, ID_IS_PRODUCT_GROUP_SQL, productId) > 0;
	}

	/**
	 * Returns true if a sales channel does not exist.
	 *
	 * @param salesChannel The sales channel to check.
	 * @return True if a sales channel does not exist and false otherwise.
	 */
	protected boolean notFound(String salesChannel) {

		return JdbcUtils.runCountQuery(this.jdbcTemplate, SALES_CHANNEL_EXISTS_SQL, salesChannel) == 0;
	}

	/**
	 * Returns a list of ProductOnlineRange for a given product or product group.
	 *
	 * It only returns current and future events.
	 *
	 * This will throw an error if the product does not exist and return an empty list if the product or group
	 * exists but there is no product online information.
	 *
	 * @param productId The ID of product or product group to look for information for.
	 * @return The list of ProductOnlineRanges for the supplied product ID.
	 */
	public List<ProductOnline> getByProductId(Long productId) {

		if (this.notFound(productId)) {
			throw new NotFoundException(String.format(NOT_FOUND_ERROR, productId));
		}

		return this.jdbcTemplate.query(SELECT_ALL_FOR_PRODUCT_SQL, JdbcUtils.argsAsArray(productId), PRODUCT_ONLINE_ROW_MAPPER);
	}

	/**
	 * Reads all the ProductOnline records for a given product or product group for a given sales channel.
	 *
	 * @param productId The ID of the product to look for ProductOnline records for.
	 * @param salesChannel The ID of the sales channel to look for ProductOnline records for.
	 * @return All the ProductOnline records for a given product or product group for a given sales channel.
	 */
	@Transactional
	protected List<ProductOnline> readListByProductAndSalesChannel(Long productId, String salesChannel) {


		if (this.notFound(productId)) {
			throw new NotFoundException(String.format(NOT_FOUND_ERROR, productId));
		}

		if (this.notFound(salesChannel)) {
			throw new NotFoundException(String.format("Sales channel '%s' not found.", salesChannel));
		}

		return this.jdbcTemplate.query(SELECT_ALL_FOR_PRODUCT_AND_SALES_CHANNEL_SQL,
				JdbcUtils.argsAsArray(productId, salesChannel),
				PRODUCT_ONLINE_ROW_MAPPER);
	}
}
