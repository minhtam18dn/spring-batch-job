package com.heb.pm.core.service.maintenance.ecommerce;

import com.heb.pm.dao.core.entity.ProductRelationship;
import com.heb.pm.dao.core.entity.codes.ProductRelationshipTypeCode;
import com.heb.pm.dao.core.rowmappers.ProductRelationshipRowMapper;
import com.heb.pm.dao.core.rowmappers.SingleRowResultSetExtractor;
import com.heb.pm.util.JdbcUtils;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.annotation.Transactional;

import java.util.Optional;

/**
 * Looks up ProductRelationships.
 *
 * @author d116773
 * @since 1.23.0
 */
public class ProductRelationshipLookup {

	private static final String SEARCH_FOR_PRODUCT_RELATIONSHIP = ProductRelationshipRowMapper.SELECT_SQL +
			" WHERE PROD_ID = ? AND PROD_RLSHP_CD = ? AND RELATED_PROD_ID = ?";

	private static final ProductRelationshipRowMapper PRODUCT_RELATIONSHIP_ROW_MAPPER = new ProductRelationshipRowMapper();

	private final transient JdbcTemplate jdbcTemplate;

	/**
	 * Constructs a new ProductRelationshipLookup.
	 *
	 * @param jdbcTemplate The JdbcTemplate to use to run queries.
	 */
	public ProductRelationshipLookup(JdbcTemplate jdbcTemplate) {

		this.jdbcTemplate = jdbcTemplate;
	}

	/**
	 * Finds a specific ProductRelationship.
	 *
	 * @param productId The product ID in the relationship.
	 * @param relatedProductId The related product ID.
	 * @param productRelationshipTypeCode The type of relationship.
	 * @return A ProductRelationship matching the parameters if it exists and empty if not.
	 */
	@Transactional
	public Optional<ProductRelationship> find(Long productId, Long relatedProductId, ProductRelationshipTypeCode productRelationshipTypeCode) {

		SingleRowResultSetExtractor<ProductRelationship> productRelationshipExtractor = new SingleRowResultSetExtractor<>(PRODUCT_RELATIONSHIP_ROW_MAPPER);

		return Optional.ofNullable(this.jdbcTemplate.query(SEARCH_FOR_PRODUCT_RELATIONSHIP,
				JdbcUtils.argsAsArray(productId, productRelationshipTypeCode.getId(), relatedProductId),
				productRelationshipExtractor));
	}
}
