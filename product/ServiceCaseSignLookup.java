package com.heb.pm.core.service.maintenance.product;

import com.heb.pm.dao.core.entity.ProductDescription;
import com.heb.pm.dao.core.entity.codes.LanguageType;
import com.heb.pm.dao.core.rowmappers.ProudctDescriptionRowMapper;
import com.heb.pm.util.JdbcUtils;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.List;

/**
 * Handles looking up ServiceCaseSign.
 *
 * @author m314029
 * @since 1.25.0
 */
public class ServiceCaseSignLookup {

	private static final String FIND_BY_PRODUCT_AND_DESCRIPTIONS_AND_LANGUAGE_LOOKUP =
			ProudctDescriptionRowMapper.SELECT_SQL + " WHERE PROD_ID = ? AND LANG_TYP_CD = ? AND DES_TYP_CD in ('SRVCC','SGNRC','SGNRP')";

	private static final ProudctDescriptionRowMapper PRODUCT_DESCRIPTION_ROW_MAPPER = new ProudctDescriptionRowMapper();

	private final transient JdbcTemplate jdbcTemplate;

	/**
	 * Constructs a new ProductDescriptionLookup.
	 *
	 * @param jdbcTemplate The JdbcTemplate to use to run queries with.
	 */
	public ServiceCaseSignLookup(JdbcTemplate jdbcTemplate) {
		this.jdbcTemplate = jdbcTemplate;
	}

	/**
	 * Finds product descriptions by product id, language type, and where description type is one of the service case
	 * descriptions type.
	 *
	 * @param productId Product id to search for.
	 * @param languageType Language type to search for.
	 * @return The ProductDescriptions that match the search.
	 */
	public List<ProductDescription> findAllByProductIdAndLanguageType(Long productId, LanguageType languageType) {

		return this.jdbcTemplate.query(FIND_BY_PRODUCT_AND_DESCRIPTIONS_AND_LANGUAGE_LOOKUP,
				JdbcUtils.argsAsArray(productId, languageType.getId()), PRODUCT_DESCRIPTION_ROW_MAPPER);
	}
}
