package com.heb.pm.core.service.maintenance.product;

import com.heb.pm.dao.core.entity.GoodsProduct;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;

import javax.annotation.Nonnull;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

/**
 * Base class for classes that update goods_prod.
 *
 * @author d116773
 * @since 1.24.0
 */
/* default */ abstract class BaseGoodsProductSetter implements BatchPreparedStatementSetter {

	private final transient List<GoodsProduct> products;

	/**
	 * Constructs a new BaseGoodsProductSetter.
	 *
	 * @param products The list of GoodsProduct objects to update.
	 */
	protected BaseGoodsProductSetter(Collection<GoodsProduct> products) {
		this.products = new ArrayList<>(products);
	}

	/**
	 * Overriding classes should implement this method and set whatever fields are needed. It should return the number
	 * of fields set in the prepared statement (if you set one field, return 1, if you set two fields, return 2).
	 *
	 * @param ps The PreparedStatement to set values of.
	 * @param goodsProduct The GoodsProduct to pull the values from to do the update.
	 * @return The number of fields updated.
	 * @throws SQLException
	 */
	protected abstract int setProductValues(@Nonnull PreparedStatement ps, @Nonnull GoodsProduct goodsProduct) throws SQLException;

	@Override
	public void setValues(@Nonnull PreparedStatement ps, int i) throws SQLException {

		GoodsProduct gp = this.products.get(i);
		if (Objects.isNull(gp)) {
			throw new IllegalArgumentException("Attempted to set goods product values for empty object.");
		}

		// Delegate the other fields.
		int index = this.setProductValues(ps, gp);

		// Set the common fields (last update fields and he product ID).
		ps.setString(++index, gp.getLastUpdateUserId());
		ps.setTimestamp(++index, Timestamp.from(Instant.now()));
		ps.setLong(++index, gp.getLastSystemUpdateId());
		ps.setLong(++index, gp.getProdId());
	}

	@Override
	public int getBatchSize() {
		return this.products.size();
	}
}
