package com.heb.pm.core.service.maintenance.product;

import com.heb.pm.core.event.LegacyEventProcessor;
import com.heb.pm.core.service.maintenance.common.MaintenanceConfig;
import com.heb.pm.dao.core.LegacyEventGenerator;
import com.heb.pm.dao.core.entity.GoodsProduct;
import com.heb.pm.dao.core.entity.LegacyEvent;
import com.heb.pm.dao.core.entity.codes.LegacyEventFunction;
import com.heb.pm.dao.core.quicklookup.ProductLookup;
import org.springframework.transaction.annotation.Transactional;

import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Holds common functions when updating product-level data.
 *
 * @author d116773
 * @since 1.24.0
 */
/* default */ class ProductMaintenanceUtils {

	private final transient String programName;
	private final transient ProductLookup productLookup;
	private final transient LegacyEventProcessor legacyEventProcessor;
	private final transient String userId;
	private final transient long systemId;

	/**
	 * Constructs a new ProductMaintenanceUtils.
	 *
	 * @param maintenanceConfig The configuration for the class to use.
	 * @param userId The ID of the user who is making changes.
	 */
	protected ProductMaintenanceUtils(MaintenanceConfig maintenanceConfig, String userId) {

		this.productLookup = new ProductLookup(maintenanceConfig.getJdbcTemplate());
		this.programName = maintenanceConfig.getProgramName();
		this.legacyEventProcessor = maintenanceConfig.getLegacyEventProcessor();
		this.userId = userId;
		this.systemId = maintenanceConfig.getSystemId();
	}

	/**
	 * General purpose function for doing a set of updates to GoodsProd. This will also generate a common
	 * set of events (PRMM, PRM2, GPDM, and PSC2 for the product's primary UPC).
	 *
	 * @param goodsProducts The potential list of GoodsProduct records to update.
	 * @param goodsProductMapper A callback that will be used to to manipulate and filter the original list. The function
	 *                           will be passed, one-by-one, a GoodsProduct from goodsProducts. The called method
	 *                           can manipulate the object if necessary. The update fields (user, time, etc.) will already
	 *                           be set, so, in general, the function should return the same object passed in. If the
	 *                           method wishes to not update the record (for example, if nothing changed), then
	 *                           it can return empty.
	 * @param updater A callback that will perform the actual update to the table.
	 * @return The number of records updated.
	 */
	@Transactional
	protected int doGoodsProductUpdate(List<GoodsProduct> goodsProducts, Function<GoodsProduct, Optional<GoodsProduct>> goodsProductMapper, Consumer<List<GoodsProduct>> updater) {

		List<GoodsProduct> goodsProductsToUpdate = this.getGoodsProductsToUpdate(goodsProducts, goodsProductMapper);

		updater.accept(goodsProductsToUpdate);

		List<LegacyEvent> legacyEvents = this.getLegacyEvents(goodsProductsToUpdate);

		this.legacyEventProcessor.addAndFlush(legacyEvents);

		return goodsProductsToUpdate.size();
	}

	/**
	 * Method that takes a list of GoodsProducts, applies a mapper, and then sets the common update fields (user, time, etc.).
	 *
	 * @param goodsProducts The list of GoodsProducts to process.
	 * @param goodsProductMapper The mapping function. This function follows the same contract as goodsProductMapper in
	 *                           doGoodsProductUpdate.
	 * @return The list of GoodsProduct objects to update.
	 */
	protected List<GoodsProduct> getGoodsProductsToUpdate(List<GoodsProduct> goodsProducts, Function<GoodsProduct, Optional<GoodsProduct>> goodsProductMapper) {

		return goodsProducts.stream()
				.map(goodsProductMapper)
				.filter(Optional::isPresent)
				.map(Optional::get)
				.map(this::setUpdateFields)
				.collect(Collectors.toList());
	}

	/**
	 * Generates a list of LegacyEvents to publish.
	 *
	 * @param products The list of GoodsProucts to generate events for.
	 * @param generator The method that actually does the generation.
	 * @return The list of LegacyEvents to publish.
	 */
	protected List<LegacyEvent> getLegacyEvents(List<GoodsProduct> products, Function<GoodsProduct, List<LegacyEvent>> generator) {

		return products.stream()
				.map(generator)
				.flatMap(List::stream)
				.collect(Collectors.toList());
	}

	/**
	 * Generates a list of LegacyEvents to publish. This overloading uses generateProductEvents below.
	 *
	 * @param products The list of GoodsProucts to generate events for.
	 * @return The list of LegacyEvents to publish.
	 */
	protected List<LegacyEvent> getLegacyEvents(List<GoodsProduct> products) {

		return this.getLegacyEvents(products, this::generateProductEvents);
	}

	/**
	 * Generates a list of LegacyEvents for a single GoodsProduct. This will generate a PRM2, a PRMM, a GPDM, and a PSC2
	 * for the product's primary UPC.
	 *
	 * @param goodsProduct The GoodsProduct to generate events for.
	 * @return The list of generated LegacyEvents.
	 */
	protected List<LegacyEvent> generateProductEvents(GoodsProduct goodsProduct) {

		List<LegacyEvent> legacyEvents = new LinkedList<>();

		this.productLookup.getProductPrimaryUpc(goodsProduct.getProdId())
				.map(u -> generatePSC2(u, goodsProduct.getLastUpdateUserId()))
				.ifPresent(legacyEvents::add);

		legacyEvents.add(LegacyEventGenerator.generatePRM2(goodsProduct.getProdId(), this.programName, goodsProduct.getLastUpdateUserId(), LegacyEventFunction.UPDATE));
		legacyEvents.add(LegacyEventGenerator.generatePRMM(goodsProduct.getProdId(), this.programName, goodsProduct.getLastUpdateUserId(), LegacyEventFunction.UPDATE));
		legacyEvents.add(LegacyEventGenerator.generateGPDM(goodsProduct.getProdId(), this.programName, goodsProduct.getLastUpdateUserId(), LegacyEventFunction.UPDATE));

		return legacyEvents;
	}

	/**
	 * Generates a PSC2 for a UPC.
	 *
	 * @param upc The UPC to generate the PSC2 for.
	 * @param userId The ID of the user who triggered the event.
	 * @return A PSC2 event for the UPC.
	 */
	protected LegacyEvent generatePSC2(long upc, String userId) {
		return LegacyEventGenerator.generatePSC2(upc, this.programName, userId, LegacyEventFunction.UPDATE);
	}

	/**
	 * Sets the common update fields on a GoodsProduct (last update user, time, and system ID).
	 *
	 * @param goodsProduct The GoodsProduct to set the fields for.
	 * @return The updated GoodsProduct.
	 */
	protected GoodsProduct setUpdateFields(GoodsProduct goodsProduct) {

		goodsProduct.setLastSystemUpdateId(this.systemId);
		goodsProduct.setLastUpdateUserId(this.userId);
		return goodsProduct;
	}
}
