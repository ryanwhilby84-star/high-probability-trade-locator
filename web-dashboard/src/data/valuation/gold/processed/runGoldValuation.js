import { loadGoldValuationInputs } from "../raw/goldDataLoader.js";

import {
  calculateGoldFairValue,
  calculateValuationPercent,
  classifyGoldValuationBand,
} from "../models/goldValuationModel.js";

export async function runGoldValuation() {
  const inputs = await loadGoldValuationInputs();

  const fairValue = calculateGoldFairValue({
    realYield10Y: inputs.realYield10Y,
    dollarIndex: inputs.dollarIndex,
    breakeven10Y: inputs.breakeven10Y,
  });

  const valuationPct = calculateValuationPercent(inputs.price, fairValue);
  const band = classifyGoldValuationBand(valuationPct);

  return {
    instrument: "gold",
    model: "gold_fair_value_v1",
    date: new Date().toISOString().slice(0, 10),

    price: Number(inputs.price.toFixed(2)),
    fairValue: Number(fairValue.toFixed(2)),
    valuationPct: Number(valuationPct.toFixed(2)),
    band,

    inputs,
  };
}