export function calculateValuationPercent(price, fairValue) {
  if (!price || !fairValue) return null;
  return ((price - fairValue) / fairValue) * 100;
}

export function classifyGoldValuationBand(valuationPct) {
  if (valuationPct === null || valuationPct === undefined) return "unknown";

  if (valuationPct >= 20) return "extreme_overvalued";
  if (valuationPct >= 10) return "overvalued";
  if (valuationPct >= 5) return "mildly_overvalued";

  if (valuationPct <= -20) return "extreme_undervalued";
  if (valuationPct <= -10) return "undervalued";
  if (valuationPct <= -5) return "mildly_undervalued";

  return "fair_value";
}

export function calculateGoldFairValue({
  realYield10Y,
  dollarIndex,
  breakeven10Y,
}) {
  const baseFairValue = 2000;

  const realYieldImpact = realYield10Y * -120;
  const dollarImpact = (dollarIndex - 100) * -12;
  const breakevenImpact = breakeven10Y * 80;

  return baseFairValue + realYieldImpact + dollarImpact + breakevenImpact;
}