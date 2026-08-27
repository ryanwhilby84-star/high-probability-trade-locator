const OHLC_URL = "/data/workstation_ohlc_latest.json";

async function loadWorkstationOHLC() {
  const response = await fetch(`${OHLC_URL}?v=${Date.now()}`, {
    cache: "no-store",
  });

  if (!response.ok) {
    throw new Error("Unable to load workstation OHLC.");
  }

  return response.json();
}

export async function loadGoldValuationInputs() {
  const doc = await loadWorkstationOHLC();

  const gold =
    doc.instruments?.Gold ??
    doc.instruments?.["Gold / USD"] ??
    doc.instruments?.XAU_USD ??
    doc.instruments?.XAUUSD;

  if (!gold) {
    throw new Error("Gold not found in workstation_ohlc_latest.json");
  }

  const bars = gold.weekly_ohlc;

  const latest = bars[bars.length - 1];

  return {
    price: latest.close,

    // Temporary until we wire live macro feeds
    realYield10Y: 1.85,
    dollarIndex: 97.3,
    breakeven10Y: 2.35,
  };
}