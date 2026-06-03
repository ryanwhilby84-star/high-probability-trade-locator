# HPTL Asset Universe & Data Coverage Audit

- Instruments in registry: **125**
- Displayed on radar (latest week): **125**
- Price candle/OHLC data store exists: **no** (price symbols configured: 112, instruments with no price candles: 125)
- FRED-backed macro relationship maps: **13**
- Instruments with any COT (direct/leg/proxy): **59**
- Classification: {'PRIMARY': 23, 'DERIVED': 36, 'ORPHANED': 33, 'NO_DATA': 10, 'DUPLICATE': 23}

## Task 1 — Full instrument inventory

| Instrument | Asset class | Canonical id | Price src | Macro | COT | COT status | Radar | Class |
|---|---|---|---|---|---|---|---|---|
| Bund | bonds | Bund | yes | no | no | no_cot_available | yes | NO_DATA |
| UK 10Y Gilt | bonds | UK 10Y Gilt | yes | no | no | no_cot_available | yes | NO_DATA |
| US 10Y T-Note | bonds | US 10Y T-Note | yes | no | no | no_cot_available | yes | NO_DATA |
| US 2Y T-Note | bonds | US 2Y T-Note | yes | no | no | no_cot_available | yes | NO_DATA |
| US 5Y T-Note | bonds | US 5Y T-Note | yes | no | no | no_cot_available | yes | ORPHANED |
| US T-Bond | bonds | US T-Bond | yes | no | no | no_cot_available | yes | NO_DATA |
| Brent Crude Oil | commodities | Brent Crude Oil | yes | no | no | no_cot_available | yes | NO_DATA |
| Cocoa | commodities | Cocoa | no | yes | yes | direct_cot | yes | PRIMARY |
| Coffee | commodities | Coffee | no | yes | yes | direct_cot | yes | PRIMARY |
| Corn | commodities | Corn | no | yes | yes | direct_cot | yes | PRIMARY |
| Crude Oil / CL | commodities | Crude Oil / CL | no | yes | yes | direct_cot | yes | PRIMARY |
| Natural Gas / NG | commodities | Natural Gas / NG | no | yes | yes | direct_cot | yes | PRIMARY |
| Soybeans | commodities | Soybeans | no | yes | yes | direct_cot | yes | PRIMARY |
| Sugar | commodities | Sugar | no | no | yes | direct_cot | yes | PRIMARY |
| West Texas Oil | commodities | Crude Oil / CL | yes | no | yes | proxy_cot | yes | DUPLICATE |
| Wheat | commodities | Wheat | no | yes | yes | direct_cot | yes | PRIMARY |
| Bitcoin | crypto | Bitcoin | yes | no | no | no_cot_available | yes | ORPHANED |
| Bitcoin Cash | crypto | Bitcoin Cash | yes | no | no | no_cot_available | yes | ORPHANED |
| Ethereum/Ether | crypto | Ethereum/Ether | yes | no | no | no_cot_available | yes | ORPHANED |
| Litecoin | crypto | Litecoin | yes | no | no | no_cot_available | yes | ORPHANED |
| AUD/CAD | fx | AUD/CAD | yes | no | yes | leg_derived_cot | yes | DERIVED |
| AUD/CHF | fx | AUD/CHF | yes | no | yes | leg_derived_cot | yes | DERIVED |
| AUD/HKD | fx | AUD/HKD | yes | no | no | macro_only | yes | DERIVED |
| AUD/JPY | fx | AUD/JPY | yes | no | yes | leg_derived_cot | yes | DERIVED |
| AUD/NZD | fx | AUD/NZD | yes | no | yes | leg_derived_cot | yes | DERIVED |
| AUD/SGD | fx | AUD/SGD | yes | no | no | macro_only | yes | DERIVED |
| AUD/USD | fx | Australian Dollar / 6A | yes | no | yes | leg_derived_cot | yes | DERIVED |
| Australian Dollar / 6A | fx | Australian Dollar / 6A | yes | no | yes | direct_cot | yes | PRIMARY |
| British Pound / 6B | fx | British Pound / 6B | yes | no | yes | direct_cot | yes | PRIMARY |
| CAD/HKD | fx | CAD/HKD | yes | no | no | macro_only | yes | DERIVED |
| CAD/SGD | fx | CAD/SGD | yes | no | no | macro_only | yes | DERIVED |
| CHF/HKD | fx | CHF/HKD | yes | no | no | macro_only | yes | DERIVED |
| CHF/ZAR | fx | CHF/ZAR | yes | no | no | macro_only | yes | ORPHANED |
| Canadian Dollar / 6C | fx | Canadian Dollar / 6C | yes | no | yes | direct_cot | yes | PRIMARY |
| EUR/AUD | fx | EUR/AUD | yes | no | yes | leg_derived_cot | yes | DERIVED |
| EUR/CZK | fx | EUR/CZK | yes | no | no | macro_only | yes | ORPHANED |
| EUR/DKK | fx | EUR/DKK | yes | no | no | macro_only | yes | DERIVED |
| EUR/HKD | fx | EUR/HKD | yes | no | no | macro_only | yes | DERIVED |
| EUR/HUF | fx | EUR/HUF | yes | no | no | macro_only | yes | ORPHANED |
| EUR/NOK | fx | EUR/NOK | yes | no | no | macro_only | yes | DERIVED |
| EUR/NZD | fx | EUR/NZD | yes | no | yes | leg_derived_cot | yes | DERIVED |
| EUR/SEK | fx | EUR/SEK | yes | no | no | macro_only | yes | DERIVED |
| EUR/SGD | fx | EUR/SGD | yes | no | no | macro_only | yes | DERIVED |
| EUR/TRY | fx | EUR/TRY | yes | no | no | macro_only | yes | ORPHANED |
| EUR/ZAR | fx | EUR/ZAR | yes | no | no | macro_only | yes | ORPHANED |
| Euro FX / 6E | fx | Euro FX / 6E | yes | no | yes | direct_cot | yes | PRIMARY |
| GBP/AUD | fx | GBP/AUD | yes | no | yes | leg_derived_cot | yes | DERIVED |
| GBP/HKD | fx | GBP/HKD | yes | no | no | macro_only | yes | DERIVED |
| GBP/NZD | fx | GBP/NZD | yes | no | yes | leg_derived_cot | yes | DERIVED |
| GBP/PLN | fx | GBP/PLN | yes | no | no | macro_only | yes | ORPHANED |
| GBP/SGD | fx | GBP/SGD | yes | no | no | macro_only | yes | DERIVED |
| GBP/ZAR | fx | GBP/ZAR | yes | no | no | macro_only | yes | ORPHANED |
| HKD/JPY | fx | HKD/JPY | yes | no | no | macro_only | yes | DERIVED |
| Japanese Yen / 6J | fx | Japanese Yen / 6J | yes | no | yes | direct_cot | yes | PRIMARY |
| NZ Dollar / 6N | fx | NZ Dollar / 6N | yes | no | yes | direct_cot | yes | PRIMARY |
| NZD/CAD | fx | NZD/CAD | yes | no | yes | leg_derived_cot | yes | DERIVED |
| NZD/CHF | fx | NZD/CHF | yes | no | yes | leg_derived_cot | yes | DERIVED |
| NZD/HKD | fx | NZD/HKD | yes | no | no | macro_only | yes | DERIVED |
| NZD/JPY | fx | NZD/JPY | yes | no | yes | leg_derived_cot | yes | DERIVED |
| NZD/SGD | fx | NZD/SGD | yes | no | no | macro_only | yes | DERIVED |
| NZD/USD | fx | NZ Dollar / 6N | yes | no | yes | leg_derived_cot | yes | DERIVED |
| SGD/CHF | fx | SGD/CHF | yes | no | no | macro_only | yes | DERIVED |
| SGD/JPY | fx | SGD/JPY | yes | no | no | macro_only | yes | DERIVED |
| Swiss Franc / 6S | fx | Swiss Franc / 6S | yes | no | yes | direct_cot | yes | PRIMARY |
| TRY/JPY | fx | TRY/JPY | yes | no | no | macro_only | yes | ORPHANED |
| USD/CNH | fx | USD/CNH | yes | no | no | no_cot_available | yes | ORPHANED |
| USD/CZK | fx | USD/CZK | yes | no | no | no_cot_available | yes | ORPHANED |
| USD/DKK | fx | USD/DKK | yes | no | no | no_cot_available | yes | DERIVED |
| USD/HKD | fx | USD/HKD | yes | no | no | no_cot_available | yes | DERIVED |
| USD/HUF | fx | USD/HUF | yes | no | no | no_cot_available | yes | ORPHANED |
| USD/INR | fx | USD/INR | yes | no | no | no_cot_available | yes | ORPHANED |
| USD/MXN | fx | USD/MXN | yes | no | no | no_cot_available | yes | ORPHANED |
| USD/NOK | fx | USD/NOK | yes | no | no | no_cot_available | yes | DERIVED |
| USD/PLN | fx | USD/PLN | yes | no | no | no_cot_available | yes | ORPHANED |
| USD/SAR | fx | USD/SAR | yes | no | no | no_cot_available | yes | ORPHANED |
| USD/SEK | fx | USD/SEK | yes | no | no | no_cot_available | yes | DERIVED |
| USD/SGD | fx | USD/SGD | yes | no | no | no_cot_available | yes | DERIVED |
| USD/THB | fx | USD/THB | yes | no | no | no_cot_available | yes | ORPHANED |
| USD/TRY | fx | USD/TRY | yes | no | no | no_cot_available | yes | ORPHANED |
| USD/ZAR | fx | USD/ZAR | yes | no | no | no_cot_available | yes | ORPHANED |
| ZAR/JPY | fx | ZAR/JPY | yes | no | no | macro_only | yes | ORPHANED |
| Australia 200 | indices | Australia 200 | yes | no | no | no_cot_available | yes | ORPHANED |
| China A50 | indices | China A50 | yes | no | no | no_cot_available | yes | ORPHANED |
| Dow / YM | indices | Dow / YM | yes | yes | yes | direct_cot | yes | PRIMARY |
| Europe 50 | indices | Europe 50 | yes | no | no | no_cot_available | yes | ORPHANED |
| France 40 | indices | France 40 | yes | no | no | no_cot_available | yes | ORPHANED |
| Germany 30 | indices | Germany 30 | yes | no | no | no_cot_available | yes | NO_DATA |
| Hong Kong 33 | indices | Hong Kong 33 | yes | no | no | no_cot_available | yes | ORPHANED |
| India 50 | indices | India 50 | yes | no | no | no_cot_available | yes | ORPHANED |
| Japan 225 | indices | Japan 225 | yes | no | no | no_cot_available | yes | NO_DATA |
| NASDAQ / NQ | indices | NASDAQ / NQ | yes | yes | yes | direct_cot | yes | PRIMARY |
| Netherlands 25 | indices | Netherlands 25 | yes | no | no | no_cot_available | yes | ORPHANED |
| S&P 500 / ES | indices | S&P 500 / ES | yes | yes | yes | direct_cot | yes | PRIMARY |
| Singapore 30 | indices | Singapore 30 | yes | no | no | no_cot_available | yes | ORPHANED |
| Taiwan Index | indices | Taiwan Index | yes | no | no | no_cot_available | yes | ORPHANED |
| UK 100 | indices | UK 100 | yes | no | no | no_cot_available | yes | NO_DATA |
| US Nas 100 | indices | NASDAQ / NQ | yes | no | yes | proxy_cot | yes | DUPLICATE |
| US Russ 2000 | indices | US Russ 2000 | yes | no | no | no_cot_available | yes | NO_DATA |
| US SPX 500 | indices | S&P 500 / ES | yes | no | yes | proxy_cot | yes | DUPLICATE |
| US Wall St 30 | indices | Dow / YM | yes | no | yes | proxy_cot | yes | DUPLICATE |
| Copper | metals | Copper / HG | yes | no | yes | proxy_cot | yes | DUPLICATE |
| Copper / HG | metals | Copper / HG | no | yes | yes | direct_cot | yes | PRIMARY |
| Gold | metals | Gold | no | yes | yes | direct_cot | yes | PRIMARY |
| Gold/AUD | metals | Gold | yes | no | yes | proxy_cot | yes | DUPLICATE |
| Gold/CAD | metals | Gold | yes | no | yes | proxy_cot | yes | DUPLICATE |
| Gold/CHF | metals | Gold | yes | no | yes | proxy_cot | yes | DUPLICATE |
| Gold/EUR | metals | Gold | yes | no | yes | proxy_cot | yes | DUPLICATE |
| Gold/GBP | metals | Gold | yes | no | yes | proxy_cot | yes | DUPLICATE |
| Gold/HKD | metals | Gold | yes | no | yes | proxy_cot | yes | DUPLICATE |
| Gold/JPY | metals | Gold | yes | no | yes | proxy_cot | yes | DUPLICATE |
| Gold/NZD | metals | Gold | yes | no | yes | proxy_cot | yes | DUPLICATE |
| Gold/SGD | metals | Gold | yes | no | yes | proxy_cot | yes | DUPLICATE |
| Gold/Silver | metals | Gold/Silver | yes | no | no | no_cot_available | yes | DERIVED |
| Palladium | metals | Palladium | no | no | yes | direct_cot | yes | PRIMARY |
| Platinum | metals | Platinum | no | no | yes | direct_cot | yes | PRIMARY |
| Silver | metals | Silver | no | yes | yes | direct_cot | yes | PRIMARY |
| Silver/AUD | metals | Silver | yes | no | yes | proxy_cot | yes | DUPLICATE |
| Silver/CAD | metals | Silver | yes | no | yes | proxy_cot | yes | DUPLICATE |
| Silver/CHF | metals | Silver | yes | no | yes | proxy_cot | yes | DUPLICATE |
| Silver/EUR | metals | Silver | yes | no | yes | proxy_cot | yes | DUPLICATE |
| Silver/GBP | metals | Silver | yes | no | yes | proxy_cot | yes | DUPLICATE |
| Silver/HKD | metals | Silver | yes | no | yes | proxy_cot | yes | DUPLICATE |
| Silver/JPY | metals | Silver | yes | no | yes | proxy_cot | yes | DUPLICATE |
| Silver/NZD | metals | Silver | yes | no | yes | proxy_cot | yes | DUPLICATE |
| Silver/SGD | metals | Silver | yes | no | yes | proxy_cot | yes | DUPLICATE |

## Task 2 — Duplicate / derived / no-data / orphaned

### DUPLICATE (23)
- Copper (metals) -> Copper / HG
- Gold/AUD (metals) -> Gold
- Gold/CAD (metals) -> Gold
- Gold/CHF (metals) -> Gold
- Gold/EUR (metals) -> Gold
- Gold/GBP (metals) -> Gold
- Gold/HKD (metals) -> Gold
- Gold/JPY (metals) -> Gold
- Gold/NZD (metals) -> Gold
- Gold/SGD (metals) -> Gold
- Silver/AUD (metals) -> Silver
- Silver/CAD (metals) -> Silver
- Silver/CHF (metals) -> Silver
- Silver/EUR (metals) -> Silver
- Silver/GBP (metals) -> Silver
- Silver/HKD (metals) -> Silver
- Silver/JPY (metals) -> Silver
- Silver/NZD (metals) -> Silver
- Silver/SGD (metals) -> Silver
- US Nas 100 (indices) -> NASDAQ / NQ
- US SPX 500 (indices) -> S&P 500 / ES
- US Wall St 30 (indices) -> Dow / YM
- West Texas Oil (commodities) -> Crude Oil / CL

### DERIVED (36)
- AUD/CAD (fx)
- AUD/CHF (fx)
- AUD/HKD (fx)
- AUD/JPY (fx)
- AUD/NZD (fx)
- AUD/SGD (fx)
- AUD/USD (fx)
- CAD/HKD (fx)
- CAD/SGD (fx)
- CHF/HKD (fx)
- EUR/AUD (fx)
- EUR/DKK (fx)
- EUR/HKD (fx)
- EUR/NOK (fx)
- EUR/NZD (fx)
- EUR/SEK (fx)
- EUR/SGD (fx)
- GBP/AUD (fx)
- GBP/HKD (fx)
- GBP/NZD (fx)
- GBP/SGD (fx)
- Gold/Silver (metals)
- HKD/JPY (fx)
- NZD/CAD (fx)
- NZD/CHF (fx)
- NZD/HKD (fx)
- NZD/JPY (fx)
- NZD/SGD (fx)
- NZD/USD (fx)
- SGD/CHF (fx)
- SGD/JPY (fx)
- USD/DKK (fx)
- USD/HKD (fx)
- USD/NOK (fx)
- USD/SEK (fx)
- USD/SGD (fx)

### MACRO_ONLY (0)

### NO_DATA (10)
- Brent Crude Oil (commodities)
- Bund (bonds)
- Germany 30 (indices)
- Japan 225 (indices)
- UK 100 (indices)
- UK 10Y Gilt (bonds)
- US 10Y T-Note (bonds)
- US 2Y T-Note (bonds)
- US Russ 2000 (indices)
- US T-Bond (bonds)

### ORPHANED (33)
- Australia 200 (indices)
- Bitcoin (crypto)
- Bitcoin Cash (crypto)
- CHF/ZAR (fx)
- China A50 (indices)
- EUR/CZK (fx)
- EUR/HUF (fx)
- EUR/TRY (fx)
- EUR/ZAR (fx)
- Ethereum/Ether (crypto)
- Europe 50 (indices)
- France 40 (indices)
- GBP/PLN (fx)
- GBP/ZAR (fx)
- Hong Kong 33 (indices)
- India 50 (indices)
- Litecoin (crypto)
- Netherlands 25 (indices)
- Singapore 30 (indices)
- TRY/JPY (fx)
- Taiwan Index (indices)
- US 5Y T-Note (bonds)
- USD/CNH (fx)
- USD/CZK (fx)
- USD/HUF (fx)
- USD/INR (fx)
- USD/MXN (fx)
- USD/PLN (fx)
- USD/SAR (fx)
- USD/THB (fx)
- USD/TRY (fx)
- USD/ZAR (fx)
- ZAR/JPY (fx)

## Task 3 + 4 — Canonical universe coverage

| Canonical | Class | Registry instrument | Price | Macro | COT | COT status | Missing |
|---|---|---|---|---|---|---|---|
| USD | Currencies | — | no | no | n/a | — | price_symbol, fred_macro_map, registry_instrument |
| EUR | Currencies | Euro FX / 6E | yes | no | yes | direct_cot | fred_macro_map |
| GBP | Currencies | British Pound / 6B | yes | no | yes | direct_cot | fred_macro_map |
| JPY | Currencies | Japanese Yen / 6J | yes | no | yes | direct_cot | fred_macro_map |
| CHF | Currencies | Swiss Franc / 6S | yes | no | yes | direct_cot | fred_macro_map |
| AUD | Currencies | Australian Dollar / 6A | yes | no | yes | direct_cot | fred_macro_map |
| NZD | Currencies | NZ Dollar / 6N | yes | no | yes | direct_cot | fred_macro_map |
| CAD | Currencies | Canadian Dollar / 6C | yes | no | yes | direct_cot | fred_macro_map |
| Gold | Metals | Gold | no | yes | yes | direct_cot | price_symbol |
| Silver | Metals | Silver | no | yes | yes | direct_cot | price_symbol |
| Copper | Metals | Copper / HG | no | yes | yes | direct_cot | price_symbol |
| Platinum | Metals | Platinum | no | no | yes | direct_cot | price_symbol, fred_macro_map |
| WTI | Energy | Crude Oil / CL | no | yes | yes | direct_cot | price_symbol |
| Brent | Energy | Brent Crude Oil | yes | no | no | no_cot_available | fred_macro_map, cot |
| Natural Gas | Energy | Natural Gas / NG | no | yes | yes | direct_cot | price_symbol |
| Wheat | Agriculture | Wheat | no | yes | yes | direct_cot | price_symbol |
| Corn | Agriculture | Corn | no | yes | yes | direct_cot | price_symbol |
| Soybeans | Agriculture | Soybeans | no | yes | yes | direct_cot | price_symbol |
| Coffee | Agriculture | Coffee | no | yes | yes | direct_cot | price_symbol |
| Cocoa | Agriculture | Cocoa | no | yes | yes | direct_cot | price_symbol |
| US 2Y | Rates | US 2Y T-Note | yes | no | n/a | no_cot_available | fred_macro_map |
| US 10Y | Rates | US 10Y T-Note | yes | no | n/a | no_cot_available | fred_macro_map |
| US 30Y | Rates | US T-Bond | yes | no | n/a | no_cot_available | fred_macro_map |
| Bund | Rates | Bund | yes | no | n/a | no_cot_available | fred_macro_map |
| Gilt | Rates | UK 10Y Gilt | yes | no | n/a | no_cot_available | fred_macro_map |
| SPX | Indices | S&P 500 / ES | yes | yes | yes | direct_cot | — |
| NDX | Indices | NASDAQ / NQ | yes | yes | yes | direct_cot | — |
| Dow | Indices | Dow / YM | yes | yes | yes | direct_cot | — |
| Russell | Indices | US Russ 2000 | yes | no | n/a | no_cot_available | fred_macro_map |
| DAX | Indices | Germany 30 | yes | no | n/a | no_cot_available | fred_macro_map |
| FTSE | Indices | UK 100 | yes | no | n/a | no_cot_available | fred_macro_map |
| Nikkei | Indices | Japan 225 | yes | no | n/a | no_cot_available | fred_macro_map |
