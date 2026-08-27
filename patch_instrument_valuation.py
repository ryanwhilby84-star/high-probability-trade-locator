from pathlib import Path

p = Path("web-dashboard/src/pages/InstrumentPage.jsx")
s = p.read_text(encoding="utf-8")

if "ValuationInstrumentSection" not in s:
    s = s.replace(
        "import { CotUnavailablePanel } from '../components/CotUnavailablePanel.jsx'",
        "import { CotUnavailablePanel } from '../components/CotUnavailablePanel.jsx'\n"
        "import { ValuationInstrumentSection } from '../components/IVECalculationPanel.jsx'",
    )

s = s.replace(
    "<InstrumentWorkstationLayout>",
    "<InstrumentWorkstationLayout>\n"
    "        <ValuationInstrumentSection row={row} />",
    1,
)

p.write_text(s, encoding="utf-8")
print("patched InstrumentPage.jsx")