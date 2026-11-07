# Weekly COT publication

The live hostname is `https://institutional.ztack.co.uk`, Vercel project
`high-probability-trade-locator` (`prj_azYqGdff1iAUbl8MI0pMKaG2D3tK`).

## Release path

`Weekly COT update` runs Saturday 06:00 UTC and calls
`.github/workflows/production-data-release.yml`. The release also has an 08:00 UTC
Saturday retry and manual dispatch. Windows `run_weekly_cot_update.ps1` generates
and checks local data, then `publish_weekly_cot.ps1` dispatches the same cloud
release, waits for completion, and verifies the live report date. Local generation
alone is not publication.

The cloud release downloads CFTC data, rebuilds existing exports, requires the
26-market source integrity gate, verifies all Inspector market dates and participant
readings, audits historical range indices, builds the dashboard, deploys the
validated static output, attaches the live hostname, and checks public file hashes.
The 400 MB confluence archive is preserved as digest-checked static chunks.

`Publish dashboard interface` is serialized with the data release. It retains all
published data by digest, replaces only the UI, and checks the resulting files and
live Radar/Inspector readings. It requires a successful static data deployment as
its baseline. Do not replace it with a Git build using committed historical exports.

Git-triggered Vercel deployment is disabled in both repository-root and dashboard-root
`vercel.json`. Both are needed because the Vercel root directory has previously
been changed between the repository and `web-dashboard`.

## Canonical UI source

`web-dashboard/src/data/weeklyInspectorStore.js` loads and validates
`/data/cot_weekly_inspector_latest.json` once per page session. Radar, its week
selector, Weekly Inspector and Inspector lookback share this snapshot. Reloading
loads the latest release. The Radar uses `attentionForWeek` to copy published
participant values for the selected week; it does not recalculate their indices.
Incomplete or mixed-week reports fail closed. The confluence startup date is
checked for parity but is not the Radar header's data authority.

## Verification

From the repository root, after a release:

```sh
node web-dashboard/scripts/verifyLiveCot.mjs https://institutional.ztack.co.uk YYYY-MM-DD
```

Supply the COT report date, not the deployment date. A normal Saturday release uses
the preceding Tuesday. A delayed CFTC release must fail the freshness gate rather
than relabel old observations. The verification checks all 26 markets, all five
lookbacks, weekly changes, the attention snapshot and the startup date. Also open
the live Radar and a market's Weekly Inspector to confirm rendered dates and values.

## Incident: 7 November 2026

Weekly run `42349297508` generated 3 November data and passed 26/26 source checks,
but its final step only printed a note saying production belonged on Windows.
It did not deploy. Code-only Git deployments then served the older checked-in
snapshots: the Radar header showed 6 October and Inspector data ended 13 October.
The interface publisher returned HTTP 404 when asking Vercel for the source-file
tree of a Git deployment; that API only supports CLI/file-based deployments.

The repair routes the weekly schedule through publication, disables Git builds at
both potential roots, validates every market rather than only `max(date)`, and
makes the Radar header and readings share the Inspector source. No history was
reset or force-pushed and no existing source-data files were deleted.
