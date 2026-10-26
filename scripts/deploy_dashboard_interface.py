"""Publish compiled UI using Vercel's existing content-addressed data files."""
import hashlib
import json
import os
from pathlib import Path
import time
import urllib.parse
import urllib.request

HOST = "institutional.ztack.co.uk"
TEAM = urllib.parse.quote(os.environ["VERCEL_ORG_ID"], safe="")
PROJECT = os.environ["VERCEL_PROJECT_ID"]
TOKEN = os.environ["VERCEL_TOKEN"]
BASE = "https://api.vercel.com"
HEADERS = {"Authorization": "Bearer " + TOKEN}


def api(path, payload=None, method=None):
    data = None if payload is None else json.dumps(payload).encode()
    req = urllib.request.Request(BASE + path + ("&" if "?" in path else "?") + "teamId=" + TEAM,
        data=data, headers={**HEADERS, "Content-Type": "application/json"}, method=method)
    with urllib.request.urlopen(req, timeout=60) as response:
        return json.load(response)


def flatten(nodes, prefix=""):
    """The deployment source tree has a virtual src root; it is not a file path."""
    for node in nodes:
        name = node["name"]
        path = prefix + name
        if node["type"] == "directory":
            yield from flatten(node.get("children", []), "" if not prefix and name == "src" else path + "/")
        else:
            yield {"file": path, "sha": node["uid"]}


def main():
    alias = api("/v4/aliases/" + HOST)
    if alias.get("projectId") != PROJECT:
        raise SystemExit("Production hostname belongs to another project")
    original = alias.get("deploymentId") or alias["deployment"]["id"]
    files = list(flatten(api("/v6/deployments/" + original + "/files")))
    by_path = {entry["file"]: entry for entry in files}
    required = ("confluence_bootstrap_latest.json", "confluence_history_manifest.json",
                "cot_weekly_inspector_latest.json", "cot_attention_80_20_latest.json")
    for name in required:
        if "static/data/" + name not in by_path:
            raise SystemExit("Production source missing required data: " + name)
    # Retain every published data and public asset by digest. Only compiled UI is replaced.
    retained = [entry for entry in files if entry["file"] != "static/index.html"
                and not entry["file"].startswith("static/assets/")]
    data_before = {entry["file"]: entry["sha"] for entry in files if entry["file"].startswith("static/data/")}
    root = Path("web-dashboard/dist-ui")
    html = (root / "index.html").read_text()
    if "/assets/" not in html:
        raise SystemExit("Compiled app entry is missing")
    new_files = []
    for path in sorted(root.rglob("*")):
        if not path.is_file():
            continue
        data = path.read_bytes()
        sha = hashlib.sha1(data).hexdigest()
        req = urllib.request.Request(BASE + "/v2/files?teamId=" + TEAM, data=data, method="POST",
            headers={**HEADERS, "Content-Type": "application/octet-stream",
                     "Content-Length": str(len(data)), "x-vercel-digest": sha})
        for attempt in range(3):
            try:
                with urllib.request.urlopen(req, timeout=60):
                    pass
                break
            except Exception:
                if attempt == 2:
                    raise
                time.sleep(2)
        new_files.append({"file": "static/" + path.relative_to(root).as_posix(),
                          "sha": sha, "size": len(data)})
    print("Reusing", len(data_before), "data files; uploading", len(new_files), "interface files", flush=True)
    # Check again before publication so a data release cannot be replaced with an older snapshot.
    current = api("/v4/aliases/" + HOST)
    if (current.get("deploymentId") or current["deployment"]["id"]) != original:
        raise SystemExit("Production changed during build; rerun against its new data")
    result = api("/v13/deployments", {
        "name": "high-probability-trade-locator", "project": PROJECT, "target": "production",
        "files": retained + new_files, "version": 2,
        "projectSettings": {"framework": None, "rootDirectory": None, "buildCommand": "echo prebuilt",
                            "installCommand": "echo prebuilt", "outputDirectory": "static"},
        "meta": {"hptl_interface_sha": os.environ.get("GITHUB_SHA", ""), "hptl_data_deployment": original}
    })
    deployment = result["id"]
    print("Deployment:", deployment, result["url"], flush=True)
    for _ in range(60):
        status = api("/v13/deployments/" + deployment)
        state = status.get("readyState")
        if state == "READY":
            break
        if state in ("ERROR", "CANCELED"):
            raise SystemExit("Deployment failed: " + str(status.get("errorMessage") or state))
        time.sleep(5)
    else:
        raise SystemExit("Deployment did not become ready")
    published = list(flatten(api("/v6/deployments/" + deployment + "/files")))
    data_after = {entry["file"]: entry["sha"] for entry in published if entry["file"].startswith("static/data/")}
    if data_before != data_after:
        raise SystemExit("Published data digests differ")
    from connect_production_domain import connect
    connect(deployment)
    # Confirm the public domain serves this exact interface, not a cached earlier release.
    url = "https://" + HOST + "/?verify=" + str(time.time_ns())
    with urllib.request.urlopen(url, timeout=30) as response:
        live = response.read()
    if live != (root / "index.html").read_bytes():
        raise SystemExit("Live app entry does not match this build")
    for path in root.glob("assets/*"):
        with urllib.request.urlopen("https://" + HOST + "/assets/" + path.name, timeout=30) as response:
            actual = response.read()
        if hashlib.sha256(actual).digest() != hashlib.sha256(path.read_bytes()).digest():
            raise SystemExit("Live asset differs: " + path.name)
    print("Verified live interface and unchanged production data:", "https://" + HOST, flush=True)


if __name__ == "__main__":
    main()
