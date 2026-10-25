"""Attach the public Institutional Edge hostname to the validated release."""
import json
import os
import urllib.error
import urllib.parse
import urllib.request

DOMAIN = 'institutional.ztack.co.uk'


def api(path, method='GET', body=None):
    team = urllib.parse.quote(os.environ['VERCEL_ORG_ID'], safe='')
    request = urllib.request.Request(
        'https://api.vercel.com' + path + '?teamId=' + team,
        data=None if body is None else json.dumps(body).encode(), method=method,
        headers={'Authorization': 'Bearer ' + os.environ['VERCEL_TOKEN'], 'Content-Type': 'application/json'})
    with urllib.request.urlopen(request, timeout=40) as response:
        return json.load(response)


def connect(deployment):
    # Assign the already-owned hostname directly. The release credential can
    # deploy this project but cannot inspect/migrate the legacy project's domain.
    # Explicit assignment on every release avoids relying on automatic promotion.
    api(f'/v2/deployments/{deployment}/aliases', 'POST', {'alias': DOMAIN})
    alias = api(f'/v4/aliases/{DOMAIN}')
    if alias.get('deploymentId') != deployment or alias.get('projectId') != os.environ['VERCEL_PROJECT_ID']:
        raise RuntimeError('Live hostname does not point to the released deployment/project')
    print(f'Production hostname verified: {DOMAIN} -> {deployment}')


if __name__ == '__main__':
    import sys
    connect(sys.argv[1])
