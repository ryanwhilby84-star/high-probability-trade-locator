"""Attach the public Institutional Edge hostname to the validated release."""
import json
import os
import urllib.error
import urllib.parse
import urllib.request

DOMAIN = 'institutional.ztack.co.uk'
LEGACY_PROJECT = 'prj_lsPZn7hxVBmhQE7MObnBe9Y62xAt'


def api(path, method='GET', body=None):
    team = urllib.parse.quote(os.environ['VERCEL_ORG_ID'], safe='')
    request = urllib.request.Request(
        'https://api.vercel.com' + path + '?teamId=' + team,
        data=None if body is None else json.dumps(body).encode(), method=method,
        headers={'Authorization': 'Bearer ' + os.environ['VERCEL_TOKEN'], 'Content-Type': 'application/json'})
    with urllib.request.urlopen(request, timeout=40) as response:
        return json.load(response)


def connect(deployment):
    project = urllib.parse.quote(os.environ['VERCEL_PROJECT_ID'], safe='')
    # Register on the pipeline's project so future releases inherit the hostname.
    try:
        api(f'/v9/projects/{project}/domains/{DOMAIN}')
    except urllib.error.HTTPError as error:
        if error.code != 404:
            raise
        try:
            api(f'/v10/projects/{project}/domains', 'POST', {'name': DOMAIN})
        except urllib.error.HTTPError as conflict:
            if conflict.code != 409:
                raise
            # One-time migration from the inspected legacy preview project only.
            old = api(f'/v9/projects/{LEGACY_PROJECT}/domains/{DOMAIN}')
            if old.get('name') != DOMAIN:
                raise RuntimeError('Unexpected legacy domain; refusing reassignment')
            api(f'/v9/projects/{LEGACY_PROJECT}/domains/{DOMAIN}', 'DELETE')
            api(f'/v10/projects/{project}/domains', 'POST', {'name': DOMAIN})
    api(f'/v2/deployments/{deployment}/aliases', 'POST', {'alias': DOMAIN})
    alias = api(f'/v4/aliases/{DOMAIN}')
    if alias.get('deploymentId') != deployment or alias.get('projectId') != os.environ['VERCEL_PROJECT_ID']:
        raise RuntimeError('Live hostname does not point to the released deployment/project')
    print(f'Production hostname verified: {DOMAIN} -> {deployment}')


if __name__ == '__main__':
    import sys
    connect(sys.argv[1])
