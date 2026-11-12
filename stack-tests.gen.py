import subprocess, json, os, re

W = os.path.dirname(os.path.abspath(__file__))
BASE = '179529ec9dd'

OVERRIDES = {
    'src/agents/failover-error.ts': ['src/agents/failover-error.test.ts'],
    'packages/agent-core/src/agent.ts': ['packages/agent-core/src/agent.listener-stale.test.ts'],
    'ui/src/lib/config/config-draft-model.ts': ['ui/src/lib/config/config-draft-model.test.ts'],
    'src/context-engine/host-compat.ts': ['src/context-engine/host-compat.test.ts'],
}

def git(*args):
    return subprocess.run(['git', '-C', W, '-c', 'safe.directory=*'] + list(args), capture_output=True, text=True)

def gitb(*args):
    return subprocess.run(['git', '-C', W, '-c', 'safe.directory=*'] + list(args), capture_output=True)

lines = git('log', '--reverse', '--format=%H%x00%s', BASE + '..HEAD').stdout.strip().splitlines()
tracked = set(git('ls-files').stdout.splitlines())
rows = []
for line in lines:
    sha, _, subject = line.partition('\x00')
    files = [f for f in git('show', '--name-only', '--format=', sha).stdout.splitlines() if f]
    tests = set()
    for f in files:
        if f in OVERRIDES:
            tests.update(OVERRIDES[f])
            continue
        stem = re.sub(r'\\.(ts|tsx|js|jsx|json)$', '', f)
        cand = stem + '.test.ts'
        if cand in tracked:
            tests.add(cand)
    diff = gitb('show', sha, '--format=').stdout
    pid = subprocess.run(['git', '-C', W, 'patch-id', '--stable'], input=diff, capture_output=True).stdout.decode().split()
    rows.append({'key': subject, 'patch_id': pid[0] if pid else '', 'files': files, 'tests': sorted(tests)})
out = {'base': BASE, 'keyed_by': 'subject+patch-id', 'commits': rows}
with open(os.path.join(W, 'stack-tests.json'), 'w') as fh:
    json.dump(out, fh, indent=1)
print('rows', len(rows), 'with tests', sum(1 for r in rows if r['tests']), 'unique keys', len({r['key'] for r in rows}))
