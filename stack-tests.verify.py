import subprocess, json, os, sys

W = os.path.dirname(os.path.abspath(__file__))
BASE = '179529ec9dd'

def git(*args):
    return subprocess.run(['git', '-C', W, '-c', 'safe.directory=*'] + list(args), capture_output=True, text=True)

def gitb(*args):
    return subprocess.run(['git', '-C', W, '-c', 'safe.directory=*'] + list(args), capture_output=True)

data = json.load(open(os.path.join(W, 'stack-tests.json')))
rows = {r['key']: r for r in data['commits']}
tracked = set(git('ls-files').stdout.splitlines())
lines = git('log', '--reverse', '--format=%H%x00%s', BASE + '..HEAD').stdout.strip().splitlines()
errors = []
for line in lines:
    sha, _, subject = line.partition('\x00')
    files = [f for f in git('show', '--name-only', '--format=', sha).stdout.splitlines() if f]
    if files and all(f in ('stack-tests.json', 'stack-tests.gen.py', 'stack-tests.verify.py') for f in files):
        continue
    diff = gitb('show', sha, '--format=').stdout
    pid = subprocess.run(['git', '-C', W, 'patch-id', '--stable'], input=diff, capture_output=True).stdout.decode().split()
    patch_id = pid[0] if pid else ''
    row = rows.get(subject)
    if not row:
        errors.append('missing row: ' + subject)
        continue
    if row.get('patch_id') != patch_id:
        errors.append('patch-id mismatch: ' + subject)
    for t in row.get('tests', []):
        if t not in tracked:
            errors.append('test not tracked: ' + t)
print('commits', len(lines), 'rows', len(rows), 'errors', len(errors))
for e in errors:
    print('ERROR', e)
sys.exit(1 if errors else 0)
