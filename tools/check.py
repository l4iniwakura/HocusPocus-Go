"""Run the complete local acceptance suite using isolated Redis/S3 containers."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import time
import tempfile
import urllib.request
import urllib.error
import uuid

ROOT = Path(__file__).resolve().parents[1]
REDIS_IMAGE = 'redis@sha256:21090af93f8324db2f0f0f4f38a7c60369d47d917e000c3423d89d2ae2883537'
S3_IMAGE = 'chrislusf/seaweedfs@sha256:4e61d15fd35994cb1e43e1e553dff106794841fd9a99ade2fc8c8bfce4d7872d'


def run(args, *, cwd=ROOT, env=None, capture=False):
    print('+ ' + ' '.join(map(str, args)), flush=True)
    return subprocess.run(args, cwd=cwd, env=env, text=True, check=True,
                          stdout=subprocess.PIPE if capture else None).stdout


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--race', action='store_true', help='also run Linux race tests in Docker')
    args = parser.parse_args()
    env = dict(os.environ, GOSUMDB='sum.golang.org https://sum.golang.org')
    go = shutil.which('go')
    npm = shutil.which('npm.cmd' if os.name == 'nt' else 'npm')
    if not go or not npm:
        raise SystemExit('Go 1.27.3 and Node/npm must be on PATH')
    artifact = ROOT / '.data'
    artifact.mkdir(exist_ok=True)
    sources = []
    for directory, directories, files in os.walk(ROOT):
        directories[:] = [name for name in directories if name not in {'.git', '.reference', '.data', 'node_modules', 'testdata'}]
        sources.extend(str(Path(directory) / name) for name in files if name.endswith('.go'))
    gofmt = str(Path(go).with_name('gofmt.exe' if os.name == 'nt' else 'gofmt'))
    for offset in range(0, len(sources), 32):
        unformatted = run([gofmt, '-l', *sources[offset:offset + 32]], capture=True).strip()
        if unformatted:
            raise SystemExit('Unformatted Go source:\n' + unformatted)
    for module in (ROOT,):
        run([go, 'mod', 'verify'], cwd=module, env=env)
        run([go, 'vet', './...'], cwd=module, env=env)
        run([go, 'test', '-timeout=180s', './...'], cwd=module, env=env)
    with tempfile.TemporaryDirectory(prefix='consumer-', dir=artifact) as temporary:
        consumer = Path(temporary)
        (consumer / 'go.mod').write_text('module example.com/hocuspocus-consumer\n\ngo 1.27.3\n\nrequire github.com/l4iniwakura/HocusPocus-Go v0.0.0\nreplace github.com/l4iniwakura/HocusPocus-Go => ' + json.dumps(ROOT.as_posix()) + '\n')
        (consumer / 'main.go').write_text('''package main
import("context"; hp "github.com/l4iniwakura/HocusPocus-Go"; "github.com/l4iniwakura/HocusPocus-Go/crdt")
func main(){s,e:=hp.New(hp.DefaultConfiguration());if e!=nil{panic(e)};c,e:=s.OpenDirect(context.Background(),"note",nil);if e!=nil{panic(e)};e=c.Transact(context.Background(),func(d *crdt.Doc)error{d.Transact(func(t *crdt.Transaction){t.GetText("body").Insert(t,0,"consumer",nil)});return nil});if e!=nil{panic(e)};if e=c.Disconnect(context.Background(),true);e!=nil{panic(e)};if e=s.Shutdown(context.Background());e!=nil{panic(e)}}
''')
        run([go, 'run', '-mod=mod', '.'], cwd=consumer, env=env)
    suffix = '.exe' if os.name == 'nt' else ''
    server = artifact / ('hocuspocus' + suffix)
    codec = artifact / ('hocuspocus-codec' + suffix)
    run([go, 'build', '-o', str(server), './cmd/hocuspocus'], env=env)
    run([go, 'build', '-o', str(codec), './cmd/hocuspocus-codec'], env=env)
    env.update(HOCUSPOCUS_GO_BINARY=str(server), HOCUSPOCUS_CODEC_BINARY=str(codec))
    run([npm, 'test'], env=env)
    prefix = 'hocuspocus-go-check-' + uuid.uuid4().hex[:10]
    containers = []
    try:
        for service, port, image, command in (
            ('redis', '6379', REDIS_IMAGE, ['redis-server', '--save', '', '--appendonly', 'no']),
            ('s3', '8333', S3_IMAGE, ['server', '-dir=/tmp', '-ip.bind=0.0.0.0',
                                    '-volume.max=100', '-master.volumeSizeLimitMB=32', '-s3', '-s3.port=8333']),
        ):
            name = prefix + '-' + service
            run(['docker', 'run', '-d', '--name', name, '-p', f'127.0.0.1::{port}', image, *command])
            containers.append(name)
            address = run(['docker', 'port', name, port + '/tcp'], capture=True).strip()
            env['TEST_REDIS_ADDR' if service == 'redis' else 'TEST_S3_ENDPOINT'] = address if service == 'redis' else 'http://' + address
        deadline = time.monotonic() + 30
        while True:
            try:
                urllib.request.urlopen(env['TEST_S3_ENDPOINT'], timeout=2).close()
                break
            except urllib.error.HTTPError:
                break  # An S3 error response proves that the HTTP listener is ready.
            except OSError:
                if time.monotonic() > deadline:
                    raise
                time.sleep(.2)
        run([go, 'test', '-tags=integration', '-timeout=90s', './extensions/redis', './extensions/s3'], env=env)
        run([npm, 'run', 'test:integration'], env=env)
        if args.race:
            # Linux is used because Go's Windows race detector needs an external C toolchain.
            image = 'hocuspocus-go-check:local'
            run(['docker', 'build', '-f', 'tools/check.Dockerfile', '-t', image, '.'])
            identity = hashlib.sha256(str(ROOT).encode()).hexdigest()[:10]
            module_cache = 'hocuspocus-go-' + identity + '-modules'
            build_cache = 'hocuspocus-go-' + identity + '-build'
            for volume in (module_cache, build_cache):
                run(['docker', 'volume', 'create', '--label', 'com.hocuspocus-go.role=validation-cache', volume])
            # Seed only modules named in this public project's go.sum. Avoid
            # sharing the operator's entire module cache with a container.
            source_cache = Path(run([go, 'env', 'GOMODCACHE'], capture=True).strip()) / 'cache/download'
            seed = artifact / 'linux-seed'
            seed.mkdir(exist_ok=True)
            for line in (ROOT / 'go.sum').read_text().splitlines():
                module, version, _ = line.split()
                version = version.removesuffix('/go.mod')
                escaped = ''.join('!' + c.lower() if c.isupper() else c for c in module)
                relative = Path(escaped) / '@v'
                target = seed / relative
                target.mkdir(parents=True, exist_ok=True)
                for ending in ('.mod', '.info', '.zip', '.ziphash'):
                    source = source_cache / relative / (version + ending)
                    if source.is_file():
                        destination = target / source.name
                        if not destination.exists():
                            shutil.copyfile(source, destination)
            cache_mounts = ['-v', str(seed) + ':/seed:ro', '-v', module_cache + ':/go/pkg/mod']
            run(['docker', 'run', '--rm', *cache_mounts, image, 'mkdir', '-p', '/go/pkg/mod/cache/download'])
            run(['docker', 'run', '--rm', *cache_mounts, image, 'cp', '-a', '/seed/.', '/go/pkg/mod/cache/download/'])
            mounts = ['-v', str(ROOT) + ':/src', '-v', module_cache + ':/go/pkg/mod',
                      '-v', build_cache + ':/root/.cache/go-build']
            run(['docker', 'run', '--rm', *mounts, '-w', '/src', image,
                 'go', 'test', '-race', '-timeout=180s', './...'])
    finally:
        for name in reversed(containers):
            subprocess.run(['docker', 'rm', '-f', name], check=False, stdout=subprocess.DEVNULL)
    print('All requested checks passed.', flush=True)


if __name__ == '__main__':
    main()
