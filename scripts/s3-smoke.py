#!/usr/bin/env python3
"""Exercise real S3 clients against an isolated local MaxStore instance."""
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import time
import urllib.request

ROOT = Path(__file__).resolve().parents[1]

def main():
    for name in ('aws', 'rclone', 'go'):
        if not shutil.which(name):
            raise SystemExit(f'Required executable not found: {name}')
    with tempfile.TemporaryDirectory(prefix='maxstore-s3-smoke-') as tmp:
        work = Path(tmp)
        binary = work / 'maxstore'
        subprocess.run(['go', 'build', '-o', str(binary), './cmd/maxstore'], cwd=ROOT, check=True)
        env = os.environ.copy()
        # The smoke test must never inherit remote providers or production keys.
        for key in list(env):
            if key.startswith(('TELEGRAM_', 'MAX_', 'S3_', 'ADMIN_', 'AWS_')) or key in ('CONSOLE_TOKEN', 'COOKIE_SECURE'):
                del env[key]
        env.update(DATA_DIR=str(work / 'data'), HTTP_ADDR='127.0.0.1:18080', S3_ADDR='127.0.0.1:19000', DEFAULT_PROVIDER='local', MAX_OBJECT_SIZE='0')
        with (work / 'server.log').open('w') as log:
            server = subprocess.Popen([str(binary)], cwd=ROOT, env=env, stdout=log, stderr=log)
            try:
                for _ in range(100):
                    if server.poll() is not None:
                        raise RuntimeError('Isolated server failed to start; check ports 18080/19000')
                    try:
                        urllib.request.urlopen('http://127.0.0.1:18080/healthz', timeout=1).close()
                        break
                    except OSError:
                        time.sleep(.1)
                else:
                    raise RuntimeError('Startup timeout')
                creds = json.loads((work / 'data/credentials.json').read_text())
                config = work / 'aws-config'
                config.write_text('[default]\nregion = us-east-1\ns3 =\n    addressing_style = path\n    max_concurrent_requests = 4\n    multipart_threshold = 8MB\n    multipart_chunksize = 8MB\n')
                env.update(AWS_ACCESS_KEY_ID=creds['s3_access_key'], AWS_SECRET_ACCESS_KEY=creds['s3_secret_key'], AWS_DEFAULT_REGION='us-east-1', AWS_REQUEST_CHECKSUM_CALCULATION='when_required', AWS_RESPONSE_CHECKSUM_VALIDATION='when_required', AWS_CONFIG_FILE=str(config), AWS_SHARED_CREDENTIALS_FILE=str(work / 'unused-credentials'), AWS_EC2_METADATA_DISABLED='true')
                def aws(*args):
                    return subprocess.run(['aws', '--endpoint-url', 'http://127.0.0.1:19000', *args], env=env, check=True, capture_output=True).stdout
                source = work / 'source.bin'
                source.write_bytes(os.urandom(24 * 1024 * 1024 + 257))
                digest = hashlib.sha256(source.read_bytes()).hexdigest()
                aws('s3', 'mb', 's3://smoke-bucket')
                aws('s3', 'cp', str(source), 's3://smoke-bucket/source.bin', '--only-show-errors')
                aws('s3', 'cp', 's3://smoke-bucket/source.bin', str(work / 'restored.bin'), '--only-show-errors')
                assert hashlib.sha256((work / 'restored.bin').read_bytes()).hexdigest() == digest
                aws('s3', 'cp', 's3://smoke-bucket/source.bin', 's3://smoke-bucket/copy.bin', '--only-show-errors')
                aws('s3api', 'head-object', '--bucket', 'smoke-bucket', '--key', 'copy.bin')
                rc = work / 'rclone.conf'
                rc.write_text('[maxstore]\ntype = s3\nprovider = Other\naccess_key_id = '+creds['s3_access_key']+'\nsecret_access_key = '+creds['s3_secret_key']+'\nregion = us-east-1\nendpoint = http://127.0.0.1:19000\nforce_path_style = true\nno_check_bucket = true\n')
                rc.chmod(0o600)
                def rclone(*args):
                    subprocess.run(['rclone', '--config', str(rc), *args], env=env, check=True, capture_output=True)
                rclone('copyto', str(source), 'maxstore:smoke-bucket/rclone.bin', '--s3-upload-cutoff', '8M', '--s3-chunk-size', '8M', '--s3-upload-concurrency', '4')
                rclone('copyto', 'maxstore:smoke-bucket/rclone.bin', str(work / 'rclone-restored.bin'))
                assert hashlib.sha256((work / 'rclone-restored.bin').read_bytes()).hexdigest() == digest
                rclone('moveto', 'maxstore:smoke-bucket/rclone.bin', 'maxstore:smoke-bucket/moved.bin')
                rclone('lsf', 'maxstore:smoke-bucket')
                rclone('purge', 'maxstore:smoke-bucket')
                print('PASS: AWS CLI multipart upload/download/copy; rclone multipart upload/download/move/list/purge; SHA-256 matches (24 MiB + 257 bytes).')
            finally:
                server.terminate()
                server.wait(timeout=25)

if __name__ == '__main__':
    main()
