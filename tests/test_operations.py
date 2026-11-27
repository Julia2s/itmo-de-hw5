"""Tests for migration, repeated measurements, retries and process termination."""
import hashlib
import io
import json
import os
import signal
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / 'src'), str(ROOT / 'scripts')]
from storage import migrate
from set_memory import measured_peaks, read_log
from prepare_job import make_job
from manifests import load_template, worker_of


class FakeS3:
    def __init__(self, data=b''):
        self.data = data
        self.version = '1'

    def head_object(self, **kwargs):
        return {'ContentLength': len(self.data), 'ETag': hashlib.md5(self.data).hexdigest(), 'VersionId': self.version}

    def get_object(self, **kwargs):
        return {'Body': io.BytesIO(self.data)}

    def upload_fileobj(self, reader, bucket, key, **kwargs):
        chunks = []
        while chunk := reader.read(3):
            chunks.append(chunk)
        self.data = b''.join(chunks)


class MigrationTests(unittest.TestCase):
    def test_migration_verifies_full_bytes_not_multipart_etag(self):
        source, target = FakeS3('Навыки\nLinux;Docker\n'.encode()), FakeS3()
        result = migrate(source, target, 'test', 'raw.csv')
        self.assertEqual(result['source_sha256'], hashlib.sha256(source.data).hexdigest())
        self.assertEqual(source.data, target.data)
        self.assertTrue(result['ok'])

    def test_corrupt_copy_is_rejected(self):
        class Corrupt(FakeS3):
            def upload_fileobj(self, *args, **kwargs):
                super().upload_fileobj(*args, **kwargs)
                self.data += b'corrupt'
        with self.assertRaisesRegex(ValueError, 'mismatch'):
            migrate(FakeS3(b'hello'), Corrupt(), 'test', 'raw.csv')

    def test_source_modified_during_copy_is_rejected(self):
        source = FakeS3(b'hello')
        class Mutating(FakeS3):
            def upload_fileobj(self, *args, **kwargs):
                super().upload_fileobj(*args, **kwargs)
                source.version = '2'
        with self.assertRaisesRegex(ValueError, 'changed'):
            migrate(source, Mutating(), 'test', 'raw.csv')


class MeasurementTests(unittest.TestCase):
    def test_three_successful_measurements_and_utf16_powershell_log(self):
        self.assertEqual(measured_peaks(['WORKER_COMPLETE\nMEMORY_PEAK_MIB=100',
                                        'WORKER_COMPLETE\nMEMORY_PEAK_MIB=120',
                                        'WORKER_COMPLETE\nMEMORY_PEAK_MIB=110']), [100, 120, 110])
        with self.assertRaises(ValueError):
            measured_peaks(['WORKER_COMPLETE\nMEMORY_PEAK_MIB=100', 'MEMORY_PEAK_MIB=120'])
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / 'test.log'
            path.write_text('WORKER_COMPLETE\nMEMORY_PEAK_MIB=123', encoding='utf-16')
            self.assertEqual(measured_peaks([read_log(path)]), [123])

    def test_memory_job_override_does_not_change_template(self):
        template = load_template('job-baseline.yaml')
        generated = make_job(template, 'measure-1', 'hw5/measure-1', '1Gi', 'http://example:9000')
        self.assertEqual(worker_of(generated)['env'][0]['value'], 'http://example:9000')
        self.assertEqual(worker_of(template)['env'][0]['value'], 'http://host.docker.internal:9010')
        self.assertEqual(generated['metadata']['name'], 'measure-1')
        self.assertEqual(worker_of(generated)['resources']['requests']['memory'], '1Gi')

    @unittest.skipUnless(os.name == 'posix', 'SIGTERM exit semantics are tested inside the Linux image')
    def test_worker_handles_sigterm_without_committing_partial_result(self):
        with tempfile.TemporaryDirectory() as folder:
            bucket = Path(folder) / 'test' / 'raw'
            bucket.mkdir(parents=True)
            (bucket / 'input.csv').write_bytes(b'bad,row\n')
            env = dict(os.environ, S3_BACKEND='local', LOCAL_FS_ROOT=folder, S3_BUCKET='test',
                       VACANCIES_KEY='raw/input.csv', PYTHONUNBUFFERED='1')
            child = subprocess.Popen([sys.executable, str(ROOT / 'src/worker.py'), '--prefix', 'test/run',
                                      '--output', str(Path(folder) / 'results'), '--pace-seconds', '30'],
                                     env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
            try:
                # Start marker occurs after handler installation and before data processing.
                while line := child.stdout.readline():
                    if '"phase": "start"' in line:
                        break
                child.send_signal(signal.SIGTERM)
                output, _ = child.communicate(timeout=10)
                self.assertEqual(child.returncode, 143, output)
                self.assertIn('TERMINATED_BY_SIGNAL=SIGTERM', output)
                self.assertFalse((Path(folder) / 'test/test/run/summary.json').exists())
            finally:
                if child.poll() is None:
                    child.kill()
                    child.communicate()


if __name__ == '__main__':
    unittest.main()
