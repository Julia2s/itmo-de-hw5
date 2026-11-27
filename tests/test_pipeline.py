"""Offline correctness tests; no Docker, Kubernetes or real S3 is contacted."""
import contextlib
import copy
import csv
import io
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / "src"), str(ROOT / "scripts")]
os.environ["VARIANT_FILTER_MODE"] = "role"

import common
from common import LocalFSClient, VACANCY_FIELDS
from partition import partition
from reduce import reduce_parts
from runtime import calculate, get_json, iter_range_chunks, memory_metrics, put_json, validate_manifest
from worker import run
from mapreduce_v2 import decode_lines, iter_byte_chunks, map_chunk
from collect_results import collect
from prepare_scheduling import generate, pod_request
from set_memory import tune
from verify_results import verify
import yaml


def row(skills="Linux;Docker;Kubernetes", role="DevOps-инженер", description="тест", **extra):
    fields = dict.fromkeys(VACANCY_FIELDS, "")
    fields.update(source="hh", vacancy_id="1", role=role, key_skills=skills,
                  description=description, **extra)
    stream = io.StringIO()
    csv.writer(stream, lineterminator="\n").writerow([fields[f] for f in VACANCY_FIELDS])
    return stream.getvalue().encode()


class PipelineTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.patch = patch.object(common, "LOCAL_FS_ROOT", str(self.root / "s3"))
        self.patch.start()
        self.addCleanup(self.patch.stop)
        self.cli = LocalFSClient()
        self.data = (row(description="Ж" * 3000) + row(skills="linux;Linux;DOCKER") +
                     row(role="Программист", skills="Java") + row(skills="Ansible;Linux") +
                     b"bad,row\n\n" + row(skills="Python;Linux;Docker").rstrip(b"\n"))
        self.cli.put_object("raw/test.csv", self.data)

    def quiet_run(self, **kwargs):
        with contextlib.redirect_stdout(io.StringIO()):
            return run(self.cli, output=str(self.root / "worker"), **kwargs)

    def quiet_reduce(self, prefix="test/parallel"):
        with contextlib.redirect_stdout(io.StringIO()):
            return reduce_parts(self.cli, prefix, str(self.root / "final"))

    def test_chunk_reader_matches_original_even_inside_utf8_and_long_lines(self):
        expected = map_chunk(decode_lines(self.data))
        counts, stats = calculate(self.cli, "raw/test.csv", 0, len(self.data), chunk_bytes=7)
        self.assertEqual(counts, expected[0])
        self.assertEqual(stats["lines_total"], expected[1]["lines_total"])
        original = list(iter_byte_chunks(self.cli, "raw/test.csv", len(self.data), 7))
        self.assertEqual(b"".join(d for d, _ in original), self.data)

    def test_full_pipeline_exact_bytes_and_idempotent_retry(self):
        baseline = self.quiet_run(key="raw/test.csv", prefix="test/baseline", chunk_bytes=11)
        manifest = partition(self.cli, "raw/test.csv", 4, "test/parallel")
        for part in manifest["parts"]:
            if 0 < part["start"] < len(self.data):
                self.assertEqual(self.data[part["start"] - 1:part["start"]], b"\n")
            self.quiet_run(mode="part", prefix="test/parallel", index=part["index"], chunk_bytes=13)
        self.quiet_run(mode="part", prefix="test/parallel", index=0, chunk_bytes=17)
        result = self.quiet_reduce()
        self.assertEqual(baseline["stats"], result["stats"])
        self.assertEqual(baseline["sha256"], result["sha256"])
        with contextlib.redirect_stdout(io.StringIO()):
            collect(self.cli, "test/parallel/final", self.root / "download")
        self.assertTrue(verify(self.root / "final", self.root / "download")["ok"])
        self.assertEqual(result["stats"]["lines_passed_prefilter"], 4)

    def test_empty_and_single_line_inputs_have_no_duplicate_lines(self):
        for data in (b"", row().rstrip(b"\n"), row()):
            self.cli.put_object("raw/edge.csv", data)
            manifest = partition(self.cli, "raw/edge.csv", 12, "test/edge")
            line_count = 0
            for part in manifest["parts"]:
                _, stats = calculate(self.cli, "raw/edge.csv", part["start"], part["end"], 5)
                line_count += stats["lines_total"]
            self.assertEqual(line_count, 1 if data else 0)

    def test_reducer_rejects_missing_stale_and_corrupt_parts(self):
        partition(self.cli, "raw/test.csv", 2, "test/parallel")
        self.quiet_run(mode="part", prefix="test/parallel", index=0)
        with self.assertRaises(FileNotFoundError):
            self.quiet_reduce()
        self.quiet_run(mode="part", prefix="test/parallel", index=1)
        key = "test/parallel/parts/0/skills_2.csv"
        data = self.cli.get_object(key)
        self.cli.put_object(key, data + b"corrupt")
        with self.assertRaisesRegex(ValueError, "corrupt"):
            self.quiet_reduce()
        self.cli.put_object(key, data)
        partition(self.cli, "raw/test.csv", 2, "test/parallel")
        with self.assertRaisesRegex(ValueError, "Stale"):
            self.quiet_reduce()

    def test_changed_input_and_manifest_gap_rejected(self):
        manifest = partition(self.cli, "raw/test.csv", 4, "test/parallel")
        self.cli.put_object("raw/test.csv", self.data + b"x")
        with self.assertRaisesRegex(ValueError, "changed"):
            self.quiet_run(mode="part", prefix="test/parallel", index=0)
        manifest["parts"][1]["start"] += 1
        with self.assertRaises(ValueError):
            validate_manifest(manifest)

    def test_new_partition_run_invalidates_old_final(self):
        partition(self.cli, "raw/test.csv", 1, "test/parallel")
        self.quiet_run(mode="part", prefix="test/parallel", index=0)
        self.quiet_reduce()
        partition(self.cli, "raw/test.csv", 1, "test/parallel")
        with self.assertRaisesRegex(ValueError, "older"):
            collect(self.cli, "test/parallel/final", self.root / "download")

    def test_short_read_fails_instead_of_skipping_data(self):
        with patch.object(self.cli, "get_object", return_value=b""):
            with self.assertRaises(IOError):
                list(iter_range_chunks(self.cli, "raw/test.csv", 0, 10))


class UtilityTests(unittest.TestCase):
    def test_memory_limit_derived_from_peak_and_new_job(self):
        template = yaml.safe_load((ROOT / "k8s/job-baseline.yaml").read_text())
        job, peak, limit = tune("WORKER_COMPLETE\nMEMORY_PEAK_MIB=136.61", copy.deepcopy(template), "skills-tuned")
        self.assertEqual(limit, 192)
        self.assertEqual(peak, 136.61)
        worker = job["spec"]["template"]["spec"]["containers"][0]
        self.assertEqual(worker["resources"]["limits"]["memory"], "192Mi")
        self.assertIn("hw5/tuned", worker["args"])
        with self.assertRaises(ValueError):
            tune("MEMORY_PEAK_MIB=100", copy.deepcopy(template), "skills-tuned")
        with self.assertRaises(ValueError):
            tune("WORKER_COMPLETE\nMEMORY_PEAK_MIB=100", copy.deepcopy(template), "skills-baseline")

    def test_cgroup_memory_peak_resolves_non_root_path(self):
        files = {
            "/proc/self/cgroup": "0::/jobs/worker\n",
            "/proc/self/mountinfo": "1 2 0:3 / /sys/fs/cgroup rw - cgroup2 cgroup rw\n",
            "/sys/fs/cgroup/jobs/worker/memory.peak": "104857600\n",
            "/sys/fs/cgroup/jobs/worker/memory.max": "1073741824\n",
        }
        with patch.object(Path, "read_text", lambda p, **_: files[str(p).replace('\\', '/')]), \
             patch.object(Path, "exists", lambda p: str(p).replace('\\', '/') in files):
            metrics = memory_metrics()
        self.assertEqual(metrics["source"], "cgroup_v2")
        self.assertEqual(metrics["peak_mib"], 100)

    def test_pod_requests_include_init_sidecars_and_overhead(self):
        spec = {"containers": [{"resources": {"requests": {"cpu": "200m"}}}],
                "initContainers": [
                    {"restartPolicy": "Always", "resources": {"requests": {"cpu": "100m"}}},
                    {"resources": {"requests": {"cpu": "700m"}}}],
                "overhead": {"cpu": "50m"}}
        self.assertEqual(float(pod_request(spec, "cpu")), .85)

    def test_scheduling_manifests_force_contention_without_node_name(self):
        nodes = {"items": [{"metadata": {"name": "node", "labels": {"kubernetes.io/hostname": "node"}},
                            "status": {"allocatable": {"cpu": "4", "memory": "8Gi", "pods": "110"},
                                       "conditions": [{"type": "Ready", "status": "True"}]}}]}
        pods = {"items": [{"metadata": {"name": "system"}, "status": {"phase": "Running"},
                           "spec": {"nodeName": "node", "containers": [
                               {"resources": {"requests": {"cpu": "500m", "memory": "128Mi"}}}]}}]}
        manifests, budget = generate(nodes, pods, "hw5")
        request = manifests["cpu-map.yaml"]["spec"]["template"]["spec"]["containers"][0]["resources"]["requests"]["cpu"]
        self.assertEqual(request, "2100m")
        self.assertIn("two_fit=False", budget)
        high = manifests["urgent.yaml"]["spec"]["template"]["spec"]
        self.assertNotIn("nodeName", high)
        self.assertEqual(high["priorityClassName"], "hw5-high")
        self.assertIn('src/worker.py', high['containers'][0]['command'])
        self.assertEqual(high['containers'][0]['env'][0]['value'], 'http://rustfs.hw5.svc.cluster.local:9000')
        self.assertEqual(manifests['low-map.yaml']['spec']['podFailurePolicy']['rules'][0]['action'], 'Ignore')
        self.assertEqual(manifests['quota-map.yaml']['metadata']['namespace'], 'hw5-quota')
        self.assertEqual(manifests['quota-map.yaml']['spec']['template']['spec']['containers'][0]['resources']['requests']['cpu'], '600m')
        priorities = list(yaml.safe_load_all((ROOT / "k8s/priorities.yaml").read_text()))
        self.assertEqual([p["value"] for p in priorities], [-1000, -500])
        pods["items"][0]["spec"]["priority"] = -700
        with self.assertRaisesRegex(ValueError, "priority below"):
            generate(nodes, pods, "hw5")

    def test_mismatch_verifier_returns_failure(self):
        with tempfile.TemporaryDirectory() as folder:
            target = Path(folder)
            for k in (1, 2, 3):
                source = ROOT / f"reference/hw3/results_v2/skills_{k}.csv"
                (target / source.name).write_bytes(source.read_bytes())
            path = target / "skills_2.csv"
            path.write_bytes(path.read_bytes().replace(b",1251\n", b",1252\n", 1))
            self.assertFalse(verify(ROOT / "reference/hw3/results_v2", target)["ok"])

    def test_job_templates_have_safe_lifecycle_and_consistent_env(self):
        for path in (ROOT / "k8s").glob("job-*.yaml"):
            job = yaml.safe_load(path.read_text())
            self.assertEqual(job["spec"]["backoffLimit"], 0)
            self.assertNotIn("ttlSecondsAfterFinished", job["spec"])
            spec = job["spec"]["template"]["spec"]
            self.assertEqual(spec["restartPolicy"], "Never")
            self.assertEqual(spec["containers"][0]["imagePullPolicy"], "Never")
        parallel = yaml.safe_load((ROOT / "k8s/job-parallel.yaml").read_text())
        self.assertEqual(parallel["spec"]["completions"], 4)
        self.assertEqual(parallel["spec"]["parallelism"], 2)


if __name__ == "__main__":
    unittest.main()
