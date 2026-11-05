"""Run C++ jobs plus unchanged service/step clients with exact native evidence."""

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import shlex
import shutil
import subprocess
import sys


def digest(path):
    with path.open("rb") as source:
        return hashlib.file_digest(source, "sha256").hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--library", type=Path, required=True)
    parser.add_argument("--library-sha256", required=True)
    parser.add_argument("--native-receipt", type=Path, required=True)
    parser.add_argument("--native-receipt-sha256", required=True)
    parser.add_argument("--job-header-sha256", required=True)
    parser.add_argument("--step-header-sha256", required=True)
    parser.add_argument("--service-header-sha256", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    library = args.library.resolve(strict=True)
    native_receipt = args.native_receipt.resolve(strict=True)
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    snapshot = output / "source"
    target = output / "target"
    target.mkdir()
    root = Path(__file__).resolve().parents[1]
    receipt = {
        "version": 1,
        "label": "final",
        "started_utc": datetime.now(timezone.utc).isoformat(),
        "library": str(library),
        "library_sha256": digest(library),
        "native_receipt": str(native_receipt),
        "native_receipt_sha256": digest(native_receipt),
        "snapshot": str(snapshot),
        "target": str(target),
        "inputs": {},
        "commands": [],
    }

    def command(name, argv, environment=None):
        print(shlex.join(argv), flush=True)
        log = output / f"{name}.log"
        entry = {"name": name, "argv": argv, "log": str(log)}
        receipt["commands"].append(entry)
        try:
            result = subprocess.run(
                argv, cwd=snapshot, env=environment, stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT, text=True, timeout=60, check=False,
            )
        except subprocess.TimeoutExpired as error:
            log.write_bytes(error.stdout or b"")
            entry["timed_out"] = True
            raise RuntimeError(f"{name} exceeded its subprocess bound") from error
        log.write_text(result.stdout)
        entry["exit_code"] = result.returncode
        entry["log_sha256"] = digest(log)
        print(result.stdout, end="", flush=True)
        print(f"{name.upper()}_EXIT={result.returncode}", flush=True)
        if result.returncode != 0:
            raise RuntimeError(f"{name} failed with exit {result.returncode}")
        return result.stdout

    try:
        if receipt["library_sha256"] != args.library_sha256:
            raise RuntimeError("native library hash mismatch")
        if receipt["native_receipt_sha256"] != args.native_receipt_sha256:
            raise RuntimeError("official native receipt hash mismatch")
        files = [
            "include/reiny.h", "include/reiny.hpp",
            "include/reiny_component.h", "include/reiny_component.hpp",
            "include/reiny_component_step.h", "include/reiny_component_step.hpp",
            "include/reiny_job.h", "include/reiny_job.hpp",
            "tests/component_cpp_client.cpp",
            "tests/component_step_cpp_client.cpp",
            "tests/component_step_cpp_lifecycle.inc",
            "tests/job_cpp_client.cpp", "tests/job_cpp_cases.inc",
            "tests/job_cpp_acceptance.py", "tests/job_cpp_README.md",
        ]
        for name in files:
            source, dest = root / name, snapshot / name
            dest.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(source, dest)
            receipt["inputs"][name] = digest(dest)
            if digest(source) != receipt["inputs"][name]:
                raise RuntimeError(f"source changed during snapshot: {name}")
        for name, expected in [
            ("include/reiny_job.h", args.job_header_sha256),
            ("include/reiny_component_step.h", args.step_header_sha256),
            ("include/reiny_component.h", args.service_header_sha256),
        ]:
            if receipt["inputs"][name] != expected:
                raise RuntimeError(f"native header hash mismatch: {name}")
        receipt["executables"] = {}
        for name, source, marker, variable in [
            ("jobs", "job_cpp_client.cpp", "CPP_JOB_LIBRARY", "REINY_JOB_CPP_LIBRARY"),
            ("service", "component_cpp_client.cpp", None, None),
            ("step", "component_step_cpp_client.cpp", "CPP_STEP_LIBRARY", "REINY_STEP_CPP_LIBRARY"),
        ]:
            executable = target / name
            command(f"{name}_compile", [
                "g++", "-std=c++17", "-Wall", "-Wextra", "-Werror",
                "-pedantic", "-pthread", "-I", str(snapshot / "include"),
                str(snapshot / "tests" / source), str(library),
                "-o", str(executable),
            ])
            receipt["executables"][name] = digest(executable)
            dynamic = command(f"{name}_link", ["readelf", "-d", str(executable)])
            if f"[{library}]" not in dynamic:
                raise RuntimeError(f"{name} does not name the exact absolute library")
            environment = dict(os.environ)
            if variable:
                environment[variable] = str(library)
            stdout = command(f"{name}_client", [str(executable)], environment)
            if marker and f"{marker} {library}\n" not in stdout:
                raise RuntimeError(f"{name} did not verify the exact mapped library")
        receipt["library_sha256_after"] = digest(library)
        if receipt["library_sha256_after"] != args.library_sha256:
            raise RuntimeError("native library changed during acceptance")
        receipt["passed"] = True
        return 0
    except Exception as error:
        receipt["passed"] = False
        receipt["error"] = str(error)
        print(f"CPP_JOB_ACCEPTANCE_ERROR {error}", file=sys.stderr, flush=True)
        return 1
    finally:
        receipt["finished_utc"] = datetime.now(timezone.utc).isoformat()
        path = output / "receipt.json"
        path.write_text(json.dumps(receipt, indent=2) + "\n")
        print(f"CPP_JOB_RECEIPT {path}", flush=True)


if __name__ == "__main__":
    sys.exit(main())
