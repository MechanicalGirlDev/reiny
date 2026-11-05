"""Compile and run the independent C++17 step client with retained evidence."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import shlex
import shutil
import subprocess
import sys
from datetime import datetime, timezone


def digest(path):
    with path.open("rb") as source:
        return hashlib.file_digest(source, "sha256").hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--library", required=True, type=Path)
    parser.add_argument("--library-sha256", required=True)
    parser.add_argument("--step-header-sha256", required=True)
    parser.add_argument("--service-header-sha256", required=True)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--label", choices=("provisional", "final"), required=True)
    parser.add_argument("--native-receipt", type=Path)
    parser.add_argument("--native-receipt-sha256")
    args = parser.parse_args()
    if args.label == "final" and not (
        args.native_receipt and args.native_receipt_sha256
    ):
        parser.error("final acceptance requires the official native receipt and hash")

    library = args.library.resolve(strict=True)
    root = Path(__file__).resolve().parents[1]
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    snapshot = output / "source"
    target = output / "target"
    target.mkdir()
    receipt = {
        "version": 1,
        "label": args.label,
        "started_utc": datetime.now(timezone.utc).isoformat(),
        "library": str(library),
        "library_sha256": digest(library),
        "snapshot": str(snapshot),
        "target": str(target),
        "inputs": {},
        "commands": [],
    }
    receipt_path = output / "receipt.json"

    def retain():
        receipt_path.write_text(json.dumps(receipt, indent=2) + "\n")
        print(f"CPP_STEP_RECEIPT {receipt_path}", flush=True)

    def command(name, argv, environment=None):
        log = output / f"{name}.log"
        print(shlex.join(argv), flush=True)
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
            raise RuntimeError("native library hash differs from the expected artifact")
        if args.native_receipt:
            native_receipt = args.native_receipt.resolve(strict=True)
            receipt["native_receipt"] = str(native_receipt)
            receipt["native_receipt_sha256"] = digest(native_receipt)
            if receipt["native_receipt_sha256"] != args.native_receipt_sha256:
                raise RuntimeError("official native receipt hash mismatch")
        files = [
            "include/reiny_component.h",
            "include/reiny_component.hpp",
            "include/reiny_component_step.h",
            "include/reiny_component_step.hpp",
            "tests/component_step_cpp_client.cpp",
            "tests/component_step_cpp_lifecycle.inc",
            "tests/component_step_cpp_acceptance.py",
            "tests/component_step_cpp_README.md",
        ]
        for name in files:
            source = root / name
            dest = snapshot / name
            dest.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(source, dest)
            receipt["inputs"][name] = digest(dest)
            if digest(source) != receipt["inputs"][name]:
                raise RuntimeError(f"source changed during snapshot: {name}")
        for name, expected in [
            ("include/reiny_component.h", args.service_header_sha256),
            ("include/reiny_component_step.h", args.step_header_sha256),
        ]:
            if receipt["inputs"][name] != expected:
                raise RuntimeError(f"ABI header hash mismatch: {name}")
        executable = target / "component_step_cpp_client"
        command("compile", [
            "g++", "-std=c++17", "-Wall", "-Wextra", "-Werror", "-pedantic",
            "-pthread", "-I", str(snapshot / "include"),
            str(snapshot / "tests/component_step_cpp_client.cpp"),
            str(library), "-o", str(executable),
        ])
        receipt["executable_sha256"] = digest(executable)
        environment = dict(os.environ, REINY_STEP_CPP_LIBRARY=str(library))
        stdout = command("client", [str(executable)], environment)
        if f"CPP_STEP_LIBRARY {library}\n" not in stdout:
            raise RuntimeError("client did not verify the exact mapped native library")
        receipt["library_sha256_after"] = digest(library)
        if receipt["library_sha256_after"] != args.library_sha256:
            raise RuntimeError("native library changed during acceptance")
        receipt["passed"] = True
        return 0
    except Exception as error:
        receipt["passed"] = False
        receipt["error"] = str(error)
        print(f"CPP_STEP_ACCEPTANCE_ERROR {error}", file=sys.stderr, flush=True)
        return 1
    finally:
        receipt["finished_utc"] = datetime.now(timezone.utc).isoformat()
        retain()


if __name__ == "__main__":
    sys.exit(main())
