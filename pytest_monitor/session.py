import datetime
import hashlib
import json
import os
import sys
import warnings
from http import HTTPStatus

import memory_profiler
import psutil
import requests

from pytest_monitor.handler import DBHandler
from pytest_monitor.sys_utils import (
    ExecutionContext,
    collect_ci_info,
    determine_scm_revision,
)


def _log(msg):
    """Log to stdout for debugging in CI environments."""
    print(f"[pytest-monitor] {msg}", file=sys.stdout, flush=True)


class PyTestMonitorSession:
    def __init__(self, db=None, remote=None, component="", scope=None, tracing=True):
        self.__db = None
        if db:
            self.__db = DBHandler(db)
        self.__monitor_enabled = tracing
        self.__remote = remote
        self.__component = component
        self.__session = ""
        self.__scope = scope or []
        self.__eid = (None, None)
        self.__mem_usage_base = None
        self.__process = psutil.Process(os.getpid())
        self.__test_order = 0  # Counter for test execution order

    @property
    def monitoring_enabled(self):
        return self.__monitor_enabled

    @property
    def remote_env_id(self):
        return self.__eid[1]

    @property
    def db_env_id(self):
        return self.__eid[0]

    @property
    def process(self):
        return self.__process

    @property
    def session_h(self):
        return self.__session

    @property
    def test_order(self):
        return self.__test_order

    def increment_test_order(self):
        self.__test_order += 1
        return self.__test_order

    def get_env_id(self, env):
        db, remote = None, None
        if self.__db:
            row = self.__db.query("SELECT ENV_H FROM EXECUTION_CONTEXTS WHERE ENV_H= ?", (env.compute_hash(),))
            db = row[0] if row else None
        if self.__remote:
            url = f"{self.__remote}/contexts/{env.compute_hash()}"
            _log(f"GET {url}")
            r = requests.get(url)
            _log(f"GET response: {r.status_code}")
            remote = None
            if r.status_code == HTTPStatus.OK:
                remote = json.loads(r.text)
                _log(f"GET response body: {remote}")
                if remote["contexts"]:
                    remote = remote["contexts"][0]["h"]
                else:
                    remote = None
        return db, remote

    def compute_info(self, description, tags):
        run_date = datetime.datetime.now().isoformat()
        scm = determine_scm_revision()
        h = hashlib.md5()
        h.update(scm.encode())
        h.update(run_date.encode())
        h.update(description.encode())
        self.__session = h.hexdigest()
        # From description + tags to JSON format
        d = collect_ci_info()
        if description:
            d["description"] = description
        for tag in tags:
            if type(tag) is str:
                _tag_info = tag.split("=", 1)
                d[_tag_info[0]] = _tag_info[1]
            else:
                for sub_tag in tag:
                    _tag_info = sub_tag.split("=", 1)
                    d[_tag_info[0]] = _tag_info[1]
        description = json.dumps(d)
        # Now get memory usage base and create the database
        self.prepare()
        self.set_environment_info(ExecutionContext())
        if self.__db:
            self.__db.insert_session(self.__session, run_date, scm, description)
        if self.__remote:
            url = f"{self.__remote}/sessions/"
            payload = {
                "session_h": self.__session,
                "run_date": run_date,
                "scm_ref": scm,
                "description": json.loads(description),
            }
            _log(f"POST {url}")
            _log(f"POST payload: {payload}")
            r = requests.post(url, json=payload)
            _log(f"POST response: {r.status_code} - {r.text[:200] if r.text else 'empty'}")
            if r.status_code != HTTPStatus.CREATED:
                self.__remote = ""
                msg = f"Cannot insert session in remote monitor server ({r.status_code})! Deactivating...')"
                warnings.warn(msg)

    def set_environment_info(self, env):
        self.__eid = self.get_env_id(env)
        db_id, remote_id = self.__eid
        _log(f"set_environment_info: db_id={db_id}, remote_id={remote_id}")
        if self.__db and db_id is None:
            self.__db.insert_execution_context(env)
            db_id = self.__db.query("select ENV_H from EXECUTION_CONTEXTS where ENV_H = ?", (env.compute_hash(),))[0]
        if self.__remote and remote_id is None:
            url = f"{self.__remote}/contexts/"
            payload = env.to_dict()
            _log(f"POST {url}")
            _log(f"POST payload: {payload}")
            r = requests.post(url, json=payload)
            _log(f"POST response: {r.status_code} - {r.text[:500] if r.text else 'empty'}")
            if r.status_code != HTTPStatus.CREATED:
                warnings.warn(f"Cannot insert execution context in remote server (rc={r.status_code}! Deactivating...")
                self.__remote = ""
            else:
                remote_id = json.loads(r.text)["h"]
                _log(f"Got remote context id: {remote_id}")
        self.__eid = db_id, remote_id
        _log(f"Final env IDs: db={db_id}, remote={remote_id}")

    def prepare(self):
        def dummy():
            return True

        memuse = memory_profiler.memory_usage((dummy,), max_iterations=1, max_usage=True)
        self.__mem_usage_base = memuse[0] if type(memuse) is list else memuse

    def add_test_info(
        self,
        item,
        item_path,
        item_variant,
        item_loc,
        kind,
        component,
        item_start_time,
        total_time,
        user_time,
        kernel_time,
        mem_usage,
    ):
        if kind not in self.__scope:
            _log(f"METRIC SKIPPED (kind={kind} not in scope={self.__scope}): {item}")
            return
        mem_usage = float(mem_usage) - self.__mem_usage_base
        cpu_usage = (user_time + kernel_time) / total_time
        item_start_time = datetime.datetime.fromtimestamp(item_start_time).isoformat()
        final_component = self.__component.format(user_component=component)
        if final_component.endswith("."):
            final_component = final_component[:-1]
        item_variant = item_variant.replace("-", ", ")  # No choice
        if self.__db and self.db_env_id is not None:
            self.__db.insert_metric(
                self.__session,
                self.db_env_id,
                item_start_time,
                item,
                item_path,
                item_variant,
                item_loc,
                kind,
                final_component,
                total_time,
                user_time,
                kernel_time,
                cpu_usage,
                mem_usage,
            )
        if self.__remote and self.remote_env_id is not None:
            url = f"{self.__remote}/metrics/"
            payload = {
                "session_h": self.__session,
                "context_h": self.remote_env_id,
                "item_start_time": item_start_time,
                "item_path": item_path,
                "item": item,
                "item_variant": item_variant,
                "item_fs_loc": item_loc,
                "kind": kind,
                "component": final_component,
                "total_time": total_time,
                "user_time": user_time,
                "kernel_time": kernel_time,
                "cpu_usage": cpu_usage,
                "mem_usage": mem_usage,
            }
            _log(f"POST {url} - {item} (mem={mem_usage:.2f}MB)")
            r = requests.post(url, json=payload)
            if r.status_code != HTTPStatus.CREATED:
                _log(f"METRIC FAILED: {r.status_code} - {r.text[:200] if r.text else 'empty'}")
                self.__remote = ""
                msg = f"Cannot insert values in remote monitor server ({r.status_code})! Deactivating...')"
                warnings.warn(msg)
            else:
                _log(f"METRIC OK: {item}")
        elif self.__remote and self.remote_env_id is None:
            _log(f"METRIC SKIPPED (no remote_env_id): {item}")

    def add_system_memory_snapshot(self, item_path, item):
        """
        Capture and send system-wide memory snapshot after a test.
        This helps identify memory leaks outside of individual test functions.
        """
        if not self.__remote:
            return

        test_order = self.increment_test_order()

        # Get system memory info
        vm = psutil.virtual_memory()
        swap = psutil.swap_memory()
        proc_mem = self.__process.memory_info()

        recorded_at = datetime.datetime.now().isoformat()

        payload = {
            "session_h": self.__session,
            "test_order": test_order,
            "item_path": item_path,
            "item": item,
            "total_memory_mb": vm.total / (1024 ** 2),
            "available_memory_mb": vm.available / (1024 ** 2),
            "used_memory_mb": vm.used / (1024 ** 2),
            "memory_percent": vm.percent,
            "process_rss_mb": proc_mem.rss / (1024 ** 2),
            "process_vms_mb": proc_mem.vms / (1024 ** 2),
            "cached_mb": getattr(vm, 'cached', 0) / (1024 ** 2) if hasattr(vm, 'cached') else None,
            "buffers_mb": getattr(vm, 'buffers', 0) / (1024 ** 2) if hasattr(vm, 'buffers') else None,
            "swap_used_mb": swap.used / (1024 ** 2),
            "recorded_at": recorded_at,
        }

        url = f"{self.__remote}/system-memory/"
        try:
            r = requests.post(url, json=payload)
            if r.status_code != HTTPStatus.CREATED:
                _log(f"SYSMEM FAILED: {r.status_code} - {r.text[:200] if r.text else 'empty'}")
            # Don't disable remote on system memory failures - it's optional
        except Exception as e:
            _log(f"SYSMEM ERROR: {e}")
