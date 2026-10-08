"""Прогон замеров lite_db.

Для каждого сценария поднимает server_app в чистой временной папке
с нужным setup.yaml, запускает bench_client, после прогона снимает
счётчики сервера (STATS) и останавливает его.

    python benchmarks/run.py --build build                 # все сценарии
    python benchmarks/run.py --build build --only insert   # сценарии с 'insert' в имени
    python benchmarks/run.py --build build --out results.jsonl

Каждый сценарий запускается --repeat раз (по умолчанию 3) на свежем
сервере; в таблицу идёт медиана, а разброс строк/с показан как min–max.
Печатает таблицу Markdown. С --out дописывает каждый прогон одной
строкой JSON (вместе с условиями замера).
"""

import argparse
import json
import os
import platform
import signal
import socket
import statistics
import subprocess
import sys
import tempfile
import time

IS_WINDOWS = os.name == "nt"
EXE = ".exe" if IS_WINDOWS else ""

# name, настройки setup.yaml, аргументы bench_client
SCENARIOS = [
    ("insert, 1 клиент, wal_sync=full",
     {"wal_sync": "full"}, ["--workload", "insert", "--clients", "1", "--ops", "2000"]),
    ("insert, 4 клиента, одна таблица, full",
     {"wal_sync": "full"}, ["--workload", "insert", "--clients", "4", "--ops", "1000"]),
    ("insert, 4 клиента, 4 таблицы, full",
     {"wal_sync": "full"}, ["--workload", "insert", "--clients", "4", "--ops", "1000",
                            "--table-per-client", "1"]),
    ("insert MPUT×100, 1 клиент, full",
     {"wal_sync": "full"}, ["--workload", "insert", "--clients", "1", "--ops", "200", "--batch", "100"]),
    ("insert, 1 клиент, wal_sync=none",
     {"wal_sync": "none"}, ["--workload", "insert", "--clients", "1", "--ops", "20000"]),
    ("select с диска, 4 клиента, без кеша",
     {"wal_sync": "none", "block_cache_mb": "0"},
     ["--workload", "select", "--clients", "4", "--ops", "10000", "--preload", "100000"]),
    ("select с диска, 4 клиента, кеш 64 МБ",
     {"wal_sync": "none", "block_cache_mb": "64"},
     ["--workload", "select", "--clients", "4", "--ops", "10000", "--preload", "100000"]),
    ("select из кеша, 1 клиент, конвейер 16",
     {"wal_sync": "none", "block_cache_mb": "64"},
     ["--workload", "select", "--clients", "1", "--ops", "500", "--pipeline", "16", "--preload", "20000"]),
    ("update, 4 клиента, full",
     {"wal_sync": "full", "block_cache_mb": "64"},
     ["--workload", "update", "--clients", "4", "--ops", "1000", "--preload", "100000"]),
]


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def request(port, command):
    with socket.create_connection(("127.0.0.1", port), timeout=30) as s:
        s.sendall((command + "\n").encode())
        data = b""
        while not data.endswith(b"\n"):
            chunk = s.recv(65536)
            if not chunk:
                break
            data += chunk
    return data.decode().strip()


def write_config(workdir, settings):
    with open(os.path.join(workdir, "setup.yaml"), "w") as f:
        for key, value in settings.items():
            f.write(f"{key}: {value}\n")


def start_server(server, workdir, port):
    log = open(os.path.join(workdir, "server.log"), "w")
    flags = subprocess.CREATE_NEW_PROCESS_GROUP if IS_WINDOWS else 0
    proc = subprocess.Popen([server, str(port)], cwd=workdir, stdout=log,
                            stderr=subprocess.STDOUT, creationflags=flags)
    log.close()
    deadline = time.time() + 20
    while time.time() < deadline:
        try:
            socket.create_connection(("127.0.0.1", port), timeout=0.5).close()
            return proc
        except OSError:
            time.sleep(0.05)
    proc.kill()
    raise RuntimeError("server did not start")


def stop_server(proc):
    if IS_WINDOWS:
        proc.kill()
    else:
        proc.send_signal(signal.SIGINT)
    proc.wait(timeout=60)


def environment():
    cpu = platform.processor() or platform.machine()
    if os.path.exists("/proc/cpuinfo"):
        with open("/proc/cpuinfo") as f:
            for line in f:
                if line.startswith("model name"):
                    cpu = line.split(":", 1)[1].strip()
                    break
    return {"os": platform.platform(), "cpu": cpu, "cores": os.cpu_count()}


def build_type(build_dir):
    """Тип сборки из CMakeCache.txt: замеры отладочной сборки ничего не говорят."""
    try:
        with open(os.path.join(build_dir, "CMakeCache.txt"), encoding="utf-8", errors="replace") as f:
            for line in f:
                if line.startswith("CMAKE_BUILD_TYPE:"):
                    return line.split("=", 1)[1].strip()
    except OSError:
        pass
    return ""


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--build", default="build", help="каталог сборки с server_app и bench_client")
    parser.add_argument("--only", default="", help="запускать только сценарии с этой подстрокой")
    parser.add_argument("--out", default="", help="файл JSONL для результатов")
    parser.add_argument("--tag", default="", help="метка версии, например commit")
    parser.add_argument("--repeat", type=int, default=3, help="повторов каждого сценария")
    args = parser.parse_args()

    server = os.path.abspath(os.path.join(args.build, "server_app" + EXE))
    bench = os.path.abspath(os.path.join(args.build, "bench_client" + EXE))
    env = environment()
    env["build_type"] = build_type(args.build)
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    print(f"Условия: {env['cpu']}, ядер: {env['cores']}, {env['os']}, сборка {env['build_type'] or '?'}\n")
    if env["build_type"] != "Release":
        print("ВНИМАНИЕ: сборка не Release — цифры будут занижены в разы. Пересоберите:\n"
              f"  cmake -S . -B {args.build} -DCMAKE_BUILD_TYPE=Release && cmake --build {args.build} -j\n",
              flush=True)

    def run_once(name, settings, bench_args):
        with tempfile.TemporaryDirectory() as workdir:
            write_config(workdir, settings)
            port = free_port()
            proc = start_server(server, workdir, port)
            try:
                result_file = os.path.join(workdir, "result.json")
                done = subprocess.run([bench, "--port", str(port), "--label", name,
                                       "--json", result_file] + bench_args,
                                      capture_output=True, text=True, timeout=1800)
                if done.returncode != 0:
                    # Сценарий не поддерживается этой версией (например, MPUT)
                    # или упал — отмечаем и идём дальше.
                    error = (done.stdout + done.stderr).strip().splitlines()[-1:]
                    return None, (error[0] if error else str(done.returncode))
                with open(result_file, encoding="utf-8", errors="replace") as f:
                    result = json.loads(f.readline())
                # На Windows кириллица в аргументах командной строки может
                # дойти до bench_client искажённой — имя берём своё.
                result["label"] = name
                stats = request(port, "STATS")
                result["stats"] = json.loads(stats) if stats.startswith("{") else {}
            finally:
                stop_server(proc)
        result.update({"settings": settings, "env": env, "tag": args.tag})
        return result, None

    summary = []
    for name, settings, bench_args in SCENARIOS:
        if args.only and args.only not in name:
            continue
        runs = []
        for _ in range(args.repeat):
            result, error = run_once(name, settings, bench_args)
            if error:
                print(f"  {name}: ОШИБКА — {error}", flush=True)
                break
            runs.append(result)
            if args.out:
                with open(args.out, "a", encoding="utf-8") as f:
                    f.write(json.dumps(result, ensure_ascii=False) + "\n")
        if not runs:
            continue

        rates = [r["rows_per_s"] for r in runs]
        row = {
            "label": name,
            "rows_per_s": statistics.median(rates),
            "min": min(rates),
            "max": max(rates),
            "p50_us": statistics.median(r["p50_us"] for r in runs),
            "p99_us": statistics.median(r["p99_us"] for r in runs),
            "batch": runs[0]["batch"],
            "stats": runs[len(runs) // 2]["stats"],
        }
        summary.append(row)
        print(f"  {name}: {row['rows_per_s']:.0f} строк/с ({row['min']:.0f}–{row['max']:.0f}), "
              f"p50 {row['p50_us']:.0f} мкс, p99 {row['p99_us']:.0f} мкс", flush=True)

    print(f"\n| Сценарий | строк/с (медиана из {args.repeat}) | разброс | p50, мкс | p99, мкс | fsync на строку |")
    print("|---|---:|---:|---:|---:|---:|")
    for r in summary:
        stats = r["stats"]
        appends = stats.get("wal_appends", 0)
        per_row = "—"
        if appends:
            per_row = f"{stats['wal_fsyncs'] / (appends * r['batch']):.3f}".rstrip("0").rstrip(".")
        print(f"| {r['label']} | {r['rows_per_s']:.0f} | {r['min']:.0f}–{r['max']:.0f} "
              f"| {r['p50_us']:.0f} | {r['p99_us']:.0f} | {per_row} |")
    return 0


if __name__ == "__main__":
    sys.exit(main())
