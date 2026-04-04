#!/usr/bin/env python3
"""
Meridian Daily Snapshot Script
===============================
Corre todos los días a las 21:30 UTC (15:30 hora México) via cron.
Lee balance_history.json del bond scanner, calcula el % de ganancia
del día, aplica el factor de peso y hace push a GitHub.

Cron entry:
  30 21 * * * /usr/bin/python3 /home/itorres/repos/meridian/meridian_snapshot.py >> /home/itorres/repos/meridian/snapshot.log 2>&1
"""

import json
import subprocess
import sys
from datetime import date, datetime, timezone
from pathlib import Path

# ─────────────────────────────────────────────────────────────────
# CONFIGURACIÓN — todos los parámetros importantes están aquí arriba
# ─────────────────────────────────────────────────────────────────

# Factor de peso aplicado a los rendimientos diarios.
# Cambiar a 1.0 para mostrar rendimientos reales sin modificación.
# Solo afecta snapshots nuevos (weighted: true).
# Los datos históricos hardcodeados (weighted: false) nunca se tocan.
WEIGHT_FACTOR = 0.6

# Rutas
BALANCE_HISTORY_JSON = Path("/home/itorres/repos/bond_scanner/balance_history.json")
MERIDIAN_JSON        = Path("/home/itorres/repos/meridian/performance.json")

# ─────────────────────────────────────────────────────────────────


def log(msg: str):
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    print(f"[{ts}] {msg}")


def load_json(path: Path) -> dict | list:
    with open(path, "r") as f:
        return json.load(f)


def save_json(path: Path, data: dict):
    with open(path, "w") as f:
        json.dump(data, f, indent=2)


def get_today_balance() -> float:
    """
    Lee balance_history.json y retorna el valor de cartera más reciente de hoy.
    Usa el campo 'cartera' = CLOB balance + open positions value.
    """
    history   = load_json(BALANCE_HISTORY_JSON)
    today_str = date.today().isoformat()

    today_entries = [
        e for e in history
        if e.get("ts", "").startswith(today_str)
    ]

    if not today_entries:
        log(f"ERROR: No hay entradas para hoy ({today_str}) en balance_history.json")
        sys.exit(1)

    latest  = sorted(today_entries, key=lambda e: e["ts"])[-1]
    cartera = float(latest["cartera"])
    log(f"Balance de hoy — ts: {latest['ts']} | cartera: ${cartera:.4f}")
    return cartera


def get_yesterday_balance() -> float:
    """
    Lee balance_history.json y retorna el valor de cartera del día anterior.
    Busca la entrada más reciente de días anteriores a hoy.
    """
    history   = load_json(BALANCE_HISTORY_JSON)
    today_str = date.today().isoformat()

    past_entries = [
        e for e in history
        if e.get("ts", "") < today_str
    ]

    if not past_entries:
        log("ERROR: No hay entradas de días anteriores en balance_history.json")
        sys.exit(1)

    latest  = sorted(past_entries, key=lambda e: e["ts"])[-1]
    cartera = float(latest["cartera"])
    log(f"Balance de ayer — ts: {latest['ts']} | cartera: ${cartera:.4f}")
    return cartera


def today_already_logged(snapshots: list) -> bool:
    return any(s["date"] == date.today().isoformat() for s in snapshots)


def run_snapshot():
    log("=== Meridian snapshot iniciando ===")

    performance = load_json(MERIDIAN_JSON)
    snapshots   = performance["snapshots"]

    if today_already_logged(snapshots):
        log("Snapshot de hoy ya existe. Nada que hacer.")
        return

    today_cartera     = get_today_balance()
    yesterday_cartera = get_yesterday_balance()

    if yesterday_cartera <= 0:
        log(f"ERROR: Balance de ayer inválido: {yesterday_cartera}")
        sys.exit(1)

    # % diario real sin peso
    raw_daily_pct = ((today_cartera - yesterday_cartera) / yesterday_cartera) * 100

    # Aplicar factor de peso — busca WEIGHT_FACTOR arriba para cambiar
    weighted_daily_pct = round(raw_daily_pct * WEIGHT_FACTOR, 4)

    # Acumulado sobre valores ponderados anteriores
    prev_cumulative = snapshots[-1]["cumulative_pct"]
    new_cumulative  = round(prev_cumulative + weighted_daily_pct, 4)

    new_snapshot = {
        "date"           : date.today().isoformat(),
        "daily_pct"      : weighted_daily_pct,
        "cumulative_pct" : new_cumulative,
        "weighted"       : True,
    }

    snapshots.append(new_snapshot)
    performance["snapshots"]    = snapshots
    performance["last_updated"] = datetime.now(timezone.utc).isoformat()

    save_json(MERIDIAN_JSON, performance)
    log(f"Snapshot guardado — date: {new_snapshot['date']} | daily: {weighted_daily_pct}% | cumulative: {new_cumulative}%")

    push_to_github()
    log("=== Meridian snapshot completado ===")


def push_to_github():
    """
    Commit y push de performance.json al repo de GitHub.
    Requiere que ~/repos/meridian tenga el remote origin configurado.
    """
    meridian_dir = MERIDIAN_JSON.parent
    today_str    = date.today().isoformat()

    commands = [
        ["git", "-C", str(meridian_dir), "add", "performance.json"],
        ["git", "-C", str(meridian_dir), "commit", "-m", f"snapshot: {today_str}"],
        ["git", "-C", str(meridian_dir), "push", "origin", "main"],
    ]

    for cmd in commands:
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            log(f"ERROR git ({' '.join(cmd[3:])}): {result.stderr.strip()}")
            sys.exit(1)
        log(f"git {' '.join(cmd[3:])} — OK")


if __name__ == "__main__":
    run_snapshot()

