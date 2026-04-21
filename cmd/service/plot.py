import csv
import math
import sys
from pathlib import Path

import matplotlib.pyplot as plt


def safe_float(value):
    try:
        return float(value)
    except Exception:
        return None


def read_results(csv_path):
    rows = []

    with open(csv_path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)

        for row in reader:
            freq = safe_float(row.get("frequency_hz"))
            vin = safe_float(row.get("vin_vpp"))
            vout = safe_float(row.get("vout_vpp"))
            gain = safe_float(row.get("gain"))
            gain_db = safe_float(row.get("gain_db"))
            ok_raw = row.get("ok", "true").strip().lower()
            ok = ok_raw in ("true", "1", "yes")
            point_type = row.get("point_type", "unknown").strip().lower()

            if freq is None:
                continue

            if gain_db is None and gain is not None and gain > 0:
                gain_db = 20 * math.log10(gain)

            rows.append({
                "frequency_hz": freq,
                "vin_vpp": vin,
                "vout_vpp": vout,
                "gain": gain,
                "gain_db": gain_db,
                "ok": ok,
                "point_type": point_type,
            })

    rows.sort(key=lambda x: x["frequency_hz"])
    return rows


def find_unity_crossing(rows):
    valid = [r for r in rows if r["ok"] and r["gain_db"] is not None]

    for i in range(len(valid) - 1):
        a = valid[i]
        b = valid[i + 1]

        y1 = a["gain_db"]
        y2 = b["gain_db"]

        if y1 == 0:
            return a["frequency_hz"], a
        if y2 == 0:
            return b["frequency_hz"], b

        if y1 * y2 < 0:
            x1 = math.log10(a["frequency_hz"])
            x2 = math.log10(b["frequency_hz"])

            if x2 == x1:
                return a["frequency_hz"], None

            # линейная интерполяция по оси log(f)
            x0 = x1 + (0 - y1) * (x2 - x1) / (y2 - y1)
            f0 = 10 ** x0
            return f0, (a, b)

    return None, None


def split_by_point_type(rows):
    groups = {}
    for r in rows:
        pt = r["point_type"] or "unknown"
        groups.setdefault(pt, []).append(r)
    return groups


def plot_gain_linear(rows):
    valid = [r for r in rows if r["ok"] and r["gain"] is not None]
    if not valid:
        return

    plt.figure(figsize=(10, 5))
    plt.semilogx(
        [r["frequency_hz"] for r in valid],
        [r["gain"] for r in valid],
        marker="o",
        linestyle="-",
    )
    plt.title("АЧХ: линейный коэффициент усиления")
    plt.xlabel("Частота, Гц")
    plt.ylabel("Ku = Vout / Vin")
    plt.grid(True, which="both")
    plt.tight_layout()
    plt.savefig("ach_linear.png", dpi=160)


def plot_gain_db(rows):
    valid = [r for r in rows if r["ok"] and r["gain_db"] is not None]
    if not valid:
        return

    plt.figure(figsize=(10, 5))

    groups = split_by_point_type(valid)

    style_map = {
        "base": ("o", "Базовые точки"),
        "refined": ("s", "Уточняющие точки"),
        "unity": ("^", "Точки поиска Ku=1"),
        "unknown": ("o", "Точки"),
    }

    used_any_group = False
    for point_type, items in groups.items():
        marker, label = style_map.get(point_type, ("o", point_type))
        plt.semilogx(
            [r["frequency_hz"] for r in items],
            [r["gain_db"] for r in items],
            marker=marker,
            linestyle="-",
            label=label,
        )
        used_any_group = True

    if not used_any_group:
        plt.semilogx(
            [r["frequency_hz"] for r in valid],
            [r["gain_db"] for r in valid],
            marker="o",
            linestyle="-",
            label="Точки",
        )

    plt.axhline(0, linestyle="--", linewidth=1)

    unity_freq, info = find_unity_crossing(rows)
    if unity_freq is not None:
        plt.axvline(unity_freq, linestyle="--", linewidth=1)
        plt.annotate(
            f"Ku=1 @ {unity_freq:.1f} Hz",
            xy=(unity_freq, 0),
            xytext=(10, 10),
            textcoords="offset points",
        )

    plt.title("АЧХ в дБ")
    plt.xlabel("Частота, Гц")
    plt.ylabel("Ku, дБ")
    plt.grid(True, which="both")
    plt.legend()
    plt.tight_layout()
    plt.savefig("ach_db.png", dpi=160)


def plot_vin_vout(rows):
    valid = [r for r in rows if r["ok"] and r["vin_vpp"] is not None and r["vout_vpp"] is not None]
    if not valid:
        return

    plt.figure(figsize=(10, 5))
    plt.semilogx(
        [r["frequency_hz"] for r in valid],
        [r["vin_vpp"] for r in valid],
        marker="o",
        linestyle="-",
        label="Vin, Vpp",
    )
    plt.semilogx(
        [r["frequency_hz"] for r in valid],
        [r["vout_vpp"] for r in valid],
        marker="s",
        linestyle="-",
        label="Vout, Vpp",
    )

    plt.title("Входной и выходной сигналы")
    plt.xlabel("Частота, Гц")
    plt.ylabel("Напряжение, В")
    plt.grid(True, which="both")
    plt.legend()
    plt.tight_layout()
    plt.savefig("vin_vout.png", dpi=160)


def main():
    csv_path = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("results.csv")

    if not csv_path.exists():
        print(f"Файл не найден: {csv_path}")
        return

    rows = read_results(csv_path)
    if not rows:
        print("В CSV нет пригодных данных")
        return

    plot_gain_linear(rows)
    plot_gain_db(rows)
    plot_vin_vout(rows)

    unity_freq, _ = find_unity_crossing(rows)
    if unity_freq is not None:
        print(f"Частота единичного усиления примерно: {unity_freq:.3f} Hz")
    else:
        print("Пересечение Ku=1 (0 dB) не найдено в текущем диапазоне")

    print("Графики сохранены:")
    print("  ach_linear.png")
    print("  ach_db.png")
    print("  vin_vout.png")

    plt.show()


if __name__ == "__main__":
    main()