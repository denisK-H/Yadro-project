import csv
import math
import sys
from pathlib import Path

import matplotlib.pyplot as plt


def safe_float(value):
    try:
        return float(str(value).strip())
    except Exception:
        return None


def is_ok(row):
    return str(row.get("ok", "true")).strip().lower() in ("true", "1", "yes")


def read_rows(csv_path):
    rows = []
    with open(csv_path, "r", encoding="utf-8", newline="") as f:
        for row in csv.DictReader(f):
            freq = safe_float(row.get("frequency_hz"))
            vin = safe_float(row.get("vin_vpp"))
            vout = safe_float(row.get("vout_vpp"))
            gain = safe_float(row.get("gain"))
            gain_db = safe_float(row.get("gain_db"))

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
                "ok": is_ok(row),
                "point_type": row.get("point_type", "unknown").strip().lower(),
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
            return a["frequency_hz"]
        if y2 == 0:
            return b["frequency_hz"]
        if y1 * y2 < 0:
            x1 = math.log10(a["frequency_hz"])
            x2 = math.log10(b["frequency_hz"])
            if x2 == x1:
                return a["frequency_hz"]
            x0 = x1 + (0 - y1) * (x2 - x1) / (y2 - y1)
            return 10 ** x0

    return None


def split_by_point_type(rows):
    groups = {}
    for row in rows:
        point_type = row["point_type"] or "unknown"
        groups.setdefault(point_type, []).append(row)
    return groups


def savefig(path):
    plt.savefig(path, dpi=160)
    print(f"Saved: {path.resolve()}")


def save_plots(rows, output_dir):
    valid_db = [r for r in rows if r["ok"] and r["gain_db"] is not None]
    valid_voltage = [
        r for r in rows
        if r["ok"] and r["vin_vpp"] is not None and r["vout_vpp"] is not None
    ]

    if valid_db:
        plt.figure(figsize=(10, 5))
        style_map = {
            "base": ("o", "base"),
            "refined": ("s", "refined"),
            "unity": ("^", "unity search"),
            "extended": ("D", "extended"),
            "unknown": ("o", "points"),
        }
        for point_type, items in split_by_point_type(valid_db).items():
            marker, label = style_map.get(point_type, ("o", point_type))
            plt.semilogx(
                [r["frequency_hz"] for r in items],
                [r["gain_db"] for r in items],
                marker=marker,
                linestyle="-",
                label=label,
            )

        plt.axhline(0, linestyle="--", linewidth=1)
        unity_freq = find_unity_crossing(rows)
        if unity_freq is not None:
            plt.axvline(unity_freq, linestyle="--", linewidth=1)
            plt.annotate(
                f"Ku=1 @ {unity_freq:.1f} Hz",
                xy=(unity_freq, 0),
                xytext=(10, 10),
                textcoords="offset points",
            )

        K_mid_db = 12.6
        f_L = 100.0
        f_H = 1140000.0

        f_th = [
            10 ** (math.log10(100) + i * (math.log10(10000000) - math.log10(100)) / 99)
            for i in range(100)
        ]
        gain_th_db = [
            K_mid_db - 10 * math.log10(1 + (f_L / f) ** 2) - 10 * math.log10(1 + (f / f_H) ** 2)
            for f in f_th
        ]

        plt.semilogx(f_th, gain_th_db, linestyle="--", color="red", linewidth=1.5, label="theory")

        plt.title("Frequency response: dB")
        plt.xlabel("Frequency, Hz")
        plt.ylabel("Ku, dB")
        plt.grid(True, which="both")
        plt.legend()
        plt.xlim(left=100, right=6000000)
        plt.tight_layout()
        savefig(output_dir / "ach_db.png")

    if valid_voltage:
        plt.figure(figsize=(10, 5))
        plt.semilogx(
            [r["frequency_hz"] for r in valid_voltage],
            [r["vin_vpp"] for r in valid_voltage],
            marker="o",
            linestyle="-",
            label="Vin, Vpp",
        )
        plt.semilogx(
            [r["frequency_hz"] for r in valid_voltage],
            [r["vout_vpp"] for r in valid_voltage],
            marker="s",
            linestyle="-",
            label="Vout, Vpp",
        )
        plt.title("Input and output amplitudes")
        plt.xlabel("Frequency, Hz")
        plt.ylabel("Voltage, V")
        plt.grid(True, which="both")
        plt.legend()
        plt.tight_layout()
        savefig(output_dir / "vin_vout.png")


def plot_csv(csv_path):
    rows = read_rows(csv_path)
    if not rows:
        print(f"No usable rows in {csv_path}")
        return

    output_dir = csv_path.parent if str(csv_path.parent) != "" else Path(".")
    save_plots(rows, output_dir)

    unity_freq = find_unity_crossing(rows)
    if unity_freq is not None:
        print(f"Unity gain frequency: {unity_freq:.3f} Hz")
    else:
        print("Unity gain crossing was not found")


def main():
    paths = [Path(arg) for arg in sys.argv[1:]] if len(sys.argv) > 1 else [Path("results.csv")]

    for path in paths:
        if not path.exists():
            print(f"File not found: {path}")
            continue
        print(f"Plotting {path}")
        plot_csv(path)

    plt.show()


if __name__ == "__main__":
    main()
