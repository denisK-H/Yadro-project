import re
import time
import traceback
import pyvisa
from flask import Flask, request, jsonify

app = Flask(__name__)

rm = pyvisa.ResourceManager()
devices = {}


def get_device(name: str):
    if name not in devices:
        raise ValueError(f"Device '{name}' is not connected")
    return devices[name]


def parse_scpi_value(text: str):
    text = text.strip()

    match = re.search(r'([-+]?\d*\.?\d+(?:[eE][-+]?\d+)?)\s*([uUmMkK]?V)', text)
    if match:
        value = float(match.group(1))
        unit = match.group(2)

        multipliers = {
            "uV": 1e-6,
            "UV": 1e-6,
            "mV": 1e-3,
            "MV": 1e6,
            "V": 1.0,
            "kV": 1e3,
            "KV": 1e3,
        }

        if unit in multipliers:
            return value * multipliers[unit]

        unit_norm = unit.replace("K", "k").replace("U", "u")
        if unit_norm in multipliers:
            return value * multipliers[unit_norm]

    match_num = re.search(r'[-+]?\d*\.?\d+(?:[eE][-+]?\d+)?', text)
    if match_num:
        return float(match_num.group())

    return None


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"ok": True})


@app.route("/resources", methods=["GET"])
def resources():
    try:
        items = rm.list_resources()
        return jsonify({"ok": True, "resources": list(items)})
    except Exception as e:
        return jsonify({
            "ok": False,
            "error": str(e),
            "traceback": traceback.format_exc()
        }), 500


@app.route("/connect", methods=["POST"])
def connect():
    try:
        data = request.get_json(force=True)

        name = data["name"]
        resource = data["resource"]
        timeout_ms = int(data.get("timeout_ms", 3000))
        write_termination = data.get("write_termination", "\n")
        read_termination = data.get("read_termination", "")
        skip_idn = bool(data.get("skip_idn", False))

        print(f"[CONNECT] name={name}, resource={resource}")

        inst = rm.open_resource(resource)
        inst.timeout = timeout_ms
        inst.write_termination = write_termination
        inst.read_termination = read_termination

        devices[name] = inst

        idn = "skipped"
        if not skip_idn:
            try:
                idn = inst.query("*IDN?").strip()
            except Exception as e:
                idn = f"IDN unavailable: {e}"

        return jsonify({
            "ok": True,
            "name": name,
            "resource": resource,
            "idn": idn
        })

    except Exception as e:
        return jsonify({
            "ok": False,
            "error": str(e),
            "traceback": traceback.format_exc()
        }), 500


@app.route("/write", methods=["POST"])
def write_cmd():
    try:
        data = request.get_json(force=True)

        name = data["device"]
        cmd = data["cmd"]

        inst = get_device(name)
        inst.write(cmd)

        return jsonify({"ok": True})

    except Exception as e:
        return jsonify({
            "ok": False,
            "error": str(e),
            "traceback": traceback.format_exc()
        }), 500


@app.route("/query", methods=["POST"])
def query_cmd():
    try:
        data = request.get_json(force=True)

        name = data["device"]
        cmd = data["cmd"]

        inst = get_device(name)
        response = inst.query(cmd)

        return jsonify({
            "ok": True,
            "response": response
        })

    except Exception as e:
        return jsonify({
            "ok": False,
            "error": str(e),
            "traceback": traceback.format_exc()
        }), 500


@app.route("/query_number", methods=["POST"])
def query_number():
    try:
        data = request.get_json(force=True)

        name = data["device"]
        cmd = data["cmd"]
        mode = data.get("mode", "raw")  # raw | query
        delay_ms = int(data.get("delay_ms", 200))
        max_bytes = int(data.get("max_bytes", 4096))

        inst = get_device(name)

        if mode == "query":
            text = inst.query(cmd)
        else:
            inst.write(cmd)
            time.sleep(delay_ms / 1000.0)
            raw = inst.read_raw(max_bytes)
            text = raw.decode(errors="ignore")

        value = parse_scpi_value(text)
        if value is None:
            return jsonify({
                "ok": False,
                "error": "No numeric value found",
                "raw": text
            }), 400

        return jsonify({
            "ok": True,
            "value": value,
            "raw": text
        })

    except Exception as e:
        return jsonify({
            "ok": False,
            "error": str(e),
            "traceback": traceback.format_exc()
        }), 500


@app.route("/close", methods=["POST"])
def close_device():
    try:
        data = request.get_json(force=True)
        name = data["device"]

        inst = get_device(name)
        inst.close()
        del devices[name]

        return jsonify({"ok": True})

    except Exception as e:
        return jsonify({
            "ok": False,
            "error": str(e),
            "traceback": traceback.format_exc()
        }), 500


@app.route("/close_all", methods=["POST"])
def close_all():
    errors = []

    for name in list(devices.keys()):
        try:
            devices[name].close()
            del devices[name]
        except Exception as e:
            errors.append(f"{name}: {e}")

    return jsonify({
        "ok": len(errors) == 0,
        "errors": errors
    })


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=8080, debug=False)