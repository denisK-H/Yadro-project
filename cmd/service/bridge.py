import re
import time
import pyvisa
from flask import Flask, request, jsonify

app = Flask(__name__)

rm = pyvisa.ResourceManager()
devices = {}


def get_device(name: str):
    if name not in devices:
        raise ValueError(f"Device '{name}' is not connected")
    return devices[name]


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"ok": True})


@app.route("/connect", methods=["POST"])
def connect():
    data = request.get_json(force=True)

    name = data["name"]
    resource = data["resource"]
    timeout_ms = int(data.get("timeout_ms", 3000))
    write_termination = data.get("write_termination", "\n")
    read_termination = data.get("read_termination", "")

    inst = rm.open_resource(resource)
    inst.timeout = timeout_ms
    inst.write_termination = write_termination
    inst.read_termination = read_termination

    devices[name] = inst

    idn = ""
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


@app.route("/write", methods=["POST"])
def write_cmd():
    data = request.get_json(force=True)

    name = data["device"]
    cmd = data["cmd"]

    inst = get_device(name)
    inst.write(cmd)

    return jsonify({"ok": True})


@app.route("/query", methods=["POST"])
def query_cmd():
    data = request.get_json(force=True)

    name = data["device"]
    cmd = data["cmd"]

    inst = get_device(name)
    response = inst.query(cmd)

    return jsonify({
        "ok": True,
        "response": response
    })


@app.route("/query_number", methods=["POST"])
def query_number():
    data = request.get_json(force=True)

    name = data["device"]
    cmd = data["cmd"]
    mode = data.get("mode", "raw")          # raw | query
    delay_ms = int(data.get("delay_ms", 100))
    max_bytes = int(data.get("max_bytes", 4096))

    inst = get_device(name)

    if mode == "query":
        text = inst.query(cmd)
    else:
        inst.write(cmd)
        time.sleep(delay_ms / 1000.0)
        raw = inst.read_raw(max_bytes)
        text = raw.decode(errors="ignore")

    match = re.search(r'[-+]?\d*\.?\d+(?:[eE][-+]?\d+)?', text)
    if not match:
        return jsonify({
            "ok": False,
            "error": "No numeric value found",
            "raw": text
        }), 400

    return jsonify({
        "ok": True,
        "value": float(match.group()),
        "raw": text
    })


@app.route("/close", methods=["POST"])
def close_device():
    data = request.get_json(force=True)
    name = data["device"]

    inst = get_device(name)
    inst.close()
    del devices[name]

    return jsonify({"ok": True})


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