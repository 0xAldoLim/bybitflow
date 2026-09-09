"""Local Chromium/CDP smoke test. Run while the dashboard listens on port 8765.

No browser package dependency. Screenshots go to data/ui; never seeds fake market observations.
"""

import asyncio
import base64
import json
import subprocess
import tempfile
from pathlib import Path

import httpx
import websockets


async def main():
    directory = Path("data/ui")
    directory.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="bybit-flow-browser-") as profile:
        process = subprocess.Popen(
            [
                "chromium",
                "--headless",
                "--no-sandbox",
                "--disable-gpu",
                "--disable-dev-shm-usage",
                "--remote-debugging-address=127.0.0.1",
                "--remote-debugging-port=9223",
                "--user-data-dir=" + profile,
                "about:blank",
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        try:
            async with httpx.AsyncClient() as http:
                for _ in range(300):
                    try:
                        tabs = (await http.get("http://127.0.0.1:9223/json")).json()
                        if tabs:
                            break
                    except httpx.TransportError:
                        pass
                    await asyncio.sleep(0.1)
                else:
                    raise RuntimeError("Chromium did not start")
            async with websockets.connect(tabs[0]["webSocketDebuggerUrl"]) as ws:
                ident = 0

                async def call(method, params=None):
                    nonlocal ident
                    ident += 1
                    await ws.send(json.dumps({"id": ident, "method": method, "params": params or {}}))
                    while True:
                        response = json.loads(await ws.recv())
                        if response.get("id") == ident:
                            if "error" in response:
                                raise RuntimeError(response["error"])
                            return response.get("result", {})

                await call("Page.enable")
                for label, width, height in (("desktop", 1440, 1000), ("mobile", 390, 844)):
                    await call(
                        "Emulation.setDeviceMetricsOverride",
                        {
                            "width": width,
                            "height": height,
                            "deviceScaleFactor": 1,
                            "mobile": label == "mobile",
                        },
                    )
                    await call("Page.navigate", {"url": "http://127.0.0.1:8765"})
                    await asyncio.sleep(1)
                    layout = (
                        await call(
                            "Runtime.evaluate",
                            {
                                "expression": "JSON.stringify({width:innerWidth,scroll:document.documentElement.scrollWidth,error:document.querySelector('#error').textContent})"
                            },
                        )
                    )["result"]["value"]
                    layout = json.loads(layout)
                    assert not layout["error"], layout
                    assert layout["scroll"] <= width, layout
                    shot = await call("Page.captureScreenshot", {"format": "png"})
                    (directory / f"{label}.png").write_bytes(base64.b64decode(shot["data"]))
                    print(label, layout)
                for route in (
                    "watchlist",
                    "signals",
                    "tradingview",
                    "research",
                    "journal",
                    "health",
                    "settings",
                ):
                    await call("Runtime.evaluate", {"expression": f"location.hash='{route}'"})
                    await asyncio.sleep(0.3)
                    result = (
                        await call(
                            "Runtime.evaluate", {"expression": "document.querySelector('#error').textContent"}
                        )
                    )["result"]["value"]
                    assert not result, (route, result)
                    print(route, "rendered")
        finally:
            process.terminate()
            process.wait(timeout=10)


if __name__ == "__main__":
    asyncio.run(main())
