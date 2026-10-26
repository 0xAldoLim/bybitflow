"""Disposable offline stack + stopped-writer volume backup/restore.

Never reads the real Compose/.env, publishes port 8000, uses a production image
name or mounts existing data. Only unique volumes created here are removed.
"""

import json
import os
import secrets
import sqlite3
import subprocess
import tempfile
from pathlib import Path


def specification(root, image):
    return dict(services=dict(desk=dict(image=image,build=dict(context=str(root),target="runtime",
        args=dict(FLOW_CODE_COMMIT="isolated-smoke")),network_mode="none",init=True,
        environment=dict(FLOW_DATA_DIR="/app/data",FLOW_SCAN_ENABLED="false",FLOW_MACRO_NEWS_ENABLED="false",
            FLOW_ML_ENABLED="false",FLOW_RESEARCH_ALERTS="false",FLOW_RESEARCH_WEBHOOK="",
            FLOW_MONITORING_WEBHOOK="",FLOW_DISCORD_WEBHOOK="",FLOW_OPS_WEBHOOK="",FLOW_ADMIN_TOKEN="isolated-smoke-only"),
        volumes=["smoke-data:/app/data"],mem_limit="1g",cpus=1,stop_grace_period="30s",
        healthcheck=dict(test=["CMD","python","-c","import urllib.request,base64; r=urllib.request.Request('http://127.0.0.1:8000/healthz'); r.add_header('Authorization','Basic '+base64.b64encode(b'research:isolated-smoke-only').decode()); urllib.request.urlopen(r,timeout=5)"],interval="2s",timeout="6s",retries=30))),volumes={"smoke-data":{}})


def main():
    project="bybitflow-isolated-"+secrets.token_hex(6)
    restored=project+"-restore"
    image=project+":test"
    env={k:v for k,v in os.environ.items() if not k.startswith("FLOW_")}
    root=Path(__file__).resolve().parents[1]
    with tempfile.TemporaryDirectory(prefix="bybitflow-isolated-") as folder:
        folder=Path(folder)
        config=folder/"compose.json"
        config.write_text(json.dumps(specification(root,image)),encoding="utf-8")
        empty_env=folder/"empty.env"
        empty_env.write_text("",encoding="utf-8")
        def run(*args,project_name=project,check=True):
            result=subprocess.run(["docker","compose","--env-file",str(empty_env),"-f",str(config),"-p",project_name,*args],
                env=env,text=True,capture_output=True,check=False)
            if check and result.returncode:
                logs=subprocess.run(["docker","compose","--env-file",str(empty_env),"-f",str(config),"-p",project_name,"logs","--tail","30"],env=env,text=True,capture_output=True,check=False)
                print(logs.stdout[-6000:], flush=True)
                raise RuntimeError("Isolated Compose failed: "+result.stderr[-4000:]+result.stdout[-2000:])
            return result
        def ready(name):
            run("up","-d","--no-build","--wait","--wait-timeout","120",project_name=name)
            run("exec","-T","desk","bybit-flow","doctor","--json","--no-network",project_name=name)
            version=json.loads(run("exec","-T","desk","bybit-flow","version",project_name=name).stdout)
            assert version["deployment"]["git_commit"]=="isolated-smoke"
            assert version["deployment"]["database"]["level"]=="009"
        try:
            run("build","desk")
            ready(project)
            marker="from bybit_flow.storage import Store; from pathlib import Path; s=Store(Path('/app/data')); s.put('backup_probe',{'software_test':True}); s.close()"
            run("exec","-T","desk","python","-c",marker)
            run("stop","desk")
            stopped=json.loads(run("ps","-a","--format","json").stdout)
            if isinstance(stopped,list):
                stopped=stopped[0]
            assert stopped["ExitCode"] in {0, 143},stopped
            logs=run("logs","--tail","50","desk").stdout
            assert "Application shutdown complete" in logs, logs[-3000:]
            backup=folder/"backup"
            run("cp","desk:/app/data",str(backup))
            with sqlite3.connect(backup/"research.sqlite") as db:
                assert db.execute("PRAGMA quick_check").fetchone()[0]=="ok"
                assert db.execute("SELECT count(*) FROM signals").fetchone()[0]==0
                assert db.execute("SELECT count(*) FROM migration_registry").fetchone()[0]==9
            db.close()
            run("create","desk",project_name=restored)
            run("cp",str(backup)+"/.","desk:/app/data",project_name=restored)
            run("run","--rm","--no-deps","--user","0","--entrypoint","chown","desk","-R","10001:10001","/app/data",project_name=restored)
            ready(restored)
            check="from bybit_flow.storage import Store; from pathlib import Path; s=Store(Path('/app/data')); assert s.get('backup_probe')=={'software_test':True}; assert s.db.execute('PRAGMA quick_check').fetchone()[0]=='ok'; assert s.db.execute('SELECT count(*) FROM signals').fetchone()[0]==0; s.close()"
            run("exec","-T","desk","python","-c",check,project_name=restored)
            print("PASS: isolated build, startup, health, offline doctor, version, clean stop, migrated backup and fresh-volume restore")
        finally:
            for name in (project,restored):
                run("down","--volumes",project_name=name,check=False)


if __name__=="__main__":
    main()
