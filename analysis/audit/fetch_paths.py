"""Fetch public Binance USDT-M 1m/5m klines after each delivered alert (read-only public data)."""
import json, time, urllib.request, urllib.parse, os
B='F:/Codex/bybitflow/analysis/audit/'
d=json.load(open(B+'alerted.json'))
out=json.load(open(B+'paths.json')) if os.path.exists(B+'paths.json') else {}
def get(sym,iv,start,limit=1500):
    u='https://fapi.binance.com/fapi/v1/klines?'+urllib.parse.urlencode(dict(symbol=sym,interval=iv,startTime=start,limit=limit))
    for a in range(4):
        try:
            with urllib.request.urlopen(urllib.request.Request(u,headers={'User-Agent':'research-audit'}),timeout=20) as r:
                return [[k[0],float(k[1]),float(k[2]),float(k[3]),float(k[4])] for k in json.load(r)]
        except Exception as e:
            err=str(e); time.sleep(1.5*(a+1))
    return {'error':err}
for n,x in enumerate(d):
    if x['id'] in out and not isinstance(out[x['id']].get('m1'),dict): continue
    p=x['payload']; a=[t for t in x['transitions'] if t[1]=='ALERTED']
    a_ms=a[0][0] if a else p['created_ms']
    sym=p['evidence'].get('exchange_symbol') or p['symbol']
    if p.get('source')!='binance': sym=p['symbol']
    start=a_ms//60000*60000
    rec={'a_ms':a_ms,'sym':sym,'m1':get(sym,'1m',start)}
    rec['m5']=get(sym,'5m',start//300000*300000)
    rec['pre']=get(sym,'1h',start-200*3600000,200)
    out[x['id']]=rec
    if n%25==0:
        print(n,sym,len(rec['m1']) if isinstance(rec['m1'],list) else rec['m1'],flush=True); json.dump(out,open(B+'paths.json','w'))
    time.sleep(0.35)
json.dump(out,open(B+'paths.json','w'))
print('done',len(out),sum(1 for v in out.values() if isinstance(v['m1'],dict)),'errors')
