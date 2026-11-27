"""Which recorded inputs relate to what price did after a delivered alert?

Outcome: +1R before -1R from market entry (path_rows.json, key b1) and the signed
1h/4h return expressed in units of the published stop distance. 367 alerts and
~40 features means several |z|>2 results are expected by chance; treat single
hits as hypotheses, not findings.
"""
import json, math, statistics as st, datetime
B='F:/Codex/bybitflow/analysis/audit/'
A={x['id']:x for x in json.load(open(B+'alerted.json'))}
P=json.load(open(B+'paths.json'))
rows=[r for r in json.load(open(B+'path_rows.json')) if 'skip' not in r]
def g(d,*ks):
    for k in ks:
        if not isinstance(d,dict): return None
        d=d.get(k)
    return d
F=[]
for r in rows:
    x=A[r['id']]; p=x['payload']; e=p['evidence']; sign=1 if p['direction']=='LONG' else -1
    f={}
    sc=p.get('score_components') or e.get('score_components') or {}
    for k,v in sc.items():
        if isinstance(v,dict) and v.get('weight'):
            f['score:'+k]=v.get('score', 100*v.get('earned',0)/v['weight'])
    for grp in ('evidence','tradeability'):
        for k,v in (g(e,'v22',grp,'components') or {}).items():
            if isinstance(v,dict) and isinstance(v.get('score'),(int,float)): f[f'v22:{k}']=v['score']
    f['setup score']=p.get('quality')
    h1=e.get('setup_features') or e.get('h1') or {}; h4=e.get('context_features') or e.get('h4') or {}
    atr=h1.get('atr')
    if atr: f['stop distance / 1h ATR']=abs(p['entry']-p['stop'])/atr; f['target distance / 1h ATR']=abs(p['tp1']-p['entry'])/atr
    f['stop distance bps']=r['stop_bps']; f['planned reward:risk']=r['tpR']
    reg=h4.get('regime') or p.get('regime')
    f['with 4h trend (1) / range (0) / against (-1)']= (1 if reg==('trending up' if sign>0 else 'trending down') else -1 if reg in('trending up','trending down') else 0)
    for nm,d in (('4h',h4),('1h',h1)):
        if d.get('slope') is not None: f[f'{nm} slope in trade direction']=sign*d['slope']
        if d.get('efficiency') is not None: f[f'{nm} trend efficiency']=d['efficiency']
        if d.get('ma20') and p['entry']: f[f'{nm} price vs MA20 in trade direction']=sign*(p['entry']/d['ma20']-1)*1e4
    pre=P[r['id']].get('pre'); a=P[r['id']]['a_ms']
    if isinstance(pre,list):
        c=[b for b in pre if b[0]+3600000<=a]
        for n in (1,4,24,72):
            if len(c)>n: f[f'prior {n}h return in trade direction (bps)']=sign*(c[-1][4]/c[-1-n][4]-1)*1e4
        if len(c)>25:
            rng=[(b[2]-b[3])/b[4] for b in c[-24:]]; f['24h mean hourly range (bps)']=st.mean(rng)*1e4
            hi=max(b[2] for b in c[-24:]); lo=min(b[3] for b in c[-24:])
            if hi>lo: f['position in 24h range, trade direction (0=buy low / sell high)']=((p['entry']-lo)/(hi-lo) if sign>0 else (hi-p['entry'])/(hi-lo))
    fl=e.get('flow') or {}
    if fl.get('available'):
        f['flow delta % in trade direction']=sign*(fl.get('delta_pct') or 0)
        f['flow window traded notional (USD)']=(fl.get('buy_notional') or 0)+(fl.get('sell_notional') or 0)
    bk=e.get('book') or {}
    for k in ('obi_touch','obi_5bps','obi_25bps','obi_persistence'):
        if bk.get(k) is not None: f[f'book {k} in trade direction']=sign*bk[k]
    if bk.get('spread_bps') is not None: f['spread bps']=bk['spread_bps']
    fr=g(e,'derivatives','funding_rate')
    if fr is not None: f['funding paid by this side (bps)']=sign*fr*1e4
    f['data coverage']=p.get('data_coverage')
    f['seconds from creation to alert']=(a-p['created_ms'])/1000
    f['hour UTC']=datetime.datetime.fromtimestamp(a/1000,datetime.UTC).hour
    f['BTC or ETH (1) vs alt (0)']=int(p['symbol'] in('BTCUSDT','ETHUSDT'))
    f['long (1) / short (0)']=int(sign>0)
    sv=e.get('stop_viability') or {}
    if sv.get('stop_noise_ratio') is not None: f['stop / ordinary noise']=sv['stop_noise_ratio']
    y=dict(win=1.0 if r['b1'][0]=='UP' else 0.0, r60=(r['r60'] or 0)/r['stop_bps'], r240=(r['r240'] or 0)/r['stop_bps'], mfe=r['mfe'])
    F.append((f,y))
def rank(v):
    o=sorted(range(len(v)),key=lambda i:v[i]); rk=[0.0]*len(v); i=0
    while i<len(o):
        j=i
        while j+1<len(o) and v[o[j+1]]==v[o[i]]: j+=1
        for k in range(i,j+1): rk[o[k]]=(i+j)/2
        i=j+1
    return rk
def spear(a,b):
    a,b=rank(a),rank(b); ma,mb=st.mean(a),st.mean(b)
    den=math.sqrt(sum((x-ma)**2 for x in a)*sum((y-mb)**2 for y in b))
    return sum((x-ma)*(y-mb) for x,y in zip(a,b))/den if den else 0
names=sorted({k for f,_ in F for k in f})
out=[]
for n in names:
    pts=[(f[n],y) for f,y in F if isinstance(f.get(n),(int,float))]
    if len(pts)<40 or len({v for v,_ in pts})<2: continue
    xs=[v for v,_ in pts]
    rw=spear(xs,[y['win'] for _,y in pts]); r4=spear(xs,[y['r240'] for _,y in pts])
    z=rw*math.sqrt(len(pts)-1)
    s=sorted(pts,key=lambda t:t[0]); k=len(s)//3
    lo=st.mean(y['win'] for _,y in s[:k]); mid=st.mean(y['win'] for _,y in s[k:2*k]); hi=st.mean(y['win'] for _,y in s[2*k:])
    out.append((abs(z),n,len(pts),rw,z,r4,lo,mid,hi,s[k][0],s[2*k][0]))
print(f"{'feature':62s} {'n':>4} {'rho(win)':>8} {'z':>5} {'rho(4h R)':>9}   win% by tercile low/mid/high   (tercile cuts)")
for _,n,c,rw,z,r4,lo,mid,hi,c1,c2 in sorted(out,reverse=True):
    print(f"{n[:62]:62s} {c:4d} {rw:+8.3f} {z:+5.1f} {r4:+9.3f}   {100*lo:5.1f} {100*mid:5.1f} {100*hi:5.1f}   ({c1:.3g}, {c2:.3g})")
json.dump([dict(f=f,y=y) for f,y in F],open(B+'features.json','w'))
