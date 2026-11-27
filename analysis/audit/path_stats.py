"""What actually happened to price after each delivered alert (public Binance 1m/5m candles)."""
import json, collections, statistics as st, math
B='F:/Codex/bybitflow/analysis/audit/'
A={x['id']:x for x in json.load(open(B+'alerted.json'))}
P=json.load(open(B+'paths.json'))
rows=[]
for i,rec in P.items():
    x=A[i]; p=x['payload']
    m1=rec['m1'] if isinstance(rec['m1'],list) else []
    m5=rec['m5'] if isinstance(rec['m5'],list) else []
    if len(m1)<30: continue
    a=rec['a_ms']; sign=1 if p['direction']=='LONG' else -1
    bars=[b for b in m1 if b[0]>a]          # first full minute after the alert
    if not bars: continue
    last1=bars[-1][0]
    bars+= [b for b in m5 if b[0]>last1]     # extend with 5m bars (up to ~5 days)
    e=bars[0][1]                              # market entry: next minute open
    risk=sign*(e-p['stop']); prisk=sign*(p['entry']-p['stop'])
    if risk<=0 or prisk<=0:
        rows.append(dict(id=i,skip='stop already crossed at alert',q=p.get('quality'),hz=p.get('horizon_profile'),fam=p['family'])); continue
    dl=p.get('primary_tracking_deadline') or p.get('holding_deadline_ms') or a+4*3600e3
    def first(up_R,dn_R,until=None):
        """first barrier touched, in units of actual risk; same-bar tie -> stop (pessimistic)"""
        up=e+sign*up_R*risk; dn=e-sign*dn_R*risk
        for b in bars:
            if until and b[0]>until: return 'TIME', sign*(b[1]-e)/risk
            hi,lo=b[2],b[3]
            hit_dn=(lo<=dn) if sign==1 else (hi>=dn)
            hit_up=(hi>=up) if sign==1 else (lo<=up)
            if hit_dn: return 'DN',-dn_R
            if hit_up: return 'UP',up_R
        return 'OPEN', sign*(bars[-1][4]-e)/risk
    tpR=sign*(p['tp1']-e)/risk
    def ret(mins):
        t=a+mins*60000
        c=[b for b in bars if b[0]<=t]
        return sign*(c[-1][4]-e)/e*1e4 if c and bars[-1][0]>=t-300000 else None
    mfe=mae=0
    for b in bars:
        if b[0]>dl: break
        f=sign*((b[2] if sign==1 else b[3])-e)/risk; m=sign*((b[3] if sign==1 else b[2])-e)/risk
        mfe=max(mfe,f); mae=min(mae,m)
        if m<=-1: break
    rows.append(dict(id=i,q=p.get('quality'),tier=p.get('raw_tier'),hz=p.get('horizon_profile'),fam=p['family'],dir=p['direction'],
        tpR=tpR, stop_bps=risk/e*1e4, slip_R=sign*(e-p['entry'])/prisk,
        plan=first(tpR,1,dl), plan_nodl=first(tpR,1),
        b1=first(1,1), b15=first(1.5,1), b2=first(2,1), b3=first(3,1),
        wide1=first(1,2), mfe=mfe, mae=mae,
        r15=ret(15), r60=ret(60), r240=ret(240), r1440=ret(1440), created=p['created_ms']))
json.dump(rows,open(B+'path_rows.json','w'))
ok=[r for r in rows if 'skip' not in r]
print('alerts with price path:',len(ok),' already-through-stop at alert:',len(rows)-len(ok))
COST_BPS=2*5.5+2*2  # taker fee both sides + slippage both sides (project's own assumptions)
def barrier(name,key,up,dn,rs=ok):
    c=collections.Counter(r[key][0] for r in rs)
    res=c['UP']+c['DN']
    if not res: return
    pw=c['UP']/res; null=dn/(up+dn)
    se=math.sqrt(null*(1-null)/res); z=(pw-null)/se
    # realised R incl. open/time marks, minus cost in R
    R=[r[key][1]-COST_BPS/r['stop_bps'] for r in rs]
    print(f"{name:34s} n={len(rs):3d} up={c['UP']:3d} dn={c['DN']:3d} open/time={c['OPEN']+c['TIME']:3d}  P(up)={100*pw:5.1f}% vs random-walk {100*null:4.1f}% (z={z:+.1f})  net mean {st.mean(R):+.3f}R  total {sum(R):+.1f}R")
print('\n-- Symmetric and asymmetric barrier tests from market entry at the alert (no deadline, ~25h/5d of data) --')
barrier('+1R before -1R','b1',1,1); barrier('+1.5R before -1R','b15',1.5,1); barrier('+2R before -1R','b2',2,1); barrier('+3R before -1R','b3',3,1); barrier('+1R before -2R (2x wider stop)','wide1',1,2)
print('\n-- The plan as published (TP1 vs stop) --')
c=collections.Counter(r['plan'][0] for r in ok); print('within tracking deadline:',dict(c))
c=collections.Counter(r['plan_nodl'][0] for r in ok); print('no deadline          :',dict(c))
R=[r['plan'][1]-COST_BPS/r['stop_bps'] for r in ok]; print('published plan, exit at deadline mark, net of costs: mean %.3fR total %.1fR'%(st.mean(R),sum(R)))
print('median TP1 distance from market entry: %.2fR ; median stop distance %.0f bps ; cost = %.2fR of the median stop'%(st.median(r['tpR'] for r in ok),st.median(r['stop_bps'] for r in ok),COST_BPS/st.median(r['stop_bps'] for r in ok)))
print('entry slippage vs planned entry (R, + = worse): median %.2f  p75 %.2f'%(st.median(r['slip_R'] for r in ok), sorted(r['slip_R'] for r in ok)[3*len(ok)//4]))
print('MFE before stop/deadline (R): median %.2f ; share reaching >=1R %.0f%% >=2R %.0f%% >=3R %.0f%%'%(st.median(r['mfe'] for r in ok),100*st.mean(r['mfe']>=1 for r in ok),100*st.mean(r['mfe']>=2 for r in ok),100*st.mean(r['mfe']>=3 for r in ok)))
print('\n-- Directional edge with no stop/target: signed return after the alert, bps --')
for k,l in(('r15','15m'),('r60','1h'),('r240','4h'),('r1440','24h')):
    v=[r[k] for r in ok if r[k] is not None]
    se=st.pstdev(v)/math.sqrt(len(v)); print(f"{l:4s} n={len(v):3d} mean {st.mean(v):+6.1f} bps  ±{1.96*se:.1f}  median {st.median(v):+6.1f}  right-direction {100*st.mean(x>0 for x in v):.0f}%")
print('\n-- +1R before -1R by segment --')
for name,key in(('tier','tier'),('family','fam'),('horizon','hz'),('direction','dir')):
    for g in sorted({str(r[key]) for r in ok}):
        barrier(f'{name}={g}','b1',1,1,[r for r in ok if str(r[key])==g])
for lo,hi in((0,50),(50,65),(65,75),(75,101)):
    barrier(f'score {lo}-{hi}','b1',1,1,[r for r in ok if r['q'] is not None and lo<=r['q']<hi])
for lo,hi in((0,60),(60,100),(100,150),(150,1e9)):
    barrier(f'stop {lo}-{hi} bps','b1',1,1,[r for r in ok if lo<=r['stop_bps']<hi])
