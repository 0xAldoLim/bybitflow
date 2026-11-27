import json, collections, datetime, statistics as st
d=json.load(open('F:/Codex/bybitflow/analysis/audit/alerted.json'))
rows=[]
for x in d:
    p=x['payload']; ev=p.get('evidence',{})
    trs=x['transitions']
    alerted=[t for t in trs if t[1]=='ALERTED']
    term=[t for t in trs if t[1] in('INVALIDATED','RESOLVED','EXPIRED')]
    last=term[-1] if term else None
    reason=(last[2] if last else 'OPEN')
    if last is None: out='OPEN'
    elif last[1]=='RESOLVED': out='TARGET'
    elif last[1]=='INVALIDATED' and ('stop' in reason.lower() or 'STOP' in reason): out='STOP'
    elif last[1]=='INVALIDATED': out='INVALIDATED_OTHER'
    else: out='EXPIRED'
    sign=1 if p['direction']=='LONG' else -1
    risk=sign*(p['entry']-p['stop']); rew=sign*(p['tp1']-p['entry'])
    a_ms=alerted[0][0] if alerted else None
    lp=ev.get('latest_observed_price')
    if isinstance(lp,dict): lp=lp.get('price')
    rows.append(dict(id=x['id'],sym=p['symbol'],dir=p['direction'],fam=p['family'],hz=p.get('horizon_profile'),
      q=p.get('quality'),tier=p.get('raw_tier'),out=out,reason=reason,rr=rew/risk if risk>0 else None,
      stop_pct=risk/p['entry']*100, a_ms=a_ms, t_ms=last[0] if last else None,
      entry_obs=ev.get('observed_entry_ms'), netrr=(p.get('risk') or {}).get('net_rr'),
      src=p.get('source'), ver=p.get('version','').split(':')[1] if ':' in p.get('version','') else '',
      po=ev.get('primary_outcome'), lp=lp, entry=p['entry'], stop=p['stop'], tp1=p['tp1'], sign=sign, created=p['created_ms']))
json.dump(rows,open('F:/Codex/bybitflow/analysis/audit/rows.json','w'))
def tab(key,rs=rows):
    c=collections.defaultdict(collections.Counter)
    for r in rs: c[key(r)][r['out']]+=1
    print(f"{'':28s} n   TARGET STOP EXPIRED OTHER  win%ofResolved  estR(tgt*rr - stop)")
    for k in sorted(c, key=str):
        v=c[k]; n=sum(v.values()); res=v['TARGET']+v['STOP']
        rr=[r['rr'] for r in rs if key(r)==k and r['out']=='TARGET' and r['rr']]
        R=sum(rr)-v['STOP']
        print(f"{str(k):28s} {n:3d} {v['TARGET']:6d} {v['STOP']:4d} {v['EXPIRED']:7d} {v['INVALIDATED_OTHER']+v['OPEN']:5d}  {100*v['TARGET']/res if res else float('nan'):6.1f}%   {R:+.1f}R")
ts=[r['created'] for r in rows]
f=lambda ms: datetime.datetime.utcfromtimestamp(ms/1000).strftime('%Y-%m-%d')
print('alerts',len(rows),'from',f(min(ts)),'to',f(max(ts)))
print('\nALL'); tab(lambda r:'all')
print('\nBY TIER'); tab(lambda r:r['tier'])
print('\nBY SCORE BAND'); tab(lambda r: 'none' if r['q'] is None else f"{int(r['q']//10*10):02d}-{int(r['q']//10*10+10)}")
print('\nBY FAMILY'); tab(lambda r:r['fam'])
print('\nBY HORIZON'); tab(lambda r:r['hz'])
print('\nBY DIRECTION'); tab(lambda r:r['dir'])
print('\nBY POLICY'); tab(lambda r:r['ver'])
print('\nBY MONTH'); tab(lambda r:f(r['created'])[:7])
print('\nBY SOURCE'); tab(lambda r:r['src'])
stops=[r for r in rows if r['out']=='STOP' and r['a_ms'] and r['t_ms']]
mins=sorted((r['t_ms']-r['a_ms'])/60000 for r in stops)
print('\nminutes alert->stop: median %.0f  p25 %.0f p75 %.0f  <=15m: %d  <=60m: %d of %d'%(st.median(mins),mins[len(mins)//4],mins[3*len(mins)//4],sum(m<=15 for m in mins),sum(m<=60 for m in mins),len(mins)))
sp=sorted(r['stop_pct'] for r in rows)
print('stop distance %% of price: median %.2f p25 %.2f p75 %.2f min %.2f'%(st.median(sp),sp[len(sp)//4],sp[3*len(sp)//4],sp[0]))
for o in('STOP','TARGET','EXPIRED'):
    v=[r['stop_pct'] for r in rows if r['out']==o]
    print(o,'median stop%% %.2f'%st.median(v),'median planned RR %.2f'%st.median([r['rr'] for r in rows if r['out']==o and r['rr']]), 'median score %.1f'%st.median([r['q'] for r in rows if r['out']==o and r['q'] is not None]))
print(collections.Counter(r['reason'] for r in rows).most_common(20))
print('entry observed:',sum(1 for r in rows if r['entry_obs']), 'stop w/ entry obs', sum(1 for r in stops if r['entry_obs']))
print('primary_outcome sample', json.dumps(next((r['po'] for r in rows if r['po']),None))[:800])
