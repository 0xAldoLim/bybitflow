import json, collections, statistics as st, math
L=json.load(open('F:/Codex/bybitflow/analysis/audit/labels.json'))
cols='sid stage ms policy cls outcome net_r complete q fam hz dir sym netrr accepted entry stop tp1 flowc regime'.split()
R=[dict(zip(cols,r)) for r in L]
print('rows',len(R)); print(collections.Counter((r['policy'],r['stage'],r['cls']) for r in R).most_common(40))
def summarize(rs,name):
    done=[r for r in rs if r['cls'] in('win','loss') and r['net_r'] is not None]
    if not done: print(name,'no resolved'); return
    w=sum(r['cls']=='win' for r in done); n=len(done); m=st.mean(r['net_r'] for r in done)
    se=st.pstdev([r['net_r'] for r in done])/math.sqrt(n) if n>1 else float('nan')
    print(f"{name:42s} n={n:5d} win={100*w/n:5.1f}%  meanR={m:+.3f} ±{1.96*se:.3f}  sumR={sum(r['net_r'] for r in done):+.0f}")
for pol in ('ohlc-path-v1','monitored-ohlc-v1','prints-v1','prints-recovered-v1'):
    for stage in ('generation','decision'):
        rs=[r for r in R if r['policy']==pol and r['stage']==stage]
        # one per signal
        seen={};
        for r in rs: seen[r['sid']]=r
        rs=list(seen.values())
        print('\n==',pol,stage,len(rs),'unique signals; all classes:',dict(collections.Counter(r['cls'] for r in rs)))
        summarize(rs,'ALL')
        for lo,hi in((0,35),(35,50),(50,65),(65,75),(75,85),(85,101)):
            summarize([r for r in rs if r['q'] is not None and lo<=r['q']<hi],f'score {lo}-{hi}')
        summarize([r for r in rs if r['q'] is None],'score none')
        for f in ('liquidity_sweep','range_rejection','trend_pullback','breakout_retest'): summarize([r for r in rs if r['fam']==f],f)
        for h in ('SHORT_INTRADAY','CORE_INTRADAY','SWING','EXTENDED_SWING',None): summarize([r for r in rs if r['hz']==h],str(h))
        for d in ('LONG','SHORT'): summarize([r for r in rs if r['dir']==d],d)
        summarize([r for r in rs if r['accepted']==1],'risk accepted'); summarize([r for r in rs if r['flowc']==1],'flow confirmed')
        # rank correlation score vs net_r
        done=[r for r in rs if r['cls'] in('win','loss') and r['q'] is not None]
        if len(done)>30:
            def rank(v):
                o=sorted(range(len(v)),key=lambda i:v[i]); rk=[0]*len(v)
                for i,j in enumerate(o): rk[j]=i
                return rk
            a=rank([r['q'] for r in done]); b=rank([r['net_r'] for r in done])
            ma,mb=st.mean(a),st.mean(b)
            rho=sum((x-ma)*(y-mb) for x,y in zip(a,b))/math.sqrt(sum((x-ma)**2 for x in a)*sum((y-mb)**2 for y in b))
            print(f'   Spearman(score, net_r) = {rho:+.3f} on n={len(done)}')
