"use strict";
const assert = require("node:assert/strict");
const fs = require("node:fs");
const vm = require("node:vm");
const source = fs.readFileSync("src/bybit_flow/static/app.js", "utf8").split('window.addEventListener("hashchange"')[0];
const base = () => ({scanner:{state:"collecting",deep_selected:30,eligible:72,deep_symbols:[]},exchange:"binance",at_ms:10000,
 health:{recorder:true,queued:0,records_written:1234567,recorder_metrics:{state:"NORMAL",events_dropped:0}},
 storage:{used_bytes:27e9,budget_bytes:30e9},warnings:[],decisions:{opportunities:[],active:[],blocked:[]},signals:[],watchlist:[]});
const forward = {label:"Research",opportunities:0,policies:{}};
class Element {
 constructor(){this.innerHTML="";this.textContent="";this.attrs={};this.events={};}
 addEventListener(name,fn){this.events[name]=fn;}
 setAttribute(name,value){this.attrs[name]=value;}
 append(){}
}
function harness(route="overview", values={}) {
 const nodes=Object.fromEntries(["error","refresh-state","view","status","system-summary","clock","runtime-warnings","title"].map(id=>["#"+id,new Element()]));
 const fixtures={overview:base(),forward,ml:{models:[],history:[],status:"STALE",summary_at_ms:1000},...values};
 const calls=[];
 const state={nodes,fixtures,calls,detail:null,expanded:false};
 const document={activeElement:{tagName:"BODY"},querySelector:s=>s==="#view details[open]"?(state.expanded?{}:null):nodes[s]||null,
 querySelectorAll:s=>s==="details[data-report]"&&state.detail?[state.detail]:[],createElement:()=>new Element()};
 const context=vm.createContext({document,location:{hash:"#"+route},AbortSignal,Map,Date,console,
 fetch:async url=>{const path=url.slice(5);calls.push(path);let data=fixtures[path];if(typeof data==="function")return data();
 if(data instanceof Error)throw data;
 if(data===undefined)throw Error("Unexpected request: "+path);
 return {ok:true,status:200,text:async()=>JSON.stringify(data)};}});
 vm.runInContext(source,context);
 state.run=()=>vm.runInContext("render()",context);
 state.background=()=>vm.runInContext("render(true)",context);
 state.evaluate=js=>vm.runInContext(js,context);
 return state;
}
(async()=>{
 const summary=harness();await summary.run();
 assert.match(summary.nodes["#system-summary"].innerHTML,/1,234,567/);
 assert.match(summary.nodes["#system-summary"].innerHTML,/Selected markets/);
 assert.doesNotMatch(summary.nodes["#view"].innerHTML,/RECORDED EVENTS/);
 assert.equal(summary.nodes["#status"].textContent,"collecting");
 assert.equal(summary.evaluate('number("invalid")'),"—");
 const warnings=summary.evaluate('warningSummary([{severity:"error",title:"Critical",message:"Live feeds lost",action:"Check feeds"},{severity:"warning",title:"Advisory",message:"Delayed",action:"Retry"}])');
 assert.ok(warnings.indexOf("Critical")<warnings.indexOf('<details class="warning-summary">'));
 assert.match(warnings,/1 operational advisory/);
 const degraded=harness("overview",{forward:new Error("report offline")});await degraded.run();
 assert.match(degraded.nodes["#view"].innerHTML,/Report temporarily unavailable/);
 assert.match(degraded.nodes["#system-summary"].innerHTML,/1,234,567/);
 assert.equal(degraded.nodes["#status"].textContent,"collecting");
 const ml=harness("ml",{ml:{models:[{id:"partial-model",report:{}}],history:[],status:"STALE",summary_at_ms:1000}});
 await ml.run();
 assert.match(ml.nodes["#view"].innerHTML,/Learning data/);
 assert.match(ml.nodes["#view"].innerHTML,/ML summary is stale/);
 assert.deepEqual(ml.calls,["overview","ml"]); // no heavyweight horizon request on navigation
 assert.equal(ml.nodes["#error"].textContent,"");
 const health=harness("health");await health.run();
 assert.match(health.nodes["#view"].innerHTML,/Collection health/);
 assert.deepEqual(health.calls,["overview"]); // no automatic extended diagnostics
 const content=new Element();
 ml.detail={open:true,dataset:{report:"horizons"},querySelector:()=>content,addEventListener(name,fn){this.toggle=fn;}};
 ml.fixtures.horizons={profiles:{}};ml.evaluate("bindReports()");await ml.detail.toggle();await ml.detail.toggle();
 assert.equal(ml.calls.filter(x=>x==="horizons").length,1);
 assert.match(content.innerHTML,/cached for this page session/);
 ml.expanded=true;const view=ml.nodes["#view"].innerHTML;ml.fixtures.overview.health.records_written=2000000;
 await ml.background();assert.equal(ml.nodes["#view"].innerHTML,view);assert.match(ml.nodes["#system-summary"].innerHTML,/2,000,000/);
 const stale=harness();await stale.run();const previous=stale.nodes["#view"].innerHTML;
 stale.fixtures.overview=()=>({ok:false,status:503,text:async()=>"<html>Temporarily unavailable</html>"});await stale.run();
 assert.equal(stale.nodes["#view"].innerHTML,previous);assert.match(stale.nodes["#error"].textContent,/HTTP 503/);
 assert.equal(stale.nodes["#view"].attrs["aria-busy"],"false");
 const race=harness();let rejectOld;
 race.fixtures.overview=()=>new Promise((resolve,reject)=>{rejectOld=reject;});const old=race.run();
 race.fixtures.overview=base();await race.run();rejectOld(Error("old request failed"));await old;
 assert.equal(race.nodes["#status"].textContent,"collecting");assert.equal(race.nodes["#error"].textContent,"");
 const outcomes=harness();
 assert.equal(outcomes.evaluate('priceResult({state:"RESOLVED",evidence:{primary_outcome:"TARGET",terminal_event:{method:"LIVE_EXECUTED_TRADE"}}})'),"TP1 REACHED");
 assert.equal(outcomes.evaluate('priceResult({state:"INVALIDATED",evidence:{primary_outcome:"STOP",terminal_event:{method:"CLOSED_1M_OHLC"}}})'),"STOP REACHED");
 assert.equal(outcomes.evaluate('priceResult({state:"RESOLVED",evidence:{primary_outcome:"TARGET",terminal_event:{method:"CLOSED_1M_OHLC",ambiguous:true}}})'),null);
 console.log("Dashboard regressions passed: isolated report failure, lazy/coalesced reports, partial models, stale refreshes, visible counts, and truthful outcomes.");
})().catch(error=>{console.error(error);process.exitCode=1;});
