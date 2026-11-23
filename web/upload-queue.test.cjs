const test=require('node:test');const assert=require('node:assert/strict');const Queue=require('./upload-queue.js');
const tick=()=>new Promise(resolve=>setImmediate(resolve));
const item=name=>({targetKey:name,file:{name,size:4}});
async function until(check){for(let n=0;n<200;n++){if(check())return;await tick();}assert.fail('queue did not reach expected state');}
test('files overlap, a new batch starts while first file is running, global cap is shared',async()=>{
 const sends=[],pending=[];let active=0,peak=0;const q=new Queue({concurrency:2,maxFiles:4,prepare:async value=>({count:4,send:async index=>{active++;peak=Math.max(peak,active);sends.push(value.file.name);await new Promise(r=>pending.push(r));active--;return 1;},complete:async()=>{}})});
 q.add([item('large')]);await until(()=>pending.length===2);q.add([item('new-a'),item('new-b')]);await tick();pending.shift()();await until(()=>sends.includes('new-a')||sends.includes('new-b'));assert.equal(peak,2);assert.equal(q.jobs.length,3);
 while(q.jobs.some(j=>j.state!=='done')){pending.splice(0).forEach(r=>r());await tick();}assert.equal(peak,2);assert.equal(q.activeParts,0);
});
test('pause/failure isolated to one file, retry reloads sparse received parts',async()=>{
 const pending=[];const stored=new Set();let preparations=0;const q=new Queue({concurrency:2,maxFiles:2,prepare:async value=>{preparations++;return {count:2,received:[...stored],uploadedBytes:stored.size,send:async(n,signal)=>{if(value.file.name==='bad')throw new Error('provider failed');await new Promise((resolve,reject)=>{pending.push(()=>{stored.add(n);resolve();});signal.addEventListener('abort',()=>reject(new DOMException('paused','AbortError')),{once:true});});return 1;},complete:async()=>{}};}});
 q.add([item('good'),item('bad')]);await until(()=>q.jobs[1].state==='error'&&pending.length>0);assert.equal(q.jobs[0].state,'uploading');q.pause(q.jobs[0].id);assert.equal(q.jobs[0].state,'paused');await until(()=>q.activeParts===0);pending.splice(0);q.resume(q.jobs[0].id);await until(()=>pending.length>0);while(q.jobs[0].state!=='done'){pending.splice(0).forEach(r=>r());await tick();}assert.equal(q.jobs[1].state,'error');assert.ok(preparations>=3);
});
test('empty files complete, duplicate targets do not overwrite running jobs',async()=>{let complete=0;const q=new Queue({prepare:async()=>({count:0,send:async()=>0,complete:async()=>{complete++;}})});q.add([item('zero'),item('zero')]);await until(()=>q.jobs[0].state==='done');assert.equal(q.jobs.length,1);assert.equal(complete,1);q.clearFinished();assert.equal(q.jobs.length,0);});
