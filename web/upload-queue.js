(function(root){
 'use strict';
 // One round-robin pool for every file. A large upload cannot reserve every
 // future slot, and adding a batch never replaces the currently running batch.
 class UploadQueue {
  constructor({prepare,onChange=()=>{},concurrency=4,maxFiles=4}){this.prepare=prepare;this.onChange=onChange;this.concurrency=concurrency;this.maxFiles=maxFiles;this.jobs=[];this.activeParts=0;this.cursor=0;this.serial=0;this.scheduled=false;}
  configure({concurrency=this.concurrency,maxFiles=this.maxFiles}){this.concurrency=Math.max(1,Math.min(16,concurrency));this.maxFiles=Math.max(1,Math.min(16,maxFiles));this.notify();this.pumpSoon();}
  add(items){const added=[];for(const item of items){let old=this.jobs.find(j=>j.targetKey===item.targetKey&&!['done','skipped'].includes(j.state));if(old){if(['paused','error'].includes(old.state)){old.item=item;this.resume(old.id);}added.push(old);continue;}const job={id:String(++this.serial),item,targetKey:item.targetKey,state:'queued',uploaded:0,inFlight:0,generation:0,error:'',cursor:0,plan:null};this.jobs.push(job);added.push(job);}this.notify();this.pumpSoon();return added;}
  pause(id){const job=this.jobs.find(j=>j.id===id);if(!job||!['queued','preparing','uploading','completing'].includes(job.state))return;job.generation++;job.controller?.abort();job.state='paused';this.notify();this.pumpSoon();}
  pauseAll(){for(const job of this.jobs)this.pause(job.id);}
  resume(id){const job=this.jobs.find(j=>j.id===id);if(!job||!['paused','error'].includes(job.state))return;job.generation++;job.state='queued';job.plan=null;job.error='';this.notify();this.pumpSoon();}
  clearFinished(){this.jobs=this.jobs.filter(j=>!['done','skipped'].includes(j.state));this.notify();}
  notify(){this.onChange(this);}
  pumpSoon(){if(this.scheduled)return;this.scheduled=true;queueMicrotask(()=>{this.scheduled=false;this.pump();});}
  alive(job,generation){return job.generation===generation;}
  fail(job,generation,error){if(!this.alive(job,generation))return;job.generation++;job.controller?.abort();job.error=error.message||String(error);job.state=error.name==='AbortError'?'paused':'error';this.notify();this.pumpSoon();}
  start(job){job.state='preparing';job.controller=new AbortController();const generation=++job.generation;job.plan=null;job.cursor=0;this.notify();Promise.resolve().then(()=>this.prepare(job.item,job.controller.signal,job)).then(plan=>{if(!this.alive(job,generation))return;if(!plan){job.state='skipped';this.notify();this.pumpSoon();return;}job.plan=plan;job.doneParts=new Set(plan.received||[]);job.uploaded=plan.uploadedBytes||0;job.state='uploading';this.notify();this.pumpSoon();}).catch(e=>this.fail(job,generation,e));}
  nextPart(job){while(job.cursor<job.plan.count){const n=job.cursor++;if(!job.doneParts.has(n))return n;}return null;}
  finish(job){job.state='completing';const generation=job.generation;this.notify();Promise.resolve().then(()=>job.plan.complete(job.controller.signal)).then(()=>{if(!this.alive(job,generation))return;job.uploaded=job.item.file.size;job.state='done';this.notify();this.pumpSoon();}).catch(e=>this.fail(job,generation,e));}
  send(job,index){const generation=job.generation;this.activeParts++;job.inFlight++;this.notify();Promise.resolve().then(()=>job.plan.send(index,job.controller.signal)).then(size=>{if(!this.alive(job,generation))return;job.doneParts.add(index);job.uploaded+=size;this.notify();}).catch(e=>this.fail(job,generation,e)).finally(()=>{job.inFlight--;this.activeParts--;this.notify();this.pumpSoon();});}
  pump(){
   let running=this.jobs.filter(j=>['preparing','uploading','completing'].includes(j.state)).length;
   for(const job of this.jobs){if(running>=this.maxFiles)break;if(job.state==='queued'&&job.inFlight===0){this.start(job);running++;}}
   while(this.activeParts<this.concurrency){
    for(const job of this.jobs){if(job.state==='uploading')while(job.cursor<job.plan.count&&job.doneParts.has(job.cursor))job.cursor++;}
    const candidates=this.jobs.filter(j=>j.state==='uploading'&&j.cursor<j.plan.count&&j.inFlight<(j.plan.parallelism||this.concurrency));if(!candidates.length)break;
    const least=Math.min(...candidates.map(j=>j.inFlight));for(let checked=0;checked<this.jobs.length;checked++){const job=this.jobs[this.cursor++%this.jobs.length];if(!candidates.includes(job)||job.inFlight!==least)continue;this.send(job,this.nextPart(job));break;}
   }
   for(const job of this.jobs){if(job.state==='uploading'&&job.inFlight===0&&job.cursor>=job.plan.count)this.finish(job);}
  }
 }
 if(typeof module!=='undefined'&&module.exports)module.exports=UploadQueue;else root.MaxStoreUploadQueue=UploadQueue;
})(typeof globalThis==='undefined'?this:globalThis);
