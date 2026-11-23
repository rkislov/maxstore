'use strict';
const $ = s => document.querySelector(s);
function setTheme(dark){document.documentElement.dataset.theme=dark?'dark':'light';$('#theme-toggle').textContent=dark?'Светлая тема':'Тёмная тема';$('#theme-toggle').setAttribute('aria-pressed',String(dark));}
setTheme(localStorage.getItem('maxstore-theme')==='dark');
$('#theme-toggle').onclick=()=>{const dark=document.documentElement.dataset.theme!=='dark';localStorage.setItem('maxstore-theme',dark?'dark':'light');setTheme(dark);};
const icons = {
 folder:'<path d="M3 7a2 2 0 0 1 2-2h5l2 2h7a2 2 0 0 1 2 2v10H3Z"/>',
 chart:'<path d="M4 19h16M7 15v-4m5 4V5m5 10V8"/>',
 layers:'<path d="m12 3 10 5-10 5L2 8Zm-9 9 9 5 9-5M3 16l9 5 9-5"/>',
 terminal:'<rect x="3" y="4" width="18" height="16" rx="3"/><path d="m7 9 3 3-3 3m6 0h4"/>',
 shield:'<path d="m12 3 8 3v6c0 5-8 9-8 9s-8-4-8-9V6Z"/><path d="m8 12 3 3 5-6"/>',
 logout:'<path d="M9 4H4v16h5m5-12 4 4-4 4m-6-4h13"/>',
 plus:'<path d="M12 5v14M5 12h14"/>',
 refresh:'<path d="M20 7v5h-5M4 17v-5h5"/><path d="M6 7a7 7 0 0 1 12-1l2 6M4 12l2 6a7 7 0 0 0 12-1"/>',
 trash:'<path d="M4 7h16M9 7V4h6v3M6 7l1 13h10l1-13M10 10v6m4-6v6"/>',
 search:'<circle cx="10" cy="10" r="6"/><path d="m15 15 5 5"/>',
 upload:'<path d="M12 16V3m-5 5 5-5 5 5M4 15v6h16v-6"/>',
 download:'<path d="M12 3v13m-5-5 5 5 5-5M4 17v4h16v-4"/>',
 file:'<path d="M14 3H5v18h14V8Zm0 0v6h5M8 13h8m-8 4h5"/>',
 database:'<ellipse cx="12" cy="5" rx="8" ry="3"/><path d="M4 5v14c0 4 16 4 16 0V5M4 12c0 4 16 4 16 0"/>',
 link:'<path d="m9 15 6-6m-7 4-2 2a4 4 0 0 0 6 6l3-3M9 6l3-3a4 4 0 0 1 6 6l-2 2"/>'
};
const icon = name => `<svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">${icons[name] || icons.file}</svg>`;
document.querySelectorAll('[data-icon]').forEach(el => el.innerHTML = icon(el.dataset.icon));
const esc = s => String(s ?? '').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const bytes = n => { if (!n) return '0 Б'; const i = Math.min(Math.floor(Math.log(n) / Math.log(1024)),4);return `${new Intl.NumberFormat('ru',{maximumFractionDigits:1}).format(n / 1024 ** i)} ${['Б','КБ','МБ','ГБ','ТБ'][i]}`; };
const providerName = p => ({local:'Локальное',telegram:'Telegram',max:'MAX'}[p] || p);
sessionStorage.removeItem('maxstore-token');
let status, objects = [], bucket = '', view = 'objects', toastTimer, refreshTimer;
function toast(message) { $('#toast').textContent = message; $('#toast').classList.remove('hidden'); clearTimeout(toastTimer);toastTimer = setTimeout(() => $('#toast').classList.add('hidden'),6000); }
function showLogin(){transferQueue.pauseAll(); $('#app').classList.add('hidden');$('#login').classList.remove('hidden');$('#password').value=''; }
async function logout(){ transferQueue.pauseAll();try{await fetch('/api/auth/logout',{method:'POST',headers:{'X-Maxstore-Request':'1'}});}finally{showLogin();} }
async function api(path, options={}){
 const res=await fetch(`/api/${path}`,{...options,credentials:'same-origin',headers:{'X-Maxstore-Request':'1',...options.headers}});
 if(!res.ok){const body=await res.json().catch(()=>({}));const error=new Error(body.error||`Ошибка ${res.status}`);error.status=res.status;if(res.status===401)showLogin();throw error;}
 return res.status===204?null:res.json();
}
function objectURL(b,key){return `/api/objects?${new URLSearchParams({bucket:b,key})}`;}
async function load(){
 status=await api('status');const buckets=status.buckets;
 if(!buckets.some(b=>b.name===bucket))bucket=buckets[0]?.name || '';
 $('#bucket-select').innerHTML=buckets.length ? buckets.map(b=>`<option value="${esc(b.name)}">${esc(b.name)}</option>`).join(''):'<option>Нет бакетов</option>';
 $('#bucket-select').value=bucket;$('#bucket-select').disabled=!buckets.length;
 $('#provider-select').innerHTML=status.providers.map(p=>`<option value="${esc(p)}">${providerName(p)}</option>`).join('');$('#provider-select').value=status.default_provider;
 $('#upload').disabled=!bucket;$('#delete-bucket').disabled=!bucket||hasPendingBucket(bucket);
 const maximum=status.upload_concurrency||1;const preference=Number(localStorage.getItem('maxstore-concurrency')||maximum);
 $('#transfer-concurrency').innerHTML=[...new Set([1,2,4,8,16,maximum])].filter(n=>n<=maximum).sort((a,b)=>a-b).map(n=>`<option value="${n}">${n}</option>`).join('');$('#transfer-concurrency').value=String(Math.min(preference,maximum));if(!$('#transfer-concurrency').value)$('#transfer-concurrency').value=String(maximum);$('#transfer-concurrency').disabled=false;$('#files-concurrency').value=localStorage.getItem('maxstore-files-concurrency')||'4';if(!$('#files-concurrency').value)$('#files-concurrency').value='4';transferQueue.configure({concurrency:Number($('#transfer-concurrency').value),maxFiles:Number($('#files-concurrency').value)});$('#download-concurrency').textContent=`Чтение: до ${status.download_concurrency||1} потоков`;
 renderStats();renderProviders();renderSettings();renderOverview();await loadObjects();
}
async function loadObjects(){
 const requested=bucket;const data=bucket?await api(`buckets/${encodeURIComponent(bucket)}`):[];if(requested!==bucket)return;objects=data;
 $('#bucket-provider').textContent=bucket?`${providerName(status.buckets.find(b=>b.name===bucket)?.provider)}`:'Выберите или создайте бакет';renderFiles();
}
function renderStats(){
 const stats=[['Объём данных',bytes(status.bytes),'До шифрования','database'],['Объекты',status.objects,'Во всех бакетах','file'],['Бакеты',status.buckets.length,'Изолированные пространства','folder'],['Провайдеры',status.providers.length,'Настроено на сервере','layers']];
 $('#stats').innerHTML=stats.map(([name,value,sub,i])=>`<div class="stat"><div class="flex items-center justify-between gap-2 text-sm text-slate-500"><span>${name}</span><span class="text-violet-400">${icon(i)}</span></div><div class="mt-4 text-3xl font-semibold tracking-tight">${value}</div><div class="mt-2 text-xs text-slate-400">${sub}</div></div>`).join('');
}
function renderFiles(){
 const term=$('#search').value.toLowerCase();const rows=objects.filter(o=>o.key.toLowerCase().includes(term));const sorting=$('#sort').value;
 rows.sort((a,b)=>sorting==='name'?a.key.localeCompare(b.key):sorting==='size'?b.size-a.size:new Date(b.created)-new Date(a.created));
 $('#files').innerHTML=rows.map(o=>`<tr class="border-t border-slate-100 hover:bg-slate-50/70"><td class="p-4 pl-6"><div class="flex items-center gap-3"><span class="rounded-lg bg-violet-50 p-2.5 text-brand">${icon('file')}</span><div class="min-w-40"><button class="file-detail max-w-sm truncate text-left font-medium hover:text-brand" data-key="${esc(o.key)}" title="${esc(o.key)}">${esc(o.key)}</button><div class="mt-1 text-xs text-slate-400">${esc(o.type)}</div></div></div></td><td class="whitespace-nowrap p-4 text-slate-500">${bytes(o.size)}</td><td class="p-4"><span class="badge">${providerName(o.provider)}</span></td><td class="whitespace-nowrap p-4 text-slate-500">${new Date(o.created).toLocaleDateString('ru-RU',{day:'2-digit',month:'short',year:'numeric'})}</td><td class="p-4"><div class="flex gap-1"><button class="download btn p-2 text-slate-400 hover:bg-violet-50 hover:text-brand" data-key="${esc(o.key)}" aria-label="Скачать ${esc(o.key)}">${icon('download')}</button><button class="delete btn p-2 text-slate-400 hover:bg-red-50 hover:text-red-600" data-key="${esc(o.key)}" aria-label="Удалить ${esc(o.key)}">${icon('trash')}</button></div></td></tr>`).join('');
 $('#empty').classList.toggle('hidden',rows.length>0);
 $('#empty').innerHTML=`<div class="mx-auto mb-5 flex h-16 w-16 items-center justify-center rounded-2xl bg-violet-50 text-brand">${icon(rows.length?'search':'folder')}</div><h2 class="text-lg font-semibold">${!bucket?'Начните с первого бакета':term?'Ничего не найдено':'Здесь будут ваши файлы'}</h2><p class="mx-auto mt-2 max-w-sm text-sm leading-6 text-slate-400">${!bucket?'Создайте бакет, выберите провайдер и загрузите первый файл.':term?'Попробуйте другое название или очистите поиск.':'Загрузите файлы кнопкой выше или перетащите их в эту область.'}</p>`;
 $('#file-count').textContent=`${rows.length} из ${objects.length} объектов · ${bytes(objects.reduce((n,o)=>n+o.size,0))}`;
}
function renderProviders(){
 $('#view-providers').innerHTML=`<div class="grid gap-5 xl:grid-cols-3">${['telegram','max','local'].map(p=>{const enabled=status.providers.includes(p);return `<article class="panel p-6"><div class="flex items-center justify-between"><span class="flex h-12 w-12 items-center justify-center rounded-xl ${p==='telegram'?'bg-sky-50 text-sky-500':p==='max'?'bg-violet-50 text-brand':'bg-slate-100 text-slate-500'}">${icon(p==='local'?'database':'layers')}</span><span class="badge">${enabled?'Настроен':'Не настроен'}</span></div><h2 class="mt-5 text-xl font-semibold">${providerName(p)}</h2><p class="mt-3 text-sm leading-6 text-slate-500">${p==='telegram'?'Зашифрованные части отправляются документами в ваш чат или приватный канал через Bot API.':p==='max'?'Части загружаются как файловые вложения и закрепляются отправкой сообщения в чат MAX.':'Файлы сохраняются на диске сервера. Можно начать работу без подключения мессенджеров.'}</p><div class="mt-5 border-t border-slate-100 pt-4 text-xs leading-6 text-slate-400">${p==='local'?'Каталог: DATA_DIR/chunks':`Переменные: ${p.toUpperCase()}_BOT_TOKEN<br>${p.toUpperCase()}_CHAT_ID`}</div></article>`}).join('')}</div><div class="panel mt-6 p-6"><h2 class="font-semibold">Доступность зависит от провайдера</h2><p class="mt-2 text-sm leading-7 text-slate-500">Статус «Настроен» означает наличие настроек, а не проверку связи. Данные мессенджеров могут стать недоступны при удалении сообщений, потере бота или изменении правил API. Храните отдельную резервную копию важных файлов, metadata.db и master.key.</p><p class="mt-3 text-sm text-slate-500">Частей в очереди удаления: <strong>${status.pending_gc}</strong>. Очередь обрабатывается раз в минуту. Telegram может запрещать удаление сообщений старше 48 часов.</p></div>`;
}
function renderOverview(){
 $('#view-overview').innerHTML=`<div class="grid gap-6 lg:grid-cols-2"><div class="panel p-6"><h2 class="text-lg font-semibold">Бакеты</h2>${status.buckets.length?status.buckets.map(b=>`<button class="open-bucket flex w-full items-center justify-between border-b border-slate-100 py-4 text-sm hover:text-brand" data-bucket="${esc(b.name)}"><span class="flex items-center gap-3">${icon('folder')}${esc(b.name)}</span><span class="badge">${providerName(b.provider)}</span></button>`).join(''):'<p class="py-10 text-center text-sm text-slate-400">Бакетов пока нет. Создайте первый, чтобы начать.</p>'}</div><div class="panel p-6"><h2 class="text-lg font-semibold">Как хранятся ваши данные</h2><div class="mt-6 space-y-5">${[['Файл делится на части','По 8 МБ, с ограниченным потреблением памяти.'],['Каждая часть шифруется','AES-256-GCM со случайным nonce и проверкой целостности.'],['Провайдер получает зашифрованные данные','Названия файлов и каталог хранятся на вашем сервере.'],['При скачивании данные проверяются','Каждая часть проверяется до передачи клиенту. Скачивание идёт потоком.']].map(([h,d])=>`<div class="flex gap-3"><span class="mt-0.5 text-brand">${icon('shield')}</span><div><h3 class="text-sm font-medium">${h}</h3><p class="mt-1 text-sm text-slate-400">${d}</p></div></div>`).join('')}</div></div></div>`;
}
function renderSettings(){
 const cmd=`export AWS_ACCESS_KEY_ID='<s3_access_key>'\nexport AWS_SECRET_ACCESS_KEY='<s3_secret_key>'\nexport AWS_DEFAULT_REGION='us-east-1'\nexport AWS_REQUEST_CHECKSUM_CALCULATION='when_required'\naws --endpoint-url ${status.s3_endpoint} s3api list-buckets\naws --endpoint-url ${status.s3_endpoint} s3api put-object \\\n  --bucket my-backups --key example.txt --body ./example.txt`;
 $('#view-settings').innerHTML=`<div class="panel overflow-hidden"><div class="border-b border-slate-100 p-6"><h2 class="text-lg font-semibold">Подключите привычные инструменты</h2><p class="mt-2 text-sm text-slate-500">S3 API с AWS Signature V4. Используйте адресацию path-style.</p></div><div class="grid gap-6 p-6 lg:grid-cols-2"><div><label class="text-sm font-medium" for="endpoint">Endpoint</label><div class="mt-2 flex gap-2"><input id="endpoint" class="field font-mono" readonly value="${esc(status.s3_endpoint)}"><button id="copy-endpoint" class="secondary">Копировать</button></div><p class="mt-4 text-sm leading-6 text-slate-500">Ключи S3 находятся в <code>data/credentials.json</code> или переменных S3_ACCESS_KEY и S3_SECRET_KEY. Регион: <code>us-east-1</code>.</p><p class="mt-4 text-sm leading-6 text-slate-500">Поддерживаются бакеты, ListObjects V1/V2, Put/Get/Head/DeleteObject, Range, Multipart Upload и подписанные ссылки. Лимит объекта: ${status.max_size ? bytes(status.max_size) : 'не задан'}.</p><p class="mt-4 text-sm leading-6 text-slate-500">CopyObject, ACL, версии, массовое удаление и потоковая подпись AWS пока не реализованы.</p></div><pre class="overflow-x-auto rounded-xl bg-[#101828] p-5 text-xs leading-7 text-slate-300"><code>${esc(cmd)}</code></pre></div></div>`;
}
function navigate(v){if(!['objects','overview','providers','settings'].includes(v))v='objects';view=v;location.hash=v;document.querySelectorAll('[id^="view-"]').forEach(el=>el.classList.toggle('hidden',el.id!==`view-${v}`));document.querySelectorAll('[data-view]').forEach(el=>el.classList.toggle('active',el.dataset.view===v));const names={objects:'Объекты',overview:'Обзор',providers:'Провайдеры',settings:'Подключение S3'};$('#page-title').textContent=names[v];$('#breadcrumb').textContent=names[v];}
document.querySelectorAll('[data-view]').forEach(el=>el.addEventListener('click',()=>navigate(el.dataset.view)));
window.addEventListener('hashchange',()=>navigate(location.hash.slice(1)));
$('#login-form').addEventListener('submit',async ev=>{ev.preventDefault();const btn=ev.target.querySelector('button');btn.disabled=true;try{await api('auth/login',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({username:$('#username').value.trim(),password:$('#password').value})});$('#password').value='';await load();$('#login-error').textContent='';$('#login').classList.add('hidden');$('#app').classList.remove('hidden');navigate(location.hash.slice(1)||'objects');}catch(e){$('#login-error').textContent=e.message;}finally{btn.disabled=false;}});
$('#logout').onclick=logout;$('#logout-mobile').onclick=logout;
$('#refresh').onclick=()=>load().catch(e=>toast(e.message));
$('#bucket-select').onchange=async ev=>{bucket=ev.target.value;$('#search').value='';try{await loadObjects();}catch(e){toast(e.message)}};
$('#search').oninput=renderFiles;$('#sort').onchange=renderFiles;
$('#new-bucket').onclick=()=>{$('#bucket-error').textContent='';$('#bucket-name').value='';$('#bucket-dialog').showModal();};$('#close-dialog').onclick=()=>$('#bucket-dialog').close();
$('#bucket-form').onsubmit=async ev=>{ev.preventDefault();const btn=ev.target.querySelector('[type="submit"]');btn.disabled=true;try{const name=$('#bucket-name').value.trim();await api('buckets',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({name,provider:$('#provider-select').value})});bucket=name;$('#bucket-dialog').close();navigate('objects');await load();toast('Бакет создан');}catch(e){$('#bucket-error').textContent=e.message;}finally{btn.disabled=false;}};
$('#delete-bucket').onclick=async()=>{if(!bucket)return;if(!confirm(`Удалить пустой бакет «${bucket}»?`))return;try{await api(`buckets/${encodeURIComponent(bucket)}`,{method:'DELETE'});await load();toast('Бакет удалён');}catch(e){toast(e.message)}};
$('#upload').onclick=()=>$('#file-input').click();$('#file-input').onchange=ev=>{uploadFiles([...ev.target.files]);ev.target.value='';};
async function digestBlob(blob){const buffer=await blob.arrayBuffer();const hash=await crypto.subtle.digest('SHA-256',buffer);return Array.from(new Uint8Array(hash),b=>b.toString(16).padStart(2,'0')).join('');}
async function uploadFingerprint(file,target){const sample=new Blob([file.slice(0,65536),file.slice(Math.max(0,file.size-65536))]);return 'maxstore-upload:'+JSON.stringify([target,file.name,file.size,file.lastModified,await digestBlob(sample)]);}
async function withRetry(fn,signal){for(let attempt=0;;attempt++){try{return await fn();}catch(e){if(signal.aborted||e.name==='AbortError'||attempt>=4||e.status&&e.status<500&&e.status!==429)throw e;await new Promise((resolve,reject)=>{const timer=setTimeout(()=>{signal.removeEventListener('abort',abort);resolve();},1000*2**attempt);function abort(){clearTimeout(timer);reject(new DOMException('Paused','AbortError'));}signal.addEventListener('abort',abort,{once:true});});}}}
const activeStates=['queued','preparing','uploading','completing'];
const finishedJobs=new Set();
function hasPendingBucket(name){return transferQueue.jobs.some(j=>j.item.bucket===name&&!['done','skipped'].includes(j.state));}
const transferQueue=new MaxStoreUploadQueue({prepare:prepareUpload,onChange:renderTransfers});
function renderTransfers(queue){
 $('#transfer-panel').classList.toggle('hidden',!queue.jobs.length);const running=queue.jobs.filter(j=>activeStates.includes(j.state)).length;const done=queue.jobs.filter(j=>j.state==='done').length;
 $('#upload-message').textContent=`${running} в работе · ${done} завершено · ${queue.activeParts}/${queue.concurrency} потоков занято. Можно добавлять новые файлы.`;
 $('#upload-pause').disabled=!running;$('#uploads-resume').disabled=!queue.jobs.some(j=>['paused','error'].includes(j.state));$('#uploads-clear').disabled=!queue.jobs.some(j=>['done','skipped'].includes(j.state));
 const labels={queued:'В очереди',preparing:'Подготовка',uploading:'Загрузка',completing:'Завершение',paused:'Приостановлено',error:'Ошибка',done:'Готово',skipped:'Пропущено'};
 $('#transfer-jobs').innerHTML=queue.jobs.map(j=>{const percent=j.item.file.size?Math.min(100,j.uploaded/j.item.file.size*100):(j.state==='done'?100:0);return `<article class="px-5 py-4" data-transfer-id="${j.id}"><div class="flex flex-wrap items-center justify-between gap-3"><div class="min-w-0 flex-1"><div class="flex items-center gap-2"><span class="text-brand">${icon(j.state==='done'?'shield':'upload')}</span><h3 class="truncate text-sm font-semibold" title="${esc(j.item.file.name)}">${esc(j.item.file.name)}</h3></div><p class="mt-1 text-xs text-slate-500">${esc(j.item.bucket)} · ${bytes(j.uploaded)} / ${bytes(j.item.file.size)} · ${labels[j.state]}</p>${j.error?`<p class="mt-1 text-xs text-red-600">${esc(j.error)}</p>`:''}</div><div class="flex items-center gap-2"><span class="badge">${Math.floor(percent)}%</span>${activeStates.includes(j.state)?`<button class="secondary" data-pause-job="${j.id}" aria-label="Приостановить ${esc(j.item.file.name)}">Пауза</button>`:['paused','error'].includes(j.state)?`<button class="secondary" data-resume-job="${j.id}" aria-label="Продолжить ${esc(j.item.file.name)}">Продолжить</button>`:''}</div></div><progress max="100" value="${percent}" class="mt-3 h-1.5 w-full accent-violet-600" aria-label="Прогресс ${esc(j.item.file.name)}"></progress></article>`;}).join('');
 $('#upload').disabled=!bucket;$('#delete-bucket').disabled=!bucket||hasPendingBucket(bucket);
 for(const job of queue.jobs)if(job.state==='done'&&!finishedJobs.has(job.id)){finishedJobs.add(job.id);clearTimeout(refreshTimer);refreshTimer=setTimeout(()=>load().catch(e=>toast(e.message)),200);}
}
$('#upload-pause').onclick=()=>transferQueue.pauseAll();$('#uploads-resume').onclick=()=>{for(const j of transferQueue.jobs)transferQueue.resume(j.id)};$('#uploads-clear').onclick=()=>transferQueue.clearFinished();
$('#transfer-concurrency').onchange=ev=>{localStorage.setItem('maxstore-concurrency',ev.target.value);transferQueue.configure({concurrency:Number(ev.target.value)})};
$('#files-concurrency').onchange=ev=>{localStorage.setItem('maxstore-files-concurrency',ev.target.value);transferQueue.configure({maxFiles:Number(ev.target.value)})};
async function prepareUpload(item,signal){
 const {file,bucket:target}=item;if(status.max_size&&file.size>status.max_size)throw new Error(`Размер больше ${bytes(status.max_size)}`);
 const savedKey=await uploadFingerprint(file,target);let upload;const saved=localStorage.getItem(savedKey);
 if(saved){try{upload=await api(`uploads/${encodeURIComponent(saved)}`,{signal});if(upload.bucket!==target||upload.key!==file.name||upload.size!==file.size)throw new Error('Параметры загрузки изменились');}catch(e){if(e.status===404)localStorage.removeItem(savedKey);else throw e;}}
 if(!upload){const existing=await api(`buckets/${encodeURIComponent(target)}?prefix=${encodeURIComponent(file.name)}`,{signal});if(existing.some(o=>o.key===file.name)&&!confirm(`Объект «${file.name}» в бакете «${target}» существует. Заменить?`))return null;upload=await api('uploads',{method:'POST',signal,headers:{'Content-Type':'application/json'},body:JSON.stringify({bucket:target,key:file.name,type:file.type||'application/octet-stream',size:file.size,parallel:true})});localStorage.setItem(savedKey,upload.id);}
 const parts=upload.completed?[]:await api(`uploads/${upload.id}/parts`,{signal});const chunkSize=status.chunk_size;
 return {count:upload.completed?0:Math.ceil(file.size/chunkSize),received:parts.map(p=>p.number-1),uploadedBytes:upload.completed?file.size:parts.reduce((n,p)=>n+p.size,0),parallelism:upload.mode==='resumable'?1:16,
  send:async(index,partSignal)=>{const chunk=file.slice(index*chunkSize,Math.min((index+1)*chunkSize,file.size));const checksum=await digestBlob(chunk);if(partSignal.aborted)throw new DOMException('Paused','AbortError');await withRetry(()=>api(`uploads/${upload.id}/parts/${index+1}`,{method:'PUT',signal:partSignal,headers:{'Content-Type':'application/octet-stream','X-Content-SHA256':checksum},body:chunk}),partSignal);return chunk.size;},
  complete:async completeSignal=>{await withRetry(()=>api(`uploads/${upload.id}/complete`,{method:'POST',signal:completeSignal}),completeSignal);localStorage.removeItem(savedKey);}
 };
}
function uploadFiles(files){if(!bucket||!files.length)return;if(!crypto.subtle){toast('Для загрузки откройте HTTPS или localhost.');return;}const target=bucket;transferQueue.add(files.map(file=>({file,bucket:target,targetKey:JSON.stringify([target,file.name])})));}
const panel=$('#file-panel');panel.addEventListener('dragover',ev=>{ev.preventDefault();if(bucket)panel.classList.add('dragging')});panel.addEventListener('dragleave',ev=>{if(!panel.contains(ev.relatedTarget))panel.classList.remove('dragging')});panel.addEventListener('drop',ev=>{ev.preventDefault();panel.classList.remove('dragging');uploadFiles([...ev.dataTransfer.files]);});
document.addEventListener('click',async ev=>{
 const btn=ev.target.closest('button');if(!btn)return;
 if(btn.dataset.pauseJob){transferQueue.pause(btn.dataset.pauseJob);return;}if(btn.dataset.resumeJob){transferQueue.resume(btn.dataset.resumeJob);return;}
 if(btn.classList.contains('open-bucket')){bucket=btn.dataset.bucket;navigate('objects');await load().catch(e=>toast(e.message));}
 if(btn.id==='copy-endpoint'){try{await navigator.clipboard.writeText(status.s3_endpoint);toast('Endpoint скопирован');}catch{toast('Скопируйте адрес из поля Endpoint');}}
 if(btn.classList.contains('file-detail')){const o=objects.find(o=>o.key===btn.dataset.key);if(o)toast(o.sha256 ? `SHA-256: ${o.sha256}` : o.tree_sha256 ? `SHA-256 tree: ${o.tree_sha256}` : `Целостность: AES-256-GCM каждой части · ETag: ${o.etag}`);}
 if(btn.classList.contains('delete')){if(!confirm(`Удалить «${btn.dataset.key}»?`))return;btn.disabled=true;try{await fetchDelete(btn.dataset.key);await load();toast('Объект удалён. Части поставлены в очередь очистки.');}catch(e){toast(e.message);btn.disabled=false;}}
 if(btn.classList.contains('download')){const a=document.createElement('a');a.href=objectURL(bucket,btn.dataset.key);a.download=btn.dataset.key.split('/').pop();document.body.appendChild(a);a.click();a.remove();toast(`Скачивание: до ${status.download_concurrency||1} параллельных запросов к хранилищу.`);}
});
async function fetchDelete(key){return api(`objects?${new URLSearchParams({bucket,key})}`,{method:'DELETE'});}
fetch('/api/auth/session',{credentials:'same-origin'}).then(async res=>{if(res.ok){await load();$('#login').classList.add('hidden');$('#app').classList.remove('hidden');navigate(location.hash.slice(1)||'objects');}else{showLogin();}}).catch(e=>{$('#login-error').textContent=e.message;});
