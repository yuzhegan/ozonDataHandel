(function(){
const $=(s)=>document.querySelector(s);
const apiBaseEl=$("#apiBase"),healthEl=$("#health"),collEl=$("#collection"),limitEl=$("#limit"),skipEl=$("#skip"),filtersEl=$("#filters");
const btnLoad=$("#btnLoad"),btnPeek=$("#btnPeek"),btnExport=$("#btnExportCsv"),rowsBadge=$("#rowsBadge"),logEl=$("#log");
const availableFieldsEl=$("#availableFields"),fieldSearchEl=$("#fieldSearch");
const rowsZone=$("#rowsZone"),colsZone=$("#colsZone"),valsZone=$("#valsZone");
const calcNameEl=$("#calcName"),calcExprEl=$("#calcExpr"),btnAddCalc=$("#btnAddCalc"),computedListEl=$("#computedList");
const pivotContainer=$("#pivotContainer"),formatPanelEl=$("#formatPanel");
const viewModeEl=$("#viewMode"),subtotalToggleEl=$("#subtotalToggle");
const profileNameEl=$("#profileName"),btnSaveCloud=$("#btnSaveCloud"),btnLoadCloud=$("#btnLoadCloud"),btnListCloud=$("#btnListCloud");
const btnUseConfig=$("#btnUseConfig");
const iferrAllToggle=$("#iferrAllToggle"), iferrFallbackEl=$("#iferrFallback");
const fileInput=$("#fileInput"),sheetInput=$("#sheetInput"),startRowInput=$("#startRow"),btnUpload=$("#btnUpload");
const rowFieldFiltersEl=$("#rowFieldFilters"), metricFieldFiltersEl=$("#metricFieldFilters");
const btnClearFilters=$("#btnClearFilters"), btnApplyFilters=$("#btnApplyFilters");
const toggleFilterEl=$("#toggleFilter"), toggleConfigEl=$("#toggleConfig");
const filterPanelEl=$("#filterPanel"), configPanelEl=$("#configPanel");
const filterResizeEl=$("#filterResize"), configResizeEl=$("#configResize");

collEl.addEventListener('change', async ()=>{
  if(collEl.value && !isUploadedMode){
    currentCollection = collEl.value;
    loadState();
    // Update UI components to reflect the loaded state
    renderComputedList();
    rebuildAvailableFields();
    renderZones();
    buildFilterUI();
    renderFormatPanel();
    if(viewModeEl) viewModeEl.value = viewMode;
    if(subtotalToggleEl) subtotalToggleEl.checked = subtotalEnabled;
  }
});

let currentCollection="", rawBaseRows=[], currentRows=[], rawUploadRows=[];
let hiddenFields=new Set(), computedDefs=[], layout={rows:[],cols:[],vals:[]}, formats={};
let subtotalEnabled=false, viewMode='pivot';
let iferrDefaultEnabled=false, iferrDefaultFallback=0;
let isUploadedMode=false;
let rowFieldFilters={}, metricFieldFilters={}, filteredRows=[];

function log(s){const t=new Date().toISOString().substr(11,8); logEl.textContent+=`[${t}] ${s}
`; logEl.scrollTop=logEl.scrollHeight;}

function setApiBaseFromConfig(){ if(window.__APP_CONFIG__?.API_BASE) apiBaseEl.value=window.__APP_CONFIG__.API_BASE; }
function guessApiBases(){ const list=[]; if(window.__APP_CONFIG__?.API_BASE) list.push(window.__APP_CONFIG__.API_BASE); if(apiBaseEl?.value) list.push(apiBaseEl.value); const host=location.hostname||'localhost'; list.push(`http://${host}:7002`,`http://${host}:8000`); return [...new Set(list)]; }
async function smartPingAndInit(){ for(const b of guessApiBases()){ try{ const r=await fetch(`${b}/api/health`); const j=await r.json(); if(j?.status==='ok'){ apiBaseEl.value=b; healthEl.textContent='OK'; healthEl.style.color='#4ade80'; await loadCollections(); if(collEl.options.length){ currentCollection=collEl.value; await doPeek(); } return; } }catch(e){ log('健康检查失败：'+b); } } healthEl.textContent='连接失败'; healthEl.style.color='#f87171';}

async function loadCollections(){ try{ const r=await fetch(`${apiBaseEl.value}/api/collections`); const j=await r.json(); collEl.innerHTML=''; const collections = j && j.collections && Array.isArray(j.collections) ? j.collections : []; collections.forEach(c=>{ const o=document.createElement('option'); o.value=c; o.textContent=c; collEl.appendChild(o); }); }catch(e){ log('加载集合列表失败: '+e.message); collEl.innerHTML=''; } }

btnUseConfig && (btnUseConfig.onclick=()=>{ if(window.__APP_CONFIG__?.API_BASE){ apiBaseEl.value=window.__APP_CONFIG__.API_BASE; smartPingAndInit(); }});

function num(x){ if(x===null||x===undefined||x==='') return 0; if(typeof x==='number'&&isFinite(x)) return x;
  if(typeof x==='string'){ let s=x.replace(/\u00A0/g,' ').trim().replace(/\s+/g,''); if(s.includes(',')&&s.includes('.')){ if(s.lastIndexOf(',')>s.lastIndexOf('.')){ s=s.replace(/\./g,'').replace(',', '.'); } else { s=s.replace(/,/g,''); } } else if(s.includes(',')){ s=s.replace(/\./g,''); s=s.replace(/,/g,'.'); } const v=parseFloat(s); return isNaN(v)?0:v; }
  const v=Number(x); return isNaN(v)?0:v; }

function compileExpr(expr){
  return function(r){
    try{
      const keys=Object.keys(r).sort((a,b)=>b.length-a.length);
      let code=expr;
      
      // Replace field names with num(r["fieldname"]) calls
      // Use direct string replacement for better reliability with special characters
      keys.forEach(k=>{ 
        // Create a safe replacement pattern - escape the field name for regex but use simple replacement
        const escapedKey = k.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
        // Create regex that matches the field name as a whole token, not part of another word
        // Use word boundaries but handle special characters properly
        const regex = new RegExp(`\\b${escapedKey}\\b`, 'g');
        
        // Try regex first, but if it fails (due to special characters), use manual approach
        try {
          const newCode = code.replace(regex, `num(r["${k}"])`);
          if (newCode !== code) {
            code = newCode;
            return;
          }
        } catch(e) {
          // Regex failed, use manual token-by-token approach
        }
        
        // Manual approach: split by common delimiters and replace exact matches
        // Don't include | in the split pattern since it might be part of field names
        let parts = code.split(/([\s+\-*/()=<>!&,;{}[\]\.]+)/);
        parts = parts.map(part => {
          return (part === k) ? `num(r["${k}"])` : part;
        });
        code = parts.join('');
      });
      
      const FN=new Function('r','num','__enabled','__fb',`
        const IFERROR=(v,fb=0)=>{ const n=Number(v); return (Number.isFinite(n)&&!Number.isNaN(n))?v:fb; };
        const DIV=(a,b,fb=0)=>{ const q=num(a)/num(b); return (Number.isFinite(q)&&!Number.isNaN(q))?q:fb; };
        const _res = (${code});
        return __enabled ? IFERROR(_res, __fb) : _res;
      `);
      const fb = Number(iferrFallbackEl?.value ?? iferrDefaultFallback) || 0;
      return FN(r,num, !!iferrDefaultEnabled, fb);
    }catch(e){ 
      console.warn('Custom field compilation error:', e, 'Expression:', expr);
      return 0; 
    }
  };
}
function extractVars(expr){ 
  const set=new Set(); 
  
  // For aggregation context, we need a more sophisticated approach
  // Try to extract potential field names by splitting on operators but keeping complex names
  
  // Split by operators while preserving the parts
  const parts = expr.split(/([\s+\-*/()=<>!&,;{}[\]\.]+)/);
  
  parts.forEach(part => {
    const trimmed = part.trim();
    if (trimmed && 
        !/^[\s+\-*/()=<>!&,;{}[\]\.]+$/.test(trimmed) && // Not just operators
        !/^\d+(\.\d+)?$/.test(trimmed) && // Not just numbers
        !['num','Math','Infinity','NaN','undefined','true','false','IFERROR','DIV'].includes(trimmed)) {
      set.add(trimmed);
    }
  });
  
  return [...set]; 
}
function compileAggExpr(expr){
  // Extract variables from expression and use them as potential field names
  const vars=extractVars(expr); 
  let code=expr;
  vars.sort((a,b)=>b.length-a.length);
  
  vars.forEach(v=>{ 
    // Create a safe replacement pattern
    const escapedKey = v.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    
    try {
      const regex = new RegExp(`\\b${escapedKey}\\b`, 'g');
      const newCode = code.replace(regex, `get("${v}")`);
      if (newCode !== code) {
        code = newCode;
        return;
      }
    } catch(e) {
      // Regex failed, use manual approach
    }
    
    // Manual approach for special characters
    let parts = code.split(/([\s+\-*/()=<>!&,;{}[\]\.]+)/);
    parts = parts.map(part => {
      return (part === v) ? `get("${v}")` : part;
    });
    code = parts.join('');
  });
  
  return function(get){
    try{
      const FN=new Function('get','num','__enabled','__fb',`
        const IFERROR=(v,fb=0)=>{ const n=Number(v); return (Number.isFinite(n)&&!Number.isNaN(n))?v:fb; };
        const DIV=(a,b,fb=0)=>{ const q=num(a)/num(b); return (Number.isFinite(q)&&!Number.isNaN(q))?q:fb; };
        const _res = (${code});
        return __enabled ? IFERROR(_res, __fb) : _res;
      `);
      const fb = Number(iferrFallbackEl?.value ?? iferrDefaultFallback) || 0;
      return FN(get,num, !!iferrDefaultEnabled, fb);
    }catch(e){ 
      console.warn('Custom field aggregation compilation error:', e, 'Expression:', expr);
      return 0; 
    }
  };
}

function applyComputed(rows){ if(!computedDefs.length || !Array.isArray(rows)) return rows || []; return rows.map(r=>{ const out={...r}; computedDefs.forEach(def=>{ out[def.name]=def.fn(out); }); return out; }); }

function buildFilterUI(){
  rowFieldFiltersEl.innerHTML=''; metricFieldFiltersEl.innerHTML='';
  if(!currentRows || !currentRows.length) return;
  
  const fields = Object.keys(currentRows[0] || {});
  const rowFields = layout.rows || [];
  const valFields = (layout.vals || []).map(v => v.field);
  
  // 为行字段生成字符串筛选器
  rowFields.forEach(field => {
    if(!fields.includes(field)) return;
    const filterDiv = document.createElement('div');
    filterDiv.className = 'filter-item';
    filterDiv.innerHTML = `
      <label>${field}</label>
      <input type="text" data-field="${field}" data-type="row" 
             placeholder="输入包含的字符串..." style="width:100%;" />
    `;
    rowFieldFiltersEl.appendChild(filterDiv);
  });
  
  // 为指标字段生成数字筛选器
  valFields.forEach(field => {
    if(!fields.includes(field)) return;
    const nums = currentRows.map(r => num(r[field])).filter(n => Number.isFinite(n));
    if(!nums.length) return;
    const min = Math.min(...nums);
    const max = Math.max(...nums);
    const filterDiv = document.createElement('div');
    filterDiv.className = 'filter-item';
    filterDiv.innerHTML = `
      <label>${field}</label>
      <div style="display:flex;gap:4px;align-items:center;">
        <input type="number" data-field="${field}" data-type="metric" data-operator="min" 
               placeholder="最小值" step="0.01" style="width:50%;" />
        <span style="color:#7f8ca3;">-</span>
        <input type="number" data-field="${field}" data-type="metric" data-operator="max" 
               placeholder="最大值" step="0.01" style="width:50%;" />
      </div>
    `;
    metricFieldFiltersEl.appendChild(filterDiv);
  });
}

function applyFilters(){
  if(!rawBaseRows || !rawBaseRows.length) return;
  let filtered = [...rawBaseRows];
  
  // 应用计算字段
  filtered = applyComputed(filtered);
  
  // 应用行字段筛选 (字符串包含筛选)
  Object.keys(rowFieldFilters).forEach(field => {
    const value = rowFieldFilters[field];
    if(value && value !== '') {
      filtered = filtered.filter(r => String(r[field] || '').toLowerCase().includes(value.toLowerCase()));
    }
  });
  
  // 应用指标字段筛选 (数字筛选)
  Object.keys(metricFieldFilters).forEach(field => {
    const filter = metricFieldFilters[field];
    if(filter) {
      filtered = filtered.filter(r => {
        const val = num(r[field]);
        if(filter.min !== undefined && val < filter.min) return false;
        if(filter.max !== undefined && val > filter.max) return false;
        return true;
      });
    }
  });
  
  filteredRows = filtered;
  return filtered;
}

function updateFilters(){
  rowFieldFilters = {};
  metricFieldFilters = {};
  
  // 收集行字段筛选
  rowFieldFiltersEl.querySelectorAll('input[data-type="row"]').forEach(input => {
    const field = input.dataset.field;
    if(input.value && input.value.trim() !== '') {
      rowFieldFilters[field] = input.value.trim();
    }
  });
  
  // 收集指标字段筛选
  const metricInputs = metricFieldFiltersEl.querySelectorAll('input[data-type="metric"]');
  metricInputs.forEach(input => {
    const field = input.dataset.field;
    const operator = input.dataset.operator;
    const value = input.value;
    
    if(value && value !== '') {
      if(!metricFieldFilters[field]) metricFieldFilters[field] = {};
      metricFieldFilters[field][operator] = parseFloat(value);
    }
  });
  
  log('筛选条件已更新');
}

function matchesFilter(doc, filt){
  if(!filt || !Object.keys(filt).length) return true;
  const ops=Object.keys(filt);
  if("$and" in filt){ return (filt.$and||[]).every(f=>matchesFilter(doc,f)); }
  if("$or" in filt){ return (filt.$or||[]).some(f=>matchesFilter(doc,f)); }
  for(const k of ops){
    if(k==="$and"||k==="$or") continue;
    const cond=filt[k];
    const val=doc[k];
    if(cond && typeof cond==="object" && !Array.isArray(cond)){
      for(const op in cond){
        const target=cond[op];
        if(op==="$eq"){ if(!(val===target)) return false; }
        else if(op==="$gt"){ if(!(num(val)>num(target))) return false; }
        else if(op==="$gte"){ if(!(num(val)>=num(target))) return false; }
        else if(op==="$lt"){ if(!(num(val)<num(target))) return false; }
        else if(op==="$lte"){ if(!(num(val)<=num(target))) return false; }
        else if(op==="$in"){ if(!Array.isArray(target) || !target.includes(val)) return false; }
        else if(op==="$regex"){
          const pattern = String(target);
          const flags = (cond.$options||"");
          try{
            const re=new RegExp(pattern, flags);
            if(!re.test(String(val??""))) return false;
          }catch{ return false; }
        } else {
          return false;
        }
      }
    } else {
      if(!(val===cond)) return false;
    }
  }
  return true;
}

function rebuildAvailableFields(){ availableFieldsEl.innerHTML=''; const sample=Array.isArray(currentRows) && currentRows.length > 0 ? currentRows[0] : {}; let fields=Object.keys(sample).filter(k=>!hiddenFields.has(k)); const kw=(fieldSearchEl?.value||'').trim().toLowerCase(); if(kw) fields=fields.filter(x=>x.toLowerCase().includes(kw)); fields.sort(); fields.forEach(f=>{ const chip=document.createElement('div'); chip.className='field'; chip.draggable=true; chip.dataset.field=f; chip.innerHTML=`<span>${f}</span>`; chip.addEventListener('dragstart',onDragStart); availableFieldsEl.appendChild(chip); }); }

function renderZone(zoneEl, arr, showAgg){ zoneEl.innerHTML=''; arr.forEach((item,idx)=>{ const f=typeof item==='string'?item:item.field; const chip=document.createElement('div'); chip.className='field'; chip.draggable=true; chip.dataset.field=f; chip.dataset.zone=zoneEl.id; chip.dataset.index=idx; let inner=`<span>${f}</span>`; if(showAgg){ const agg=item.agg||'sum'; inner+=` <select class='agg'><option value='sum'${agg==='sum'?' selected':''}>sum</option><option value='avg'${agg==='avg'?' selected':''}>avg</option><option value='count'${agg==='count'?' selected':''}>count</option><option value='min'${agg==='min'?' selected':''}>min</option><option value='max'${agg==='max'?' selected':''}>max</option></select>`;} inner+=` <span class='remove'>✕</span>`; chip.innerHTML=inner; chip.addEventListener('dragstart',onDragStart); chip.querySelector('.remove').onclick=()=>{ arr.splice(idx,1); saveState(); renderZones(); renderPivot(); }; if(showAgg){ chip.querySelector('.agg').onchange=e=>{ arr[idx].agg=e.target.value; saveState(); renderPivot(); renderFormatPanel(); }; } zoneEl.appendChild(chip); }); }
function renderZones(){ renderZone(rowsZone,layout.rows,false); renderZone(colsZone,layout.cols,false); renderZone(valsZone,layout.vals,true); renderFormatPanel(); }
function onDragStart(e){ const field=e.currentTarget.dataset.field; const zone=e.currentTarget.dataset.zone||'available'; const index=e.currentTarget.dataset.index; e.dataTransfer.setData('text/plain', JSON.stringify({field,from:zone,index:index?Number(index):null})); }
[rowsZone,colsZone,valsZone,availableFieldsEl,computedListEl].forEach(el=>{ el.addEventListener('dragover',e=>e.preventDefault()); el.addEventListener('drop', e=>{ e.preventDefault(); const data=JSON.parse(e.dataTransfer.getData('text/plain')); const to=el.id; if(data.from==='rowsZone') layout.rows.splice(data.index,1); if(data.from==='colsZone') layout.cols.splice(data.index,1); if(data.from==='valsZone') layout.vals.splice(data.index,1); if(to==='rowsZone') layout.rows.push(data.field); else if(to==='colsZone') layout.cols.push(data.field); else if(to==='valsZone') layout.vals.push({field:data.field,agg:'sum'}); saveState(); renderZones(); renderPivot(); }); });

function metricKey(vd){ return `${vd.field}|${vd.agg||'sum'}`; }
function renderFormatPanel(){ formatPanelEl.innerHTML=''; const list=layout.vals.length?layout.vals:[]; list.forEach(vd=>{ const key=metricKey(vd); const cfgRaw=formats[key]||{}; const cfg={decimals:Number.isFinite(cfgRaw.decimals)?cfgRaw.decimals:2, thousand:cfgRaw.thousand!==undefined?!!cfgRaw.thousand:true, currency:cfgRaw.currency||'', currencyPos:cfgRaw.currencyPos||'prefix'}; const card=document.createElement('div'); card.className='metric-card'; card.innerHTML=`<div class='metric-title'>${vd.field} ${vd.agg||'sum'}</div><div class='metric-grid'><label>小数位<input type='number' class='fmt-dec' min='0' max='8' step='1' value='${cfg.decimals}'/></label><label>千分位<input type='checkbox' class='fmt-th' ${cfg.thousand?'checked':''}/></label><label>货币符号<input type='text' class='fmt-cur' value='${cfg.currency.replace(/"/g,'&quot;')}'/></label><label>位置<select class='fmt-pos'><option value='prefix'${cfg.currencyPos==='prefix'?' selected':''}>前</option><option value='suffix'${cfg.currencyPos==='suffix'?' selected':''}>后</option></select></label></div>`; const dec=card.querySelector('.fmt-dec'),th=card.querySelector('.fmt-th'),cur=card.querySelector('.fmt-cur'),pos=card.querySelector('.fmt-pos'); dec.oninput=e=>{ const v=Number(e.target.value); cfg.decimals=Number.isFinite(v)?Math.max(0,Math.min(8,v)):0; formats[key]=cfg; saveState(); renderPivot(); }; th.onchange=e=>{ cfg.thousand=!!e.target.checked; formats[key]=cfg; saveState(); renderPivot(); }; cur.oninput=e=>{ cfg.currency=e.target.value; formats[key]=cfg; saveState(); renderPivot(); }; pos.onchange=e=>{ cfg.currencyPos=e.target.value; formats[key]=cfg; saveState(); renderPivot(); }; formatPanelEl.appendChild(card); }); if(!list.length){ const p=document.createElement('div'); p.className='small'; p.textContent='将指标拖入“指标（聚合）”后，在此配置格式。'; formatPanelEl.appendChild(p); } }
function formatNumber(n,opt){ const decimals=Number(opt?.decimals??2); const thousand=!!opt?.thousand; const currency=opt?.currency||''; const pos=opt?.currencyPos||'prefix'; const fixed=(Math.round(n*(10**decimals))/(10**decimals)).toFixed(decimals); let x=fixed; if(thousand){ const [i,d]=fixed.split('.'); const sep=i.replace(/\B(?=(\d{3})+(?!\d))/g,','); x=d!==undefined?`${sep}.${d}`:sep; } return currency? (pos==='suffix'?`${x}${currency}`:`${currency}${x}`):x; }
function formatAgg(value,agg,fmt){ const n=(typeof value==='number')?value:num(value); if(!isFinite(n)) return ''; if(agg==='count') return String(Math.round(n)); return formatNumber(n,fmt); }

function saveState(){ localStorage.setItem(`pivot_hidden_${currentCollection}`, JSON.stringify([...hiddenFields])); localStorage.setItem(`pivot_layout_${currentCollection}`, JSON.stringify(layout)); localStorage.setItem(`pivot_formulas_${currentCollection}`, JSON.stringify(computedDefs.map(({name,expr})=>({name,expr})))); localStorage.setItem(`pivot_formats_${currentCollection}`, JSON.stringify(formats)); localStorage.setItem(`pivot_subtotal_${currentCollection}`, JSON.stringify(subtotalEnabled)); localStorage.setItem(`pivot_view_${currentCollection}`, viewMode); localStorage.setItem(`pivot_iferr_enabled_${currentCollection}`, JSON.stringify(iferrDefaultEnabled)); localStorage.setItem(`pivot_iferr_fb_${currentCollection}`, JSON.stringify(Number(iferrFallbackEl?.value ?? iferrDefaultFallback)||0)); localStorage.setItem(`pivot_freeze_${currentCollection}`, JSON.stringify(freezeSettings)); }
function loadState(){ hiddenFields=new Set(JSON.parse(localStorage.getItem(`pivot_hidden_${currentCollection}`)||'[]')); layout=JSON.parse(localStorage.getItem(`pivot_layout_${currentCollection}`)||'{"rows":[],"cols":[],"vals":[]}')||{rows:[],cols:[],vals:[]}; const f=JSON.parse(localStorage.getItem(`pivot_formulas_${currentCollection}`)||'[]'); computedDefs=f.map(x=>({...x, fn:compileExpr(x.expr), fnAgg:compileAggExpr(x.expr)})); formats=JSON.parse(localStorage.getItem(`pivot_formats_${currentCollection}`)||'{}'); subtotalEnabled=JSON.parse(localStorage.getItem(`pivot_subtotal_${currentCollection}`)||'false'); viewMode=localStorage.getItem(`pivot_view_${currentCollection}`)||'pivot'; iferrDefaultEnabled=JSON.parse(localStorage.getItem(`pivot_iferr_enabled_${currentCollection}`)||'false'); iferrDefaultFallback=JSON.parse(localStorage.getItem(`pivot_iferr_fb_${currentCollection}`)||'0'); freezeSettings=JSON.parse(localStorage.getItem(`pivot_freeze_${currentCollection}`)||'{"rows":1,"cols":0}'); if(iferrAllToggle){ iferrAllToggle.checked=!!iferrDefaultEnabled; } if(iferrFallbackEl){ iferrFallbackEl.value=String(iferrDefaultFallback); } $('#freezeRows').value=String(freezeSettings.rows||1); $('#freezeCols').value=String(freezeSettings.cols||0); }

function getColumns(){ return Array.isArray(rawBaseRows) && rawBaseRows.length > 0 ? Object.keys(rawBaseRows[0]) : []; }

function pivotData(baseRows){
  if(!Array.isArray(baseRows) || baseRows.length === 0) {
    return {headerRows: [], dataRows: [], summaryRow: [], allRowData: [], rowFields: [], colFields: [], valDefs: [], colAggTypes: [], colKeys: [], rows: []};
  }
  const rowFields=layout.rows, colFields=layout.cols, valDefs=layout.vals.length?layout.vals:[{field:getColumns()[0]||'',agg:'count'}];
  const sep='\u0001';
  const computedNames=new Set(computedDefs.map(d=>d.name));
  const computedMap=Object.fromEntries(computedDefs.map(d=>[d.name,d]));

  const colKey=r=>colFields.map(f=>String(r[f]??'')).join(sep);
  const colKeys=[], colSet=new Set();
  baseRows.forEach(r=>{ const ck=colKey(r); if(!colSet.has(ck)){colSet.add(ck);colKeys.push(ck);} });

  function initAgg(){return {sum:0,count:0,min:Infinity,max:-Infinity}}
  function updateAgg(a,v){const n=num(v); a.sum+=n; a.count++; if(n<a.min)a.min=n; if(n>a.max)a.max=n;}
  function valueAgg(a,t){ if(t==='sum')return a.sum; if(t==='count')return a.count; if(t==='min')return a.min===Infinity?0:a.min; if(t==='max')return a.max===-Infinity?0:a.max; if(t==='avg')return a.count?a.sum/a.count:0; return 0; }

  // 构建层级树结构
  function buildHierarchy() {
    const root = { children: new Map(), data: new Map(), isSubtotal: false };
    
    baseRows.forEach(r0 => {
      const r = {...r0};
      computedDefs.forEach(def=>{ r[def.name] = def.fn(r); });
      
      let currentNode = root;
      const ck = colKey(r);
      
      // 构建行层级路径
      for (let level = 0; level < rowFields.length; level++) {
        const fieldValue = String(r[rowFields[level]] ?? '');
        if (!currentNode.children.has(fieldValue)) {
          currentNode.children.set(fieldValue, { 
            children: new Map(), 
            data: new Map(), 
            isSubtotal: false,
            level: level,
            fieldName: rowFields[level],
            value: fieldValue
          });
        }
        currentNode = currentNode.children.get(fieldValue);
      }
      
      // 在叶子节点存储数据
      const dataKey = ck;
      if (!currentNode.data.has(dataKey)) currentNode.data.set(dataKey, {});
      const bucket = currentNode.data.get(dataKey);
      Object.keys(r).forEach(k=>{ if(!bucket[k]) bucket[k]=initAgg(); });
      Object.keys(r).forEach(k=> updateAgg(bucket[k], r[k]));
    });
    
    return root;
  }
  
  // 递归生成所有行（包括小计行）
  function generateRows(node, currentPath = [], allRows = []) {
    if (node.children.size === 0) {
      // 叶子节点：添加数据行
      const row = [...currentPath];
      // 填充剩余列为空
      while (row.length < rowFields.length) row.push('');
      
      colKeys.forEach(ck => {
        const bucket = node.data.get(ck) || {};
        valDefs.forEach(vd => {
          let val = 0;
          if (computedNames.has(vd.field)) {
            const def = computedMap[vd.field];
            const accessor = (name) => { const a = bucket[name] || initAgg(); return valueAgg(a, 'sum'); };
            val = def.fnAgg ? def.fnAgg(accessor) : 0;
          } else {
            const a = bucket[vd.field] || initAgg();
            val = valueAgg(a, vd.agg);
          }
          row.push(val);
        });
      });
      allRows.push({ row, level: currentPath.length, isSubtotal: false, path: [...currentPath] });
      return;
    }
    
    // 中间节点：递归处理子节点
    const children = Array.from(node.children.entries()).sort((a, b) => String(a[0]).localeCompare(String(b[0])));
    
    for (const [value, childNode] of children) {
      const newPath = [...currentPath, value];
      generateRows(childNode, newPath, allRows);
    }
    
    // 在处理完所有子项后，为每个有子项的分组添加小计行
    for (const [value, childNode] of children) {
      if (subtotalEnabled && childNode.children.size > 0 && rowFields.length > 1) {
        const newPath = [...currentPath, value];
        const subtotalRow = [...newPath];
        // 填充剩余列
        while (subtotalRow.length < rowFields.length) subtotalRow.push('');
        
        // 计算这个分组的小计
        const subtotalData = new Map();
        function collectSubtotalData(n) {
          if (n.children.size === 0) {
            // 叶子节点，收集数据
            for (const [ck, bucket] of n.data) {
              if (!subtotalData.has(ck)) subtotalData.set(ck, {});
              const subtotalBucket = subtotalData.get(ck);
              Object.keys(bucket).forEach(k => {
                if (!subtotalBucket[k]) subtotalBucket[k] = initAgg();
                const agg = bucket[k];
                subtotalBucket[k].sum += agg.sum;
                subtotalBucket[k].count += agg.count;
                if (agg.min < subtotalBucket[k].min) subtotalBucket[k].min = agg.min;
                if (agg.max > subtotalBucket[k].max) subtotalBucket[k].max = agg.max;
              });
            }
          } else {
            // 递归收集子节点数据
            for (const child of n.children.values()) {
              collectSubtotalData(child);
            }
          }
        }
        collectSubtotalData(childNode);
        
        colKeys.forEach(ck => {
          const bucket = subtotalData.get(ck) || {};
          valDefs.forEach(vd => {
            let val = 0;
            if (computedNames.has(vd.field)) {
              const def = computedMap[vd.field];
              const accessor = (name) => { const a = bucket[name] || initAgg(); return valueAgg(a, 'sum'); };
              val = def.fnAgg ? def.fnAgg(accessor) : 0;
            } else {
              const a = bucket[vd.field] || initAgg();
              val = valueAgg(a, vd.agg);
            }
            subtotalRow.push(val);
          });
        });
        
        allRows.push({ row: subtotalRow, level: newPath.length, isSubtotal: true, path: [...newPath] });
      }
    }
    
    return allRows;
  }

  const hierarchy = buildHierarchy();
  const allRowData = generateRows(hierarchy);
  
  // 提取数据行
  const dataRows = allRowData.map(item => item.row);

  const colLabels=colKeys.map(ck=> ck?ck.split(sep):Array(colFields.length).fill(''));
  const headerRows=[];
  for(let level=0; level<colFields.length; level++){ const row=Array(rowFields.length).fill(''); colLabels.forEach(labels=> row.push(labels[level]||'')); headerRows.push(row); }
  const valHeader=Array(rowFields.length).fill(''); colKeys.forEach(_=>{ valDefs.forEach(vd=> valHeader.push(`${vd.field} ${vd.agg}`)); }); headerRows.push(valHeader);
  const colAggTypes=[]; colKeys.forEach(_=>{ valDefs.forEach(vd=> colAggTypes.push(vd.agg)); });

  // 计算汇总行
  const summaryRow = [];
  for(let i = 0; i < rowFields.length; i++) {
    summaryRow.push(i === 0 ? '汇总' : '');
  }
  
  // 从层级结构计算汇总值
  const totalData = new Map();
  function collectTotalData(node) {
    if (node.children.size === 0) {
      // 叶子节点，收集数据
      for (const [ck, bucket] of node.data) {
        if (!totalData.has(ck)) totalData.set(ck, {});
        const totalBucket = totalData.get(ck);
        Object.keys(bucket).forEach(k => {
          if (!totalBucket[k]) totalBucket[k] = initAgg();
          const agg = bucket[k];
          totalBucket[k].sum += agg.sum;
          totalBucket[k].count += agg.count;
          if (agg.min < totalBucket[k].min) totalBucket[k].min = agg.min;
          if (agg.max > totalBucket[k].max) totalBucket[k].max = agg.max;
        });
      }
    } else {
      // 递归收集子节点数据
      for (const child of node.children.values()) {
        collectTotalData(child);
      }
    }
  }
  collectTotalData(hierarchy);
  
  colKeys.forEach(ck => {
    const bucket = totalData.get(ck) || {};
    valDefs.forEach(vd => {
      let totalVal = 0;
      if (computedNames.has(vd.field)) {
        const def = computedMap[vd.field];
        const accessor = (name) => { const a = bucket[name] || initAgg(); return valueAgg(a, 'sum'); };
        totalVal = def.fnAgg ? def.fnAgg(accessor) : 0;
      } else {
        const a = bucket[vd.field] || initAgg();
        totalVal = valueAgg(a, vd.agg);
      }
      summaryRow.push(totalVal);
    });
  });

  return {headerRows, dataRows, summaryRow, allRowData, rowFields, colFields, valDefs, colAggTypes, colKeys, rows: baseRows};
}

function renderRawTable(rows){
  if(!Array.isArray(rows) || rows.length === 0) {
    pivotContainer.innerHTML='<div class="small">没有数据</div>';
    btnExport.disabled=true;
    return;
  }
  const headers=Object.keys(rows[0]||{});
  const table=document.createElement('table');
  const thead=document.createElement('thead');
  const trh=document.createElement('tr');
  headers.forEach(h=>{ const th=document.createElement('th'); th.textContent=h; trh.appendChild(th); });
  thead.appendChild(trh); table.appendChild(thead);
  const tbody=document.createElement('tbody');
  rows.forEach(r=>{ const tr=document.createElement('tr'); headers.forEach((h,ci)=>{ const td=document.createElement('td'); td.textContent=String(r[h]??''); td.dataset.col=ci; tr.appendChild(td); }); tbody.appendChild(tr); });
  table.appendChild(tbody);
  
  // 设置表格的最小宽度以确保有横向滚动条
  table.style.minWidth = Math.max(800, headers.length * 120) + 'px';
  
  pivotContainer.innerHTML=''; pivotContainer.appendChild(table); btnExport.disabled=false;
  enableCrosshair(table);
}

function renderPivot(){
  const base=Array.isArray(rawBaseRows) ? rawBaseRows : [];
  if(!base.length){ pivotContainer.innerHTML='<div class="small">没有数据</div>'; rowsBadge.textContent='Rows: 0'; btnExport.disabled=true; return; }
  
  // 使用筛选后的数据或原始数据
  const filteredBase = filteredRows.length > 0 ? filteredRows : applyFilters();
  currentRows = filteredBase.length > 0 ? filteredBase : applyComputed(base);
  
  rebuildAvailableFields(); renderZones(); buildFilterUI(); 
  rowsBadge.textContent=`Rows: ${Array.isArray(currentRows) ? currentRows.length : 0}`;
  if(viewMode==='raw') return renderRawTable(currentRows);

  // 使用筛选后的数据进行透视计算
  const pivotBase = filteredBase.length > 0 ? filteredBase : base;
  const {headerRows, dataRows, summaryRow, allRowData, rowFields, colFields, valDefs, colAggTypes, colKeys, rows} = pivotData(pivotBase);

  const thead=document.createElement('thead');
  headerRows.forEach((hr, ridx)=>{ const tr=document.createElement('tr'); hr.forEach((cell, ci)=>{ const th=document.createElement('th'); th.textContent = ridx<headerRows.length-1 ? (ci<rowFields.length ? (rowFields[ci]||'') : String(cell)) : String(cell); tr.appendChild(th); }); thead.appendChild(tr); });
  const tbody=document.createElement('tbody');

  allRowData.forEach((rowInfo, index) => {
    const r = rowInfo.row;
    const tr = document.createElement('tr');
    
    // 根据是否为小计行设置样式
    if (rowInfo.isSubtotal) {
      tr.className = 'subtotal-row';
      tr.style.cssText = 'background-color: #162032; font-weight: bold; border-top: 1px solid #334155;';
    }
    
    r.forEach((cell, ci) => {
      const td = document.createElement('td');
      
      // 小计行样式
      if (rowInfo.isSubtotal) {
        td.style.cssText = 'background-color: #162032; font-weight: bold;';
      }
      
      if (ci < rowFields.length) {
        // 行标签列 - 添加层级缩进
        const indent = Math.max(0, rowInfo.level - 1) * 20;
        if (indent > 0) {
          td.style.paddingLeft = `${6 + indent}px`;
        }
        
        // 小计行添加前缀
        if (rowInfo.isSubtotal && cell && ci === rowInfo.level - 1) {
          td.textContent = `${cell} 小计`;
        } else {
          td.textContent = String(cell);
        }
      } else {
        // 数值列
        const idx = ci - rowFields.length;
        const aggType = colAggTypes ? colAggTypes[idx % colAggTypes.length] : 'sum';
        const vd = valDefs[idx % valDefs.length];
        const key = `${vd.field}|${vd.agg||'sum'}`;
        const fmt = formats[key] || {decimals:2, thousand:true, currency:'', currencyPos:'prefix'};
        td.textContent = formatAgg(cell, aggType, fmt);
      }
      
      td.dataset.col = ci;
      tr.appendChild(td);
    });
    
    tbody.appendChild(tr);
  });

  // 添加汇总行
  if(summaryRow && summaryRow.length > 0) {
    const summaryTr = document.createElement('tr');
    summaryTr.className = 'summary-row';
    summaryTr.style.cssText = 'background-color: #1a2332; border-top: 2px solid #4a5568; font-weight: bold;';
    
    summaryRow.forEach((cell, ci) => {
      const td = document.createElement('td');
      td.style.cssText = 'background-color: #1a2332; font-weight: bold;';
      
      if(ci < rowFields.length) {
        td.textContent = String(cell);
      } else {
        const idx = ci - rowFields.length;
        const aggType = colAggTypes ? colAggTypes[idx % colAggTypes.length] : 'sum';
        const vd = valDefs[idx % valDefs.length];
        const key = `${vd.field}|${vd.agg||'sum'}`;
        const fmt = formats[key] || {decimals:2, thousand:true, currency:'', currencyPos:'prefix'};
        td.textContent = formatAgg(cell, aggType, fmt);
      }
      
      td.dataset.col = ci;
      summaryTr.appendChild(td);
    });
    
    tbody.appendChild(summaryTr);
  }

  const table=document.createElement('table'); table.appendChild(thead); table.appendChild(tbody);
  
  // 设置表格的最小宽度以确保有横向滚动条
  const colCount = headerRows.length > 0 ? headerRows[headerRows.length-1].length : 0;
  table.style.minWidth = Math.max(800, colCount * 120) + 'px';
  
  pivotContainer.innerHTML=''; pivotContainer.appendChild(table); btnExport.disabled=false;
  enableCrosshair(table);
  enableColumnResize(table);
  enableColumnSort(table, allRowData, rowFields, colFields, valDefs, colAggTypes, formats);
}

function enableCrosshair(table){
  let lastRow=null, lastColIndex=null;
  const clear=()=>{
    if(lastRow){ lastRow.classList.remove('hl-row'); lastRow=null; }
    if(lastColIndex!==null){
      table.querySelectorAll(`td[data-col="${lastColIndex}"]`).forEach(td=>td.classList.remove('hl-col','hl-hit'));
      lastColIndex=null;
    }
  };
  table.addEventListener('mouseleave', clear);
  table.addEventListener('mousemove', (e)=>{
    const td=e.target.closest('td'); if(!td) return;
    const tr=td.parentElement; const col=td.dataset.col;
    if(tr!==lastRow){ if(lastRow) lastRow.classList.remove('hl-row'); tr.classList.add('hl-row'); lastRow=tr; }
    if(col!==String(lastColIndex)){ if(lastColIndex!==null){ table.querySelectorAll(`td[data-col="${lastColIndex}"]`).forEach(n=>n.classList.remove('hl-col','hl-hit')); } table.querySelectorAll(`td[data-col="${col}"]`).forEach(n=>n.classList.add('hl-col')); lastColIndex=Number(col); }
    table.querySelectorAll('td.hl-hit').forEach(n=>n.classList.remove('hl-hit')); td.classList.add('hl-hit');
  });
}

// 列宽调整功能
function enableColumnResize(table) {
  const headers = table.querySelectorAll('thead th');
  headers.forEach((th, index) => {
    // 添加调整手柄
    const resizer = document.createElement('div');
    resizer.className = 'column-resizer';
    resizer.style.cssText = 'position: absolute; top: 0; right: 0; width: 4px; height: 100%; cursor: col-resize; background: transparent;';
    
    th.style.position = 'relative';
    th.style.minWidth = '60px';
    th.appendChild(resizer);
    
    let startX = 0;
    let startWidth = 0;
    let isResizing = false;
    
    const onMouseDown = (e) => {
      isResizing = true;
      startX = e.clientX;
      startWidth = th.offsetWidth;
      document.addEventListener('mousemove', onMouseMove);
      document.addEventListener('mouseup', onMouseUp);
      e.preventDefault();
    };
    
    const onMouseMove = (e) => {
      if (!isResizing) return;
      const newWidth = Math.max(60, startWidth + (e.clientX - startX));
      th.style.width = newWidth + 'px';
      
      // 同步所有相同列的单元格宽度
      const colCells = table.querySelectorAll(`td:nth-child(${index + 1}), th:nth-child(${index + 1})`);
      colCells.forEach(cell => {
        cell.style.width = newWidth + 'px';
      });
    };
    
    const onMouseUp = () => {
      isResizing = false;
      document.removeEventListener('mousemove', onMouseMove);
      document.removeEventListener('mouseup', onMouseUp);
    };
    
    resizer.addEventListener('mousedown', onMouseDown);
  });
}

// 排序功能
function enableColumnSort(table, allRowData, rowFields, colFields, valDefs, colAggTypes, formats) {
  const headers = table.querySelectorAll('thead tr:last-child th');
  
  headers.forEach((th, colIndex) => {
    // 只对聚合列添加排序功能
    if (colIndex >= rowFields.length) {
      th.style.cursor = 'pointer';
      th.style.userSelect = 'none';
      th.setAttribute('data-sortable', 'true');
      
      // 添加排序指示器
      const sortIndicator = document.createElement('span');
      sortIndicator.className = 'sort-indicator';
      sortIndicator.style.cssText = 'margin-left: 5px; opacity: 0.5; font-size: 10px;';
      sortIndicator.textContent = '⇅';
      th.appendChild(sortIndicator);
      
      let currentSort = null; // null, 'asc', 'desc'
      
      th.addEventListener('click', (e) => {
        e.preventDefault();
        
        // 清除其他列的排序状态
        headers.forEach((otherTh, otherIndex) => {
          if (otherIndex !== colIndex) {
            const otherIndicator = otherTh.querySelector('.sort-indicator');
            if (otherIndicator) {
              otherIndicator.textContent = '⇅';
              otherIndicator.style.opacity = '0.5';
            }
          }
        });
        
        // 切换当前列的排序状态
        if (currentSort === null || currentSort === 'desc') {
          currentSort = 'asc';
          sortIndicator.textContent = '↑';
          sortIndicator.style.opacity = '1';
        } else {
          currentSort = 'desc';
          sortIndicator.textContent = '↓';
          sortIndicator.style.opacity = '1';
        }
        
        // 执行排序
        sortTableByColumn(table, allRowData, colIndex, currentSort, rowFields, colFields, valDefs, colAggTypes, formats);
      });
    }
  });
}

// 按指定列对表格进行排序
function sortTableByColumn(table, allRowData, colIndex, direction, rowFields, colFields, valDefs, colAggTypes, formats) {
  // 检查是否有汇总行
  const tbody = table.querySelector('tbody');
  const allTrs = tbody.querySelectorAll('tr');
  const hasSummaryRow = allTrs[allTrs.length - 1]?.classList.contains('summary-row');
  let summaryTr = null;
  if (hasSummaryRow) {
    summaryTr = allTrs[allTrs.length - 1].cloneNode(true);
  }
  
  // 对层级数据进行智能排序 - 只对叶子节点（非小计行）进行排序
  const leafRows = allRowData.filter(rowInfo => !rowInfo.isSubtotal);
  const subtotalRows = allRowData.filter(rowInfo => rowInfo.isSubtotal);
  
  // 对叶子节点排序
  leafRows.sort((a, b) => {
    const aVal = a.row[colIndex];
    const bVal = b.row[colIndex];
    const aNum = num(aVal);
    const bNum = num(bVal);
    
    const result = aNum - bNum;
    return direction === 'asc' ? result : -result;
  });
  
  // 构建排序后的完整行数据结构
  const sortedAllRows = [];
  
  // 如果没有层级结构，直接使用排序结果
  if (rowFields.length <= 1) {
    sortedAllRows.push(...leafRows, ...subtotalRows);
  } else {
    // 有层级结构时，需要重新组织数据以保持层级关系
    const groupMap = new Map();
    
    // 按第一级分组
    leafRows.forEach(rowInfo => {
      const groupKey = rowInfo.path[0] || '';
      if (!groupMap.has(groupKey)) {
        groupMap.set(groupKey, []);
      }
      groupMap.get(groupKey).push(rowInfo);
    });
    
    // 按每组的总和排序组别
    const sortedGroups = Array.from(groupMap.entries()).sort((a, b) => {
      const aSum = a[1].reduce((sum, row) => sum + num(row.row[colIndex]), 0);
      const bSum = b[1].reduce((sum, row) => sum + num(row.row[colIndex]), 0);
      return direction === 'asc' ? aSum - bSum : bSum - aSum;
    });
    
    // 重新构建排序后的行
    sortedGroups.forEach(([groupKey, groupRows]) => {
      sortedAllRows.push(...groupRows);
      
      // 添加对应的小计行
      const groupSubtotals = subtotalRows.filter(s => s.path[0] === groupKey);
      sortedAllRows.push(...groupSubtotals);
    });
  }
  
  // 重新渲染表格主体
  tbody.innerHTML = '';
  
  // 添加排序后的所有行
  sortedAllRows.forEach(rowInfo => {
    const tr = createRowElement(rowInfo, rowFields, colFields, valDefs, colAggTypes, formats);
    tbody.appendChild(tr);
  });
  
  // 添加汇总行（如果存在）
  if (summaryTr) {
    tbody.appendChild(summaryTr);
  }
}

// 创建行元素的辅助函数
function createRowElement(rowInfo, rowFields, colFields, valDefs, colAggTypes, formats) {
  const r = rowInfo.row;
  const tr = document.createElement('tr');
  
  if (rowInfo.isSubtotal) {
    tr.className = 'subtotal-row';
    tr.style.cssText = 'background-color: #162032; font-weight: bold; border-top: 1px solid #334155;';
  }
  
  r.forEach((cell, ci) => {
    const td = document.createElement('td');
    
    if (rowInfo.isSubtotal) {
      td.style.cssText = 'background-color: #162032; font-weight: bold;';
    }
    
    if (ci < rowFields.length) {
      const indent = Math.max(0, rowInfo.level - 1) * 20;
      if (indent > 0) {
        td.style.paddingLeft = `${6 + indent}px`;
      }
      
      if (rowInfo.isSubtotal && cell && ci === rowInfo.level - 1) {
        td.textContent = `${cell} 小计`;
      } else {
        td.textContent = String(cell);
      }
    } else {
      const idx = ci - rowFields.length;
      const aggType = colAggTypes ? colAggTypes[idx % colAggTypes.length] : 'sum';
      const vd = valDefs[idx % valDefs.length];
      const key = `${vd.field}|${vd.agg||'sum'}`;
      const fmt = formats[key] || {decimals:2, thousand:true, currency:'', currencyPos:'prefix'};
      td.textContent = formatAgg(cell, aggType, fmt);
    }
    
    td.dataset.col = ci;
    tr.appendChild(td);
  });
  
  return tr;
}

btnExport.onclick=()=>{ const t=pivotContainer.querySelector('table'); if(!t) return; const rows=[]; t.querySelectorAll('tr').forEach(tr=>{ rows.push([...tr.children].map(td=>`"${String(td.textContent).replace(/"/g,'""')}"`).join(',')); }); const csv=rows.join('\n'); const blob=new Blob([csv],{type:'text/csv;charset=utf-8;'}); const url=URL.createObjectURL(blob); const a=document.createElement('a'); a.href=url; a.download=`pivot_${new Date().toISOString().replace(/[:.]/g,'-')}.csv`; document.body.appendChild(a); a.click(); setTimeout(()=>{document.body.removeChild(a); URL.revokeObjectURL(url);},0); };

// 筛选按钮事件处理
btnApplyFilters.onclick=()=>{
  updateFilters();
  applyFilters();
  renderPivot();
  log(`应用筛选完成，剩余数据: ${filteredRows.length} 行`);
};

btnClearFilters.onclick=()=>{
  rowFieldFilters = {};
  metricFieldFilters = {};
  filteredRows = [];
  buildFilterUI();
  renderPivot();
  log('已清除所有筛选条件');
};

// 创建配置面板的折叠按钮容器
function createConfigCollapsedButtons() {
  const existingContainer = configPanelEl.querySelector('.collapsed-buttons');
  if (existingContainer) {
    existingContainer.remove();
  }
  
  const container = document.createElement('div');
  container.className = 'collapsed-buttons';
  
  // 克隆重要按钮
  const btnLoad = $("#btnLoad").cloneNode(true);
  const btnPeek = $("#btnPeek").cloneNode(true);
  const btnExport = $("#btnExportCsv").cloneNode(true);
  
  // 设置按钮文本（缩写）
  btnLoad.textContent = '加载';
  btnPeek.textContent = '查看';
  btnExport.textContent = '导出';
  
  // 保持原有的事件处理（通过点击原按钮）
  btnLoad.onclick = () => $("#btnLoad").click();
  btnPeek.onclick = () => $("#btnPeek").click();
  btnExport.onclick = () => $("#btnExportCsv").click();
  
  container.appendChild(btnLoad);
  container.appendChild(btnPeek);
  container.appendChild(btnExport);
  
  configPanelEl.appendChild(container);
}

// 面板折叠/展开功能
toggleFilterEl.onclick=()=>{
  filterPanelEl.classList.toggle('collapsed');
  toggleFilterEl.textContent = filterPanelEl.classList.contains('collapsed') ? '▶' : '◀';
};

toggleConfigEl.onclick=()=>{
  configPanelEl.classList.toggle('collapsed');
  const isCollapsed = configPanelEl.classList.contains('collapsed');
  toggleConfigEl.textContent = isCollapsed ? '▶' : '◀';
  
  // 管理折叠状态下的按钮容器
  if (isCollapsed) {
    createConfigCollapsedButtons();
  } else {
    const container = configPanelEl.querySelector('.collapsed-buttons');
    if (container) {
      container.remove();
    }
  }
};

// 面板拖拽调整宽度功能
function initResize(handle, panel) {
  let isResizing = false;
  let startX = 0;
  let startWidth = 0;

  handle.addEventListener('mousedown', (e) => {
    isResizing = true;
    startX = e.clientX;
    startWidth = parseInt(window.getComputedStyle(panel).width, 10);
    document.addEventListener('mousemove', onMouseMove);
    document.addEventListener('mouseup', onMouseUp);
    e.preventDefault();
  });

  function onMouseMove(e) {
    if (!isResizing) return;
    const width = startWidth + e.clientX - startX;
    const minWidth = panel === filterPanelEl ? 200 : 300;
    const maxWidth = panel === filterPanelEl ? 500 : 800;
    
    if (width >= minWidth && width <= maxWidth) {
      panel.style.width = width + 'px';
      if (panel === configPanelEl) {
        panel.style.minWidth = width + 'px';
      }
    }
  }

  function onMouseUp() {
    isResizing = false;
    document.removeEventListener('mousemove', onMouseMove);
    document.removeEventListener('mouseup', onMouseUp);
  }
}

// 初始化调整大小功能
if (filterResizeEl && filterPanelEl) initResize(filterResizeEl, filterPanelEl);
if (configResizeEl && configPanelEl) initResize(configResizeEl, configPanelEl);

function tryParseJSON(s){ if(!s) return {}; try{ return JSON.parse(s); }catch{ return {}; } }
function makeBody(){ return {collection: collEl.value, filters: tryParseJSON(filtersEl.value), limit: Number(limitEl.value||5000), skip: Number(skipEl.value||0), projection: {"_id":0}}; }
async function postQuery(body){ try{ const r=await fetch(`${apiBaseEl.value}/api/query`,{method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify(body)}); const j=await r.json(); return Array.isArray(j.rows) ? j.rows : []; }catch(e){ log('查询失败: '+e.message); return []; } }

btnLoad.onclick=async ()=>{
  const filt=tryParseJSON(filtersEl.value);
  if(isUploadedMode){
    const uploadRows = Array.isArray(rawUploadRows) ? rawUploadRows : [];
    rawBaseRows = uploadRows.filter(r => matchesFilter(r, filt));
    log(`本地数据（上传）应用过滤后：${rawBaseRows.length} 行`);
    renderPivot(); return;
  }
  const body=makeBody(); currentCollection=body.collection; loadState(); renderComputedList(); rebuildAvailableFields(); renderZones(); renderFormatPanel(); if(viewModeEl) viewModeEl.value = viewMode; if(subtotalToggleEl) subtotalToggleEl.checked = subtotalEnabled; rawBaseRows=await postQuery(body); renderPivot(); btnSaveCloud.disabled=false; btnLoadCloud.disabled=false; btnListCloud.disabled=false; await loadCloudList();
};
async function doPeek(){ try{ const url=`${apiBaseEl.value}/api/peek?collection=${encodeURIComponent(collEl.value)}`; isUploadedMode=false; currentCollection=collEl.value; loadState(); renderComputedList(); rebuildAvailableFields(); renderZones(); renderFormatPanel(); if(viewModeEl) viewModeEl.value = viewMode; if(subtotalToggleEl) subtotalToggleEl.checked = subtotalEnabled; const r=await fetch(url); const j=await r.json(); rawBaseRows=Array.isArray(j.rows) ? j.rows : []; renderPivot(); btnSaveCloud.disabled=false; btnLoadCloud.disabled=false; btnListCloud.disabled=false; await loadCloudList(); }catch(e){ log('查看数据失败: '+e.message); rawBaseRows=[]; renderPivot(); } }
btnPeek.onclick=doPeek;

btnUpload.onclick=async ()=>{
  const f=fileInput.files && fileInput.files[0]; if(!f){ alert('请选择文件'); return; }
  
  // 显示上传进度
  btnUpload.disabled = true;
  btnUpload.textContent = '上传中...';
  
  try {
    const fd=new FormData();
    fd.append('file', f);
    if(sheetInput.value) fd.append('sheet', sheetInput.value);
    fd.append('start_row', String(Number(startRowInput.value||1)));
    
    const r=await fetch(`${apiBaseEl.value}/api/upload`,{method:'POST', body: fd});
    const j=await r.json();
    
    if(!r.ok) {
      throw new Error(j.detail || '上传失败');
    }
    
    if(j && Array.isArray(j.rows)){
      rawUploadRows=j.rows; rawBaseRows=rawUploadRows.slice(); isUploadedMode=true;
      const label=`__upload__::${j.filename||f.name}`;
      let found=false; [...collEl.options].forEach(o=>{ if(o.value===label) found=true; });
      if(!found){ const o=document.createElement('option'); o.value=label; o.textContent=label; collEl.appendChild(o); }
      collEl.value=label; currentCollection=label; loadState(); renderComputedList(); rebuildAvailableFields(); renderZones(); renderFormatPanel(); if(viewModeEl) viewModeEl.value = viewMode; if(subtotalToggleEl) subtotalToggleEl.checked = subtotalEnabled; renderPivot();
      btnSaveCloud.disabled=false; btnLoadCloud.disabled=false; btnListCloud.disabled=false;
      log(`上传完成：${j.filename}，行数=${j.count}${j.message ? '，' + j.message : ''}`);
      await loadCloudList();
    }else{ 
      throw new Error('服务器返回数据格式错误或没有有效数据'); 
    }
  } catch(e) {
    log('上传失败：' + e.message);
    alert('上传失败：' + e.message);
  } finally {
    btnUpload.disabled = false;
    btnUpload.textContent = '读取并加载';
  }
};

btnAddCalc.onclick=()=>{ const name=(calcNameEl.value||'').trim(); const expr=(calcExprEl.value||'').trim(); if(!name||!expr) return; const fn=compileExpr(expr); const fnAgg=compileAggExpr(expr); computedDefs=computedDefs.filter(d=>d.name!==name); computedDefs.push({name,expr,fn,fnAgg}); calcNameEl.value=''; calcExprEl.value=''; renderComputedList(); renderPivot(); saveState(); };
function renderComputedList(){ computedListEl.innerHTML=''; computedDefs.forEach((d,i)=>{ const w=document.createElement('div'); w.className='field'; w.draggable=true; w.dataset.field=d.name; w.innerHTML=`<b>${d.name}</b> = <code>${d.expr}</code> <span class='remove'>✕</span>`; w.addEventListener('dragstart',onDragStart); w.querySelector('.remove').onclick=()=>{ computedDefs.splice(i,1); renderComputedList(); renderPivot(); saveState(); }; computedListEl.appendChild(w); }); }

btnSaveCloud.disabled=true; btnLoadCloud.disabled=true; btnListCloud.disabled=true;
viewModeEl.onchange=()=>{ viewMode=viewModeEl.value; saveState(); renderPivot(); };
subtotalToggleEl.onchange=()=>{ subtotalEnabled=!!subtotalToggleEl.checked; saveState(); renderPivot(); };

iferrAllToggle && (iferrAllToggle.onchange=()=>{ iferrDefaultEnabled=!!iferrAllToggle.checked; saveState(); renderPivot(); });
iferrFallbackEl && (iferrFallbackEl.oninput=()=>{ saveState(); renderPivot(); });

btnSaveCloud.onclick=async ()=>{ if(!currentCollection) return; const name=(profileNameEl.value||'').trim(); if(!name) return; const body={collection: currentCollection, name, layout, hiddenFields:[...hiddenFields], formulas: computedDefs.map(({name,expr})=>({name,expr})), formats, subtotalEnabled, viewMode, iferrorAllEnabled: !!iferrDefaultEnabled, iferrorFallback: Number(iferrFallbackEl?.value ?? 0) || 0 }; const r=await fetch(`${apiBaseEl.value}/api/prefs/save`,{method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify(body)}); const j=await r.json(); log('保存云端配置: '+JSON.stringify(j)); await loadCloudList(); };
async function loadCloudList(){ const url=`${apiBaseEl.value}/api/prefs/list`; try{ const r=await fetch(url); const j=await r.json(); const items=j.items||[]; const cloudListEl=$('#cloudList'); cloudListEl.innerHTML=''; if(items.length===0){ cloudListEl.innerHTML='<div class="small">暂无保存的配置</div>'; return; } items.forEach(item=>{ const div=document.createElement('div'); div.className='cloud-item'; div.style.cssText='margin:4px 0;padding:8px;background:#0f1627;border:1px solid #2a374d;border-radius:6px;cursor:pointer;display:flex;justify-content:space-between;align-items:center;'; div.innerHTML=`<span onclick="loadCloudConfig('${item.collection}','${item.name}')" style="flex:1;color:#e7ecf6;">[${item.collection}] ${item.name}</span><span class="small" style="color:#9fb0c7;margin-left:8px;">${item.updatedAt||''}</span><span onclick="deleteCloudConfig('${item.collection}','${item.name}')" style="margin-left:8px;color:#f87171;cursor:pointer;">✕</span>`; cloudListEl.appendChild(div); }); }catch(e){ log('加载配置列表失败: '+e.message); } } window.loadCloudConfig=async (collection, name)=>{ if(!collection || !name) return; profileNameEl.value=name; const url=`${apiBaseEl.value}/api/prefs/get?collection=${encodeURIComponent(collection)}&name=${encodeURIComponent(name)}`; try{ const r=await fetch(url); const j=await r.json(); if(j && j.doc){ const d=j.doc; layout=d.layout||{rows:[],cols:[],vals:[]}; hiddenFields=new Set(d.hiddenFields||[]); computedDefs=(d.formulas||[]).map(x=>({...x, fn:compileExpr(x.expr), fnAgg:compileAggExpr(x.expr)})); formats=d.formats||{}; subtotalEnabled=!!d.subtotalEnabled; viewMode=d.viewMode||'pivot'; iferrDefaultEnabled=!!d.iferrorAllEnabled; iferrDefaultFallback=Number(d.iferrorFallback||0); if(iferrAllToggle) iferrAllToggle.checked=!!iferrDefaultEnabled; if(iferrFallbackEl) iferrFallbackEl.value=String(iferrDefaultFallback); renderComputedList(); rebuildAvailableFields(); renderZones(); viewModeEl.value=viewMode; subtotalToggleEl.checked=subtotalEnabled; renderPivot(); saveState(); log(`加载配置: [${collection}] ${name}`); }else{ log('配置不存在: '+name); } }catch(e){ log('加载配置失败: '+e.message); } }; window.deleteCloudConfig=async (collection, name)=>{ if(!confirm(`确定删除配置 "[${collection}] ${name}" 吗？`)) return; const url=`${apiBaseEl.value}/api/prefs/delete?collection=${encodeURIComponent(collection)}&name=${encodeURIComponent(name)}`; try{ const r=await fetch(url,{method:'DELETE'}); const j=await r.json(); log(`删除配置: [${collection}] ${name}`); await loadCloudList(); }catch(e){ log('删除失败: '+e.message); } }; btnLoadCloud.onclick=async ()=>{ if(!currentCollection) return; const name=(profileNameEl.value||'').trim(); if(!name) return; await loadCloudConfig(currentCollection, name); }; btnListCloud.onclick=loadCloudList;

// 冻结功能实现
let freezeSettings = { rows: 1, cols: 0 };

function applyFreeze() {
  const table = pivotContainer.querySelector('table');
  if (!table) {
    log('应用冻结失败：未找到表格');
    return;
  }
  
  const freezeRows = parseInt($('#freezeRows').value) || 0;
  const freezeCols = parseInt($('#freezeCols').value) || 0;
  
  log(`应用冻结设置：行=${freezeRows}, 列=${freezeCols}`);
  
  freezeSettings.rows = freezeRows;
  freezeSettings.cols = freezeCols;
  
  // 清除之前的冻结样式（但不重置设置）
  clearFreezeStyles();
  
  if (freezeRows === 0 && freezeCols === 0) {
    log('冻结行列都为0，跳过应用');
    return;
  }
  
  // 添加冻结类到容器而不是表格
  pivotContainer.classList.add('frozen');
  log(`容器添加frozen类，容器类列表：${pivotContainer.className}`);
  
  // 处理表头冻结
  const headerRows = table.querySelectorAll('thead tr');
  log(`找到表头行数：${headerRows.length}`);
  
  headerRows.forEach((tr, rowIndex) => {
    if (rowIndex < freezeRows) {
      const headers = tr.querySelectorAll('th');
      log(`表头行${rowIndex}包含${headers.length}列`);
      
      headers.forEach((th, colIndex) => {
        if (colIndex < freezeCols) {
          th.classList.add('frozen-corner');
          log(`列${colIndex}添加frozen-corner类`);
        } else {
          th.classList.add('frozen-row');
          log(`列${colIndex}添加frozen-row类`);
        }
      });
    }
  });
  
  // 处理冻结列
  if (freezeCols > 0) {
    // 处理表头的冻结列(已经在上面处理过corner了，这里处理非冻结行的列)
    const allHeaderCells = table.querySelectorAll('thead th');
    allHeaderCells.forEach((th, index) => {
      const colIndex = index % (table.querySelector('thead tr')?.children.length || 1);
      if (colIndex < freezeCols && !th.classList.contains('frozen-corner')) {
        th.classList.add('frozen-col');
      }
    });
    
    // 处理表体的冻结列
    const bodyRows = table.querySelectorAll('tbody tr');
    bodyRows.forEach(tr => {
      const cells = tr.querySelectorAll('td');
      for (let i = 0; i < freezeCols && i < cells.length; i++) {
        cells[i].classList.add('frozen-col');
      }
    });
    
    // 计算并设置冻结列的left位置
    let leftOffset = 0;
    for (let colIndex = 0; colIndex < freezeCols; colIndex++) {
      // 获取该列的宽度
      const sampleCell = table.querySelector(`thead th:nth-child(${colIndex + 1})`);
      if (sampleCell) {
        const colWidth = sampleCell.offsetWidth;
        
        // 设置该列的left位置
        const colCells = table.querySelectorAll(`th:nth-child(${colIndex + 1}), td:nth-child(${colIndex + 1})`);
        colCells.forEach(cell => {
          if (cell.classList.contains('frozen-col') || cell.classList.contains('frozen-corner')) {
            cell.style.left = leftOffset + 'px';
          }
        });
        
        leftOffset += colWidth;
      }
    }
  }
  
  saveState();
  log(`已应用冻结设置: ${freezeRows}行, ${freezeCols}列`);
}

function clearFreezeStyles() {
  const table = pivotContainer.querySelector('table');
  if (!table) return;
  
  // 移除容器的冻结类
  pivotContainer.classList.remove('frozen');
  
  // 移除所有冻结样式
  table.querySelectorAll('.frozen-row, .frozen-col, .frozen-corner').forEach(cell => {
    cell.classList.remove('frozen-row', 'frozen-col', 'frozen-corner');
    cell.style.left = '';
  });
}

function clearFreeze() {
  clearFreezeStyles();
  
  freezeSettings = { rows: 0, cols: 0 };
  $('#freezeRows').value = '0';
  $('#freezeCols').value = '0';
  
  saveState();
  log('已清除所有冻结设置');
}

// 冻结控件事件绑定将在初始化时执行

// 应用已保存的冻结设置（不从输入框读取）
function reapplyFreeze() {
  const table = pivotContainer.querySelector('table');
  if (!table || (freezeSettings.rows === 0 && freezeSettings.cols === 0)) return;
  
  clearFreezeStyles();
  
  const freezeRows = freezeSettings.rows;
  const freezeCols = freezeSettings.cols;
  
  // 添加冻结类
  table.classList.add('frozen');
  
  // 处理表头冻结
  const headerRows = table.querySelectorAll('thead tr');
  headerRows.forEach((tr, rowIndex) => {
    if (rowIndex < freezeRows) {
      const headers = tr.querySelectorAll('th');
      headers.forEach((th, colIndex) => {
        if (colIndex < freezeCols) {
          th.classList.add('frozen-corner');
        } else {
          th.classList.add('frozen-row');
        }
      });
    }
  });
  
  // 处理冻结列
  if (freezeCols > 0) {
    const allHeaderCells = table.querySelectorAll('thead th');
    allHeaderCells.forEach((th, index) => {
      const colIndex = index % (table.querySelector('thead tr')?.children.length || 1);
      if (colIndex < freezeCols && !th.classList.contains('frozen-corner')) {
        th.classList.add('frozen-col');
      }
    });
    
    const bodyRows = table.querySelectorAll('tbody tr');
    bodyRows.forEach(tr => {
      const cells = tr.querySelectorAll('td');
      for (let i = 0; i < freezeCols && i < cells.length; i++) {
        cells[i].classList.add('frozen-col');
      }
    });
    
    // 计算并设置冻结列的left位置
    let leftOffset = 0;
    for (let colIndex = 0; colIndex < freezeCols; colIndex++) {
      const sampleCell = table.querySelector(`thead th:nth-child(${colIndex + 1})`);
      if (sampleCell) {
        const colWidth = sampleCell.offsetWidth;
        const colCells = table.querySelectorAll(`th:nth-child(${colIndex + 1}), td:nth-child(${colIndex + 1})`);
        colCells.forEach(cell => {
          if (cell.classList.contains('frozen-col') || cell.classList.contains('frozen-corner')) {
            cell.style.left = leftOffset + 'px';
          }
        });
        leftOffset += colWidth;
      }
    }
  }
}

// 在renderPivot和renderRawTable完成后重新应用冻结
const originalRenderPivot = renderPivot;
const originalRenderRawTable = renderRawTable;

renderPivot = function() {
  originalRenderPivot.call(this);
  // 延迟应用冻结，确保表格渲染完成
  setTimeout(reapplyFreeze, 50);
};

renderRawTable = function(rows) {
  originalRenderRawTable.call(this, rows);
  setTimeout(reapplyFreeze, 50);
};

(function(){ 
  // 确保DOM加载完成
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }
  
  function init() {
    log('DOM加载完成，开始初始化');
    
    // 检查关键DOM元素是否存在
    if (!apiBaseEl) {
      log('错误：无法找到apiBase输入框');
      return;
    }
    if (!healthEl) {
      log('错误：无法找到health状态显示');
      return;
    }
    
    setApiBaseFromConfig(); 
    smartPingAndInit().then(()=> { 
    log('前端初始化完成（IFERROR 全局兜底 • 修复 DIV v3 + Upload + Crosshair • repack + 冻结功能）'); 
    loadCloudList(); 
    
    // 绑定冻结控件事件
    const btnApplyFreeze = $('#btnApplyFreeze');
    const btnClearFreeze = $('#btnClearFreeze');
    const freezeRowsInput = $('#freezeRows');
    const freezeColsInput = $('#freezeCols');
    
    log(`调试信息：btnApplyFreeze=${!!btnApplyFreeze}, btnClearFreeze=${!!btnClearFreeze}`);
    log(`调试信息：freezeRowsInput=${!!freezeRowsInput}, freezeColsInput=${!!freezeColsInput}`);
    
    if (btnApplyFreeze) {
      btnApplyFreeze.onclick = function() {
        log('应用冻结按钮被点击');
        applyFreeze();
      };
    } else {
      log('错误：找不到应用冻结按钮');
    }
    
    if (btnClearFreeze) {
      btnClearFreeze.onclick = function() {
        log('清除冻结按钮被点击');
        clearFreeze();
      };
    } else {
      log('错误：找不到清除冻结按钮');
    }
    
    log('冻结功能控件绑定完成');
    }); 
  }
})();