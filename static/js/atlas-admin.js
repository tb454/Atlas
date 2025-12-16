async function api(path, opts={}){
  const r = await fetch(path, opts);
  const j = await r.json().catch(()=> ({}));
  if(!r.ok) throw new Error(j.detail || `HTTP ${r.status}`);
  return j;
}

async function logout(){
  await api("/api/auth/logout", {method:"POST"});
  window.location.href = "/static/atlas-login.html";
}

function showTab(name){
  ["onboarding","vault","owners","assets"].forEach(t=>{
    document.getElementById("tab_"+t).style.display = (t===name) ? "block" : "none";
  });
  if(name==="onboarding") renderOnboarding();
  if(name==="vault") renderVault();
  if(name==="owners") renderOwners();
  if(name==="assets") renderAssets();
}

async function renderOnboarding(){
  const el = document.getElementById("tab_onboarding");
  el.innerHTML = `<h4>Onboarding Submissions</h4>
    <div class="d-flex gap-2 mb-3">
      <select id="ob_status" class="form-select form-select-sm" style="max-width:220px" onchange="renderOnboarding()">
        <option value="submitted">submitted</option>
        <option value="needs_more">needs_more</option>
        <option value="approved">approved</option>
        <option value="rejected">rejected</option>
        <option value="all">all</option>
      </select>
    </div>
    <div id="ob_table"></div>
    <div id="ob_detail" class="mt-3"></div>
  `;
  const status = document.getElementById("ob_status").value;
  const data = await api(`/api/admin/onboarding?status=${encodeURIComponent(status)}&limit=50`);
  const rows = data.items || [];
  const table = `
    <table class="table table-dark table-sm">
      <thead><tr>
        <th>Created</th><th>Status</th><th>Owner</th><th>Email</th><th>Assets</th><th></th>
      </tr></thead>
      <tbody>
        ${rows.map(r=>`
          <tr>
            <td>${new Date(r.created_at).toLocaleString()}</td>
            <td>${r.status}</td>
            <td>${r.owner_name||""}</td>
            <td>${r.owner_email||""}</td>
            <td>${r.ip_assets_count}</td>
            <td><button class="btn btn-outline-light btn-sm" onclick="openOnboarding('${r.id}')">Open</button></td>
          </tr>
        `).join("")}
      </tbody>
    </table>`;
  document.getElementById("ob_table").innerHTML = table;
}

async function openOnboarding(id){
  const el = document.getElementById("ob_detail");
  el.innerHTML = `<div class="text-secondary">Loading…</div>`;
  const data = await api(`/api/admin/onboarding/${id}`);
  const s = data.submission;

  el.innerHTML = `
    <hr/>
    <h5>Submission</h5>
    <div class="mono">ID: ${s.id}</div>
    <div class="mono">Status: ${s.status}</div>

    <div class="mt-2 d-flex gap-2">
      <select id="set_status" class="form-select form-select-sm" style="max-width:220px">
        ${["submitted","needs_more","approved","rejected"].map(x=>`<option ${x===s.status?"selected":""}>${x}</option>`).join("")}
      </select>
      <input id="set_notes" class="form-control form-control-sm" placeholder="notes (optional)" value="${(s.notes||"").replaceAll('"','&quot;')}"/>
      <button class="btn btn-success btn-sm" onclick="setOnboardingStatus('${id}')">Update</button>
      <button class="btn btn-warning btn-sm" onclick="approveOnboarding('${id}')">Approve + Create Login</button>
    </div>

    <pre class="mono p-3 mt-3" style="background:#0d1522;border:1px solid #2a3a52;border-radius:8px;max-height:380px;overflow:auto">${JSON.stringify(s, null, 2)}</pre>
  `;
}

async function approveOnboarding(id){
  if(!confirm("Approve this onboarding and create Owner + Assets + Owner login?")) return;
  const j = await api(`/api/admin/onboarding/${id}/approve`, { method:"POST" });
  alert(
    `OWNER LOGIN CREATED\n\nEmail: ${j.user_email}\nTemp Password: ${j.temp_password}\n\n(You should copy this now.)`
  );
  await renderOnboarding();
  await openOnboarding(id);
}

async function setOnboardingStatus(id){
  const status = document.getElementById("set_status").value;
  const notes = document.getElementById("set_notes").value;
  await api(`/api/admin/onboarding/${id}/status`, {
    method:"POST",
    headers:{ "Content-Type":"application/json" },
    body: JSON.stringify({status, notes})
  });
  await renderOnboarding();
  await openOnboarding(id);
}

async function renderVault(){
  const el = document.getElementById("tab_vault");
  el.innerHTML = `
    <h4>Vault</h4>
    <div class="muted mb-3">Upload a sealed bundle (ZIP/JSONL) and Atlas stores + hashes it forever.</div>

    <div class="row g-2 mb-3">
      <div class="col-md-2"><input id="v_source" class="form-control form-control-sm" value="dossier"/></div>
      <div class="col-md-2"><input id="v_org" class="form-control form-control-sm" placeholder="org_id"/></div>
      <div class="col-md-2"><input id="v_tenant" class="form-control form-control-sm" placeholder="tenant_id"/></div>
      <div class="col-md-2"><input id="v_schema" class="form-control form-control-sm" placeholder="schema_version"/></div>
      <div class="col-md-4"><input id="v_manifest" class="form-control form-control-sm" placeholder='manifest_json (optional JSON)'/></div>
    </div>

    <div class="d-flex gap-2 align-items-center mb-3">
      <input id="v_file" type="file" class="form-control form-control-sm"/>
      <button class="btn btn-success btn-sm" onclick="vaultUpload()">Ingest</button>
    </div>

    <div id="v_msg"></div>
    <div id="v_table" class="mt-3"></div>
  `;
  await refreshVaultList();
}

async function vaultUpload(){
  const msg = document.getElementById("v_msg");
  msg.innerHTML = "";
  const fd = new FormData();
  fd.append("source_key", document.getElementById("v_source").value.trim());
  fd.append("org_id", document.getElementById("v_org").value.trim());
  fd.append("tenant_id", document.getElementById("v_tenant").value.trim());
  fd.append("schema_version", document.getElementById("v_schema").value.trim());
  fd.append("manifest_json", document.getElementById("v_manifest").value.trim());
  const f = document.getElementById("v_file").files[0];
  if(!f){ msg.innerHTML = `<div class="alert alert-danger">Choose a file.</div>`; return; }
  fd.append("bundle", f);

  try{
    const r = await fetch("/api/vault/ingest", { method:"POST", body: fd });
    const j = await r.json();
    if(!r.ok) throw new Error(j.detail || "ingest failed");
    msg.innerHTML = `<div class="alert alert-success">Ingested. Object ID: <span class="mono">${j.object_id}</span><br/>SHA256: <span class="mono">${j.sha256}</span></div>`;
    await refreshVaultList();
  }catch(e){
    msg.innerHTML = `<div class="alert alert-danger">${String(e)}</div>`;
  }
}

async function refreshVaultList(){
  const data = await api(`/api/admin/vault/objects?source_key=all&limit=50`);
  const rows = data.items || [];
  const table = `
    <table class="table table-dark table-sm">
      <thead><tr>
        <th>Created</th><th>Source</th><th>Org</th><th>File</th><th>Bytes</th><th>SHA256</th><th></th>
      </tr></thead>
      <tbody>
        ${rows.map(r=>`
          <tr>
            <td>${new Date(r.created_at).toLocaleString()}</td>
            <td>${r.source_key}</td>
            <td>${r.org_id||""}</td>
            <td class="mono">${r.filename}</td>
            <td class="mono">${r.byte_size}</td>
            <td class="mono" style="max-width:240px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${r.sha256}</td>
            <td><a class="btn btn-outline-light btn-sm" href="/api/admin/vault/objects/${r.id}/download">Download</a></td>
          </tr>
        `).join("")}
      </tbody>
    </table>`;
  document.getElementById("v_table").innerHTML = table;
}

async function renderOwners(){
  const el = document.getElementById("tab_owners");
  el.innerHTML = `
    <h4>IP Owners</h4>
    <div class="row g-2 mb-3">
      <div class="col-md-4"><input id="o_name" class="form-control form-control-sm" placeholder="legal_name"/></div>
      <div class="col-md-2"><input id="o_type" class="form-control form-control-sm" placeholder="entity_type"/></div>
      <div class="col-md-2"><input id="o_jur" class="form-control form-control-sm" placeholder="jurisdiction"/></div>
      <div class="col-md-4"><input id="o_email" class="form-control form-control-sm" placeholder="email"/></div>
      <div class="col-md-8"><input id="o_addr" class="form-control form-control-sm" placeholder="address"/></div>
      <div class="col-md-2"><input id="o_phone" class="form-control form-control-sm" placeholder="phone"/></div>
      <div class="col-md-2"><button class="btn btn-success btn-sm w-100" onclick="createOwner()">Create</button></div>
    </div>
    <div id="o_msg"></div>
    <div id="o_table" class="mt-3"></div>
  `;
  await refreshOwners();
}

async function createOwner(){
  const payload = {
    legal_name: document.getElementById("o_name").value.trim(),
    entity_type: document.getElementById("o_type").value.trim(),
    jurisdiction: document.getElementById("o_jur").value.trim(),
    email: document.getElementById("o_email").value.trim(),
    address: document.getElementById("o_addr").value.trim(),
    phone: document.getElementById("o_phone").value.trim()
  };
  try{
    const j = await api("/api/admin/owners", {
      method:"POST",
      headers:{ "Content-Type":"application/json" },
      body: JSON.stringify(payload)
    });
    document.getElementById("o_msg").innerHTML = `<div class="alert alert-success">Created owner ${j.id}</div>`;
    await refreshOwners();
  }catch(e){
    document.getElementById("o_msg").innerHTML = `<div class="alert alert-danger">${String(e)}</div>`;
  }
}

async function refreshOwners(){
  const data = await api("/api/admin/owners?limit=50");
  const rows = data.items || [];
  const table = `
    <table class="table table-dark table-sm">
      <thead><tr><th>Created</th><th>Name</th><th>Type</th><th>Jurisdiction</th><th>Email</th><th class="mono">ID</th></tr></thead>
      <tbody>${rows.map(r=>`
        <tr>
          <td>${new Date(r.created_at).toLocaleString()}</td>
          <td>${r.legal_name}</td>
          <td>${r.entity_type||""}</td>
          <td>${r.jurisdiction||""}</td>
          <td>${r.email||""}</td>
          <td class="mono">${r.id}</td>
        </tr>`).join("")}</tbody>
    </table>`;
  document.getElementById("o_table").innerHTML = table;
}

async function renderAssets(){
  const el = document.getElementById("tab_assets");
  el.innerHTML = `
    <h4>IP Assets</h4>
    <div class="muted mb-2">Create assets here (you can link an Owner ID or leave null until verified).</div>
    <div class="row g-2 mb-3">
      <div class="col-md-3"><input id="a_owner" class="form-control form-control-sm" placeholder="owner_id (optional)"/></div>
      <div class="col-md-4"><input id="a_title" class="form-control form-control-sm" placeholder="title"/></div>
      <div class="col-md-2"><input id="a_type" class="form-control form-control-sm" placeholder="asset_type"/></div>
      <div class="col-md-3"><input id="a_reg" class="form-control form-control-sm" placeholder="reg_no"/></div>
      <div class="col-md-12"><textarea id="a_desc" class="form-control form-control-sm" rows="2" placeholder="description"></textarea></div>
      <div class="col-md-2"><button class="btn btn-success btn-sm w-100" onclick="createAsset()">Create</button></div>
    </div>
    <div id="a_msg"></div>
    <div id="a_table" class="mt-3"></div>
  `;
  await refreshAssets();
}

async function createAsset(){
  const payload = {
    owner_id: document.getElementById("a_owner").value.trim() || null,
    title: document.getElementById("a_title").value.trim(),
    asset_type: document.getElementById("a_type").value.trim(),
    reg_no: document.getElementById("a_reg").value.trim(),
    description: document.getElementById("a_desc").value.trim()
  };
  try{
    const j = await api("/api/admin/assets", {
      method:"POST",
      headers:{ "Content-Type":"application/json" },
      body: JSON.stringify(payload)
    });
    document.getElementById("a_msg").innerHTML = `<div class="alert alert-success">Created asset ${j.id}</div>`;
    await refreshAssets();
  }catch(e){
    document.getElementById("a_msg").innerHTML = `<div class="alert alert-danger">${String(e)}</div>`;
  }
}

async function refreshAssets(){
  const data = await api("/api/admin/assets?limit=50");
  const rows = data.items || [];
  const table = `
    <table class="table table-dark table-sm">
      <thead><tr><th>Created</th><th>Title</th><th>Type</th><th>Status</th><th>Reg No</th><th>Owner</th><th class="mono">ID</th></tr></thead>
      <tbody>${rows.map(r=>`
        <tr>
          <td>${new Date(r.created_at).toLocaleString()}</td>
          <td>${r.title}</td>
          <td>${r.asset_type||""}</td>
          <td>${r.status||""}</td>
          <td>${r.reg_no||""}</td>
          <td>${r.owner_name||""}</td>
          <td class="mono">${r.id}</td>
        </tr>`).join("")}</tbody>
    </table>`;
  document.getElementById("a_table").innerHTML = table;
}

(async function init(){
  try{
    const me = await api("/api/auth/me");
    if(!me.email){ window.location.href="/static/atlas-login.html"; return; }
  }catch(e){
    window.location.href="/static/atlas-login.html"; return;
  }
  showTab("onboarding");
})();
