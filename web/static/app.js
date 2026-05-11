const state = {
  token: localStorage.getItem("cloudik_token"),
  user: null,
  socket: null,
  imageUrls: new Map(),
  pendingRefreshTimer: null,
  pendingRefreshInFlight: false,
  pendingRefreshAttempts: 0,
  trackedPendingKeys: new Set(),
};

const els = {
  authView: document.getElementById("authView"),
  appView: document.getElementById("appView"),
  authAlert: document.getElementById("authAlert"),
  appAlert: document.getElementById("appAlert"),
  loginForm: document.getElementById("loginForm"),
  registerForm: document.getElementById("registerForm"),
  uploadForm: document.getElementById("uploadForm"),
  refreshBtn: document.getElementById("refreshBtn"),
  logoutBtn: document.getElementById("logoutBtn"),
  gallery: document.getElementById("gallery"),
  galleryMeta: document.getElementById("galleryMeta"),
  signedUser: document.getElementById("signedUser"),
  profileUsername: document.getElementById("profileUsername"),
  profileBucket: document.getElementById("profileBucket"),
  billingGrid: document.getElementById("billingGrid"),
};

function showAlert(element, message, type = "success") {
  element.className = `alert alert-${type}`;
  element.textContent = message;
}

function hideAlert(element) {
  element.className = "alert d-none";
  element.textContent = "";
}

function authHeaders(extra = {}) {
  return {
    ...extra,
    Authorization: `Bearer ${state.token}`,
  };
}

async function requestJson(path, options = {}) {
  const response = await fetch(path, {
    ...options,
    headers: authHeaders({
      Accept: "application/json",
      ...(options.headers || {}),
    }),
  });

  const contentType = response.headers.get("content-type") || "";
  const payload = contentType.includes("application/json") ? await response.json() : await response.text();

  if (!response.ok) {
    const detail = typeof payload === "object" ? payload.detail : payload;
    throw new Error(Array.isArray(detail) ? detail.map((item) => item.msg).join(", ") : detail || "Request failed");
  }
  return payload;
}

async function publicJson(path, body) {
  const response = await fetch(path, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Accept: "application/json",
    },
    body: JSON.stringify(body),
  });
  const payload = await response.json();
  if (!response.ok) {
    throw new Error(payload.detail || "Request failed");
  }
  return payload;
}

function setSession(authPayload) {
  state.token = authPayload.token;
  state.user = authPayload.user;
  localStorage.setItem("cloudik_token", state.token);
}

function clearSession() {
  state.token = null;
  state.user = null;
  localStorage.removeItem("cloudik_token");
  closeStatusSocket();
  stopPendingRefresh();
  state.pendingRefreshAttempts = 0;
  state.trackedPendingKeys.clear();
}

function showAuth() {
  els.authView.classList.remove("d-none");
  els.appView.classList.add("d-none");
}

function showApp() {
  els.authView.classList.add("d-none");
  els.appView.classList.remove("d-none");
}

function bytes(value) {
  const units = ["B", "KB", "MB", "GB"];
  let size = Number(value || 0);
  let index = 0;
  while (size >= 1024 && index < units.length - 1) {
    size /= 1024;
    index += 1;
  }
  return `${size.toFixed(index === 0 ? 0 : 1)} ${units[index]}`;
}

function sanitizeObjectKey(name) {
  return name.trim().replace(/[\\/:*?"<>|]+/g, "-").replace(/\s+/g, "-");
}

function escapeHtml(value) {
  return String(value).replace(/[&<>"']/g, (char) => ({
    "&": "&amp;",
    "<": "&lt;",
    ">": "&gt;",
    "\"": "&quot;",
    "'": "&#039;",
  }[char]));
}

async function bootApp() {
  if (!state.token) {
    showAuth();
    return;
  }
  try {
    state.user = await requestJson("/me");
    showApp();
    renderUser();
    await Promise.all([loadObjects(), loadBilling()]);
    connectStatusSocket();
  } catch (error) {
    clearSession();
    showAuth();
  }
}

function renderUser() {
  els.signedUser.textContent = state.user.username;
  els.profileUsername.textContent = state.user.username;
  els.profileBucket.textContent = state.user.bucket_id;
}

async function loadObjects() {
  const objects = await requestJson("/me/objects");
  els.galleryMeta.textContent = objects.length === 1 ? "1 obrazek v osobnim bucketu" : `${objects.length} obrazku v osobnim bucketu`;
  renderObjects(objects);

  const objectKeys = new Set(objects.map((item) => item.object_key));
  for (const trackedKey of Array.from(state.trackedPendingKeys)) {
    if (!objectKeys.has(trackedKey)) {
      state.trackedPendingKeys.delete(trackedKey);
    }
  }

  for (const item of objects) {
    if (state.trackedPendingKeys.has(item.object_key) && item.status === "ready") {
      state.trackedPendingKeys.delete(item.object_key);
    }
  }

  const hasTrackedUploading = objects.some(
    (item) => state.trackedPendingKeys.has(item.object_key) && item.status === "uploading",
  );

  if (hasTrackedUploading) {
    schedulePendingRefresh();
  } else {
    stopPendingRefresh();
    state.pendingRefreshAttempts = 0;
  }
}

function stopPendingRefresh() {
  if (state.pendingRefreshTimer) {
    clearTimeout(state.pendingRefreshTimer);
    state.pendingRefreshTimer = null;
  }
}

function schedulePendingRefresh(delayMs = 1500) {
  if (state.pendingRefreshTimer || !state.token) {
    return;
  }
  state.pendingRefreshTimer = window.setTimeout(async () => {
    state.pendingRefreshTimer = null;
    if (state.pendingRefreshInFlight || !state.token) {
      schedulePendingRefresh(delayMs);
      return;
    }

    if (state.pendingRefreshAttempts >= 20) {
      state.trackedPendingKeys.clear();
      state.pendingRefreshAttempts = 0;
      showAlert(els.appAlert, "Upload porad ceka na potvrzeni z Haystack. Zkontroluj, jestli bezi haystack_node.py.", "warning");
      return;
    }

    state.pendingRefreshInFlight = true;
    state.pendingRefreshAttempts += 1;
    try {
      await Promise.all([loadObjects(), loadBilling()]);
    } catch {
      schedulePendingRefresh(2500);
    } finally {
      state.pendingRefreshInFlight = false;
    }
  }, delayMs);
}

function renderObjects(objects) {
  for (const url of state.imageUrls.values()) {
    URL.revokeObjectURL(url);
  }
  state.imageUrls.clear();
  els.gallery.innerHTML = "";

  if (objects.length === 0) {
    stopPendingRefresh();
    els.gallery.innerHTML = `<div class="empty-state">Zatim tu neni zadny obrazek.</div>`;
    return;
  }

  for (const item of objects) {
    const safeKey = escapeHtml(item.object_key);
    const isReady = item.status === "ready";
    const objectId = item.object_id || item.record_id;
    const statusLabel = isReady ? "ready" : "uploading";
    const sizeLabel = isReady ? bytes(item.size) : "cekam na potvrzeni ulozeni";
    const card = document.createElement("article");
    card.className = "picture-card";
    card.innerHTML = `
      <img class="picture-preview" alt="${safeKey}">
      <div class="picture-body">
        <div class="picture-title">${safeKey}</div>
        <div class="picture-meta">${sizeLabel}</div>
        <div class="picture-meta mb-3">stav: ${statusLabel}</div>
        <div class="vstack gap-2">
          <select class="form-select operation-select" ${isReady ? "" : "disabled"}>
            <option value="grayscale">Grayscale</option>
            <option value="invert">Invert</option>
            <option value="flip">Flip</option>
            <option value="brightness">Brightness</option>
            <option value="crop">Crop</option>
          </select>
          <div class="operation-fields"></div>
          <div class="d-flex gap-2 flex-wrap">
            <button class="btn btn-cloud process-btn" type="button" ${isReady ? "" : "disabled"}><i class="bi bi-magic"></i> Upravit</button>
            <button class="btn btn-outline-secondary download-btn" type="button" ${isReady ? "" : "disabled"}><i class="bi bi-download"></i> Stahnout</button>
            <button class="btn btn-outline-danger delete-btn" type="button"><i class="bi bi-trash"></i> Smazat</button>
          </div>
        </div>
      </div>
    `;

    const img = card.querySelector("img");
    const operationSelect = card.querySelector(".operation-select");
    const fields = card.querySelector(".operation-fields");

    operationSelect.addEventListener("change", () => renderOperationFields(operationSelect.value, fields));
    card.querySelector(".process-btn").addEventListener("click", () => processObject(item.object_key, card));
    card.querySelector(".download-btn").addEventListener("click", () => downloadObject(objectId, item.object_key));
    card.querySelector(".delete-btn").addEventListener("click", () => deleteObject(objectId, item.object_key));

    renderOperationFields(operationSelect.value, fields);
    els.gallery.appendChild(card);
    if (isReady) {
      loadPreview(item.object_key, img);
    } else {
      img.alt = "Upload probiha";
    }
  }
}

function renderOperationFields(operation, container) {
  if (operation === "brightness") {
    container.innerHTML = `<input class="form-control" name="value" type="number" value="35" aria-label="Brightness value">`;
    return;
  }
  if (operation === "crop") {
    container.innerHTML = `
      <input class="form-control" name="x_start" type="number" min="0" value="0" aria-label="Crop x">
      <input class="form-control" name="y_start" type="number" min="0" value="0" aria-label="Crop y">
      <input class="form-control" name="width" type="number" min="1" value="200" aria-label="Crop width">
      <input class="form-control" name="height" type="number" min="1" value="200" aria-label="Crop height">
    `;
    return;
  }
  container.innerHTML = "";
}

function collectParams(card) {
  const operation = card.querySelector(".operation-select").value;
  if (operation === "brightness") {
    return { value: Number(card.querySelector('[name="value"]').value || 0) };
  }
  if (operation === "crop") {
    return {
      x_start: Number(card.querySelector('[name="x_start"]').value || 0),
      y_start: Number(card.querySelector('[name="y_start"]').value || 0),
      width: Number(card.querySelector('[name="width"]').value || 1),
      height: Number(card.querySelector('[name="height"]').value || 1),
    };
  }
  return {};
}

async function loadPreview(objectKey, img) {
  try {
    const response = await fetch(`/me/objects/${encodeURIComponent(objectKey)}/preview?t=${Date.now()}`, {
      headers: authHeaders(),
      cache: "no-store",
    });
    if (!response.ok) {
      throw new Error("Preview failed");
    }
    const blob = await response.blob();
    const url = URL.createObjectURL(blob);
    state.imageUrls.set(objectKey, url);
    img.src = url;
  } catch {
    img.alt = "Nahled neni dostupny";
  }
}

async function processObject(objectKey, card) {
  const operation = card.querySelector(".operation-select").value;
  try {
    await requestJson(`/me/objects/${encodeURIComponent(objectKey)}/process`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ operation, params: collectParams(card) }),
    });
    showAlert(els.appAlert, `${objectKey}: zpracovava se`, "info");
  } catch (error) {
    showAlert(els.appAlert, error.message, "danger");
  }
}

async function downloadObject(objectId, objectKey) {
  try {
    const response = await fetch(`/download/${encodeURIComponent(objectId)}?download=${Date.now()}`, {
      headers: authHeaders(),
      cache: "no-store",
    });
    if (!response.ok) {
      const contentType = response.headers.get("content-type") || "";
      const payload = contentType.includes("application/json") ? await response.json() : await response.text();
      const detail = typeof payload === "object" ? payload.detail : payload;
      throw new Error(detail || "Download failed");
    }
    const blob = await response.blob();
    const url = URL.createObjectURL(blob);
    const link = document.createElement("a");
    link.href = url;
    link.download = objectKey;
    link.click();
    URL.revokeObjectURL(url);
  } catch (error) {
    showAlert(els.appAlert, error.message, "danger");
  }
}

async function deleteObject(objectId, objectKey) {
  try {
    await requestJson(`/download/${encodeURIComponent(objectId)}`, { method: "DELETE" });
    showAlert(els.appAlert, `${objectKey}: smazano`, "success");
    await Promise.all([loadObjects(), loadBilling()]);
  } catch (error) {
    showAlert(els.appAlert, error.message, "danger");
  }
}

async function loadBilling() {
  const billing = await requestJson("/me/billing");
  const cards = [
    ["Aktualni uloziste", bytes(billing.current_storage_bytes)],
    ["Ingress", bytes(billing.ingress_bytes)],
    ["Egress", bytes(billing.egress_bytes)],
    ["Interni transfer", bytes(billing.internal_transfer_bytes)],
    ["Write requesty", billing.count_write_requests],
    ["Read requesty", billing.count_read_requests],
  ];
  els.billingGrid.innerHTML = cards.map(([label, value]) => `
    <div class="billing-card">
      <div class="billing-label">${label}</div>
      <div class="billing-value">${value}</div>
    </div>
  `).join("");
}

function connectStatusSocket() {
  closeStatusSocket();
  const protocol = window.location.protocol === "https:" ? "wss" : "ws";
  state.socket = new WebSocket(`${protocol}://${window.location.host}/ws/broker/image.done?mode=json&role=subscriber&durable=false`);
  state.socket.addEventListener("message", async (event) => {
    const envelope = JSON.parse(event.data);
    const payload = envelope.payload || {};
    if (!state.user || payload.bucket_id !== state.user.bucket_id) {
      return;
    }
    if (payload.status === "completed") {
      if (payload.object_key) {
        state.trackedPendingKeys.add(payload.object_key);
      }
      state.pendingRefreshAttempts = 0;
      showAlert(els.appAlert, `${payload.object_key}: zpracovano, cekam na ulozeni`, "info");
      await Promise.all([loadObjects(), loadBilling()]);
    }
    if (payload.status === "failed") {
      showAlert(els.appAlert, `${payload.object_key || "obrazek"}: ${payload.error || "chyba zpracovani"}`, "danger");
    }
  });
}

function closeStatusSocket() {
  if (state.socket) {
    state.socket.close();
    state.socket = null;
  }
}

els.loginForm.addEventListener("submit", async (event) => {
  event.preventDefault();
  hideAlert(els.authAlert);
  const form = new FormData(els.loginForm);
  try {
    setSession(await publicJson("/auth/login", Object.fromEntries(form)));
    await bootApp();
  } catch (error) {
    showAlert(els.authAlert, error.message, "danger");
  }
});

els.registerForm.addEventListener("submit", async (event) => {
  event.preventDefault();
  hideAlert(els.authAlert);
  const form = new FormData(els.registerForm);
  try {
    setSession(await publicJson("/auth/register", Object.fromEntries(form)));
    await bootApp();
  } catch (error) {
    showAlert(els.authAlert, error.message, "danger");
  }
});

els.uploadForm.addEventListener("submit", async (event) => {
  event.preventDefault();
  const file = document.getElementById("pictureFile").files[0];
  const enteredKey = document.getElementById("objectKey").value;
  const objectKey = sanitizeObjectKey(enteredKey || file.name);
  const body = new FormData();
  body.append("file", file, objectKey);

  try {
    const accepted = await fetch(`/me/objects/${encodeURIComponent(objectKey)}`, {
      method: "PUT",
      headers: authHeaders(),
      body,
    }).then(async (response) => {
      const payload = await response.json();
      if (!response.ok) {
        throw new Error(payload.detail || "Upload failed");
      }
      return payload;
    });
    if (accepted?.object_key) {
      state.trackedPendingKeys.add(accepted.object_key);
    }
    state.pendingRefreshAttempts = 0;
    els.uploadForm.reset();
    showAlert(els.appAlert, `${objectKey}: prijato, cekam na ulozeni do Haystack`, "info");
    await Promise.all([loadObjects(), loadBilling()]);
  } catch (error) {
    showAlert(els.appAlert, error.message, "danger");
  }
});

els.refreshBtn.addEventListener("click", async () => {
  await Promise.all([loadObjects(), loadBilling()]);
});

els.logoutBtn.addEventListener("click", () => {
  clearSession();
  showAuth();
});

bootApp();
