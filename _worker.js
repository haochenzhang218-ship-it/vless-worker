const userID = '43ecf616-3672-4d91-af8d-a3035143b293';
const proxyIPs = ['cdn.cloudflare.net'];
let proxyIP = proxyIPs[0];

export default {
  async fetch(request, env, ctx) {
    const upgradeHeader = request.headers.get('Upgrade');
    if (!upgradeHeader || upgradeHeader !== 'websocket') {
      const url = new URL(request.url);
      if (url.pathname === `/${userID}`) {
        const host = request.headers.get('Host');
        const config = `vless://${userID}@${host}:443?encryption=none&security=tls&sni=${host}&fp=randomized&type=ws&host=${host}&path=%2F#${host}`;
        return new Response(config, { status: 200 });
      }
      return new Response('Node is running', { status: 200 });
    }
    return await vlessOverWSHandler(request);
  }
};

async function vlessOverWSHandler(request) {
  const [client, server] = Object.values(new WebSocketPair());
  server.accept();
  const earlyDataHeader = request.headers.get('sec-websocket-protocol') || '';
  const readableStream = makeReadableWebSocketStream(server, earlyDataHeader);
  let remoteSocket = { value: null };
  let udpWrite = null;
  let isDns = false;

  readableStream.pipeTo(new WritableStream({
    async write(chunk) {
      if (isDns && udpWrite) return udpWrite(chunk);
      if (remoteSocket.value) {
        const w = remoteSocket.value.writable.getWriter();
        await w.write(chunk); w.releaseLock(); return;
      }
      const { hasError, portRemote = 443, addressRemote = '', rawDataIndex, vlessVersion = new Uint8Array([0,0]), isUDP } = processVlessHeader(chunk, userID);
      if (hasError) return;
      if (isUDP && portRemote === 53) isDns = true;
      else if (isUDP) return;
      const vlessHeader = new Uint8Array([vlessVersion[0], 0]);
      const rawData = chunk.slice(rawDataIndex);
      if (isDns) {
        const { write } = await handleUDP(server, vlessHeader);
        udpWrite = write; udpWrite(rawData); return;
      }
      handleTCP(remoteSocket, addressRemote, portRemote, rawData, server, vlessHeader);
    }
  })).catch(() => {});

  return new Response(null, { status: 101, webSocket: client });
}

async function handleTCP(remoteSocket, address, port, rawData, ws, vlessHeader) {
  async function connect(addr, p) {
    const { connect } = await import('cloudflare:sockets');
    const sock = connect({ hostname: addr, port: p });
    remoteSocket.value = sock;
    const w = sock.writable.getWriter();
    await w.write(rawData); w.releaseLock();
    return sock;
  }
  try {
    const sock = await connect(address, port);
    remoteToWS(sock, ws, vlessHeader, async () => {
      const s2 = await connect(proxyIP, port);
      s2.closed.catch(()=>{}).finally(()=>safeClose(ws));
      remoteToWS(s2, ws, vlessHeader, null);
    });
  } catch(e) {}
}

function makeReadableWebSocketStream(ws, earlyData) {
  let cancelled = false;
  return new ReadableStream({
    start(ctrl) {
      ws.addEventListener('message', e => { if (!cancelled) ctrl.enqueue(e.data); });
      ws.addEventListener('close', () => { safeClose(ws); if (!cancelled) ctrl.close(); });
      ws.addEventListener('error', e => ctrl.error(e));
      if (earlyData) {
        const { earlyData: ed, error } = base64ToBuffer(earlyData);
        if (error) ctrl.error(error); else if (ed) ctrl.enqueue(ed);
      }
    },
    cancel() { cancelled = true; safeClose(ws); }
  });
}

function processVlessHeader(buf, uid) {
  if (buf.byteLength < 24) return { hasError: true };
  const ver = new Uint8Array(buf.slice(0, 1));
  if (stringify(new Uint8Array(buf.slice(1, 17))) !== uid) return { hasError: true };
  const optLen = new Uint8Array(buf.slice(17, 18))[0];
  const cmd = new Uint8Array(buf.slice(18 + optLen, 19 + optLen))[0];
  let isUDP = false;
  if (cmd === 2) isUDP = true;
  else if (cmd !== 1) return { hasError: true };
  const port = new DataView(buf.slice(19 + optLen, 21 + optLen)).getUint16(0);
  let addrIdx = 21 + optLen;
  const addrType = new Uint8Array(buf.slice(addrIdx, addrIdx + 1))[0];
  let addr = '', addrLen = 0, addrValIdx = addrIdx + 1;
  if (addrType === 1) { addrLen = 4; addr = new Uint8Array(buf.slice(addrValIdx, addrValIdx + 4)).join('.'); }
  else if (addrType === 2) { addrLen = new Uint8Array(buf.slice(addrValIdx, addrValIdx + 1))[0]; addrValIdx++; addr = new TextDecoder().decode(buf.slice(addrValIdx, addrValIdx + addrLen)); }
  else if (addrType === 3) { addrLen = 16; const dv = new DataView(buf.slice(addrValIdx, addrValIdx + 16)); addr = Array.from({length:8}, (_,i) => dv.getUint16(i*2).toString(16)).join(':'); }
  else return { hasError: true };
  return { hasError: false, addressRemote: addr, portRemote: port, rawDataIndex: addrValIdx + addrLen, vlessVersion: ver, isUDP };
}

async function remoteToWS(remote, ws, vlessHeader, retry) {
  let hasData = false;
  await remote.readable.pipeTo(new WritableStream({
    async write(chunk) {
      hasData = true;
      if (ws.readyState !== WebSocket.OPEN) return;
      if (vlessHeader) { ws.send(await new Blob([vlessHeader, chunk]).arrayBuffer()); vlessHeader = null; }
      else ws.send(chunk);
    }
  })).catch(() => {});
  if (!hasData && retry) retry();
}

async function handleUDP(ws, vlessHeader) {
  let sent = false;
  const { readable, writable } = new TransformStream({
    transform(chunk, ctrl) {
      for (let i = 0; i < chunk.byteLength;) {
        const len = new DataView(chunk.slice(i, i+2)).getUint16(0);
        ctrl.enqueue(new Uint8Array(chunk.slice(i+2, i+2+len))); i += 2+len;
      }
    }
  });
  readable.pipeTo(new WritableStream({
    async write(chunk) {
      const res = await fetch('https://1.1.1.1/dns-query', { method:'POST', headers:{'content-type':'application/dns-message'}, body:chunk });
      const data = await res.arrayBuffer();
      const sz = new Uint8Array([(data.byteLength>>8)&0xff, data.byteLength&0xff]);
      if (ws.readyState === WebSocket.OPEN) {
        if (sent) ws.send(await new Blob([sz, data]).arrayBuffer());
        else { ws.send(await new Blob([vlessHeader, sz, data]).arrayBuffer()); sent = true; }
      }
    }
  })).catch(()=>{});
  const writer = writable.getWriter();
  return { write: chunk => writer.write(chunk) };
}

function base64ToBuffer(str) {
  try {
    const b = atob(str.replace(/-/g,'+').replace(/_/g,'/'));
    return { earlyData: Uint8Array.from(b, c => c.charCodeAt(0)).buffer };
  } catch(e) { return { error: e }; }
}

function safeClose(ws) {
  try { if (ws.readyState === WebSocket.OPEN || ws.readyState === WebSocket.CLOSING) ws.close(); } catch(e) {}
}

const hex = Array.from({length:256}, (_,i) => (i+256).toString(16).slice(1));
function stringify(a, o=0) {
  return [0,1,2,3,'-',4,5,'-',6,7,'-',8,9,'-',10,11,12,13,14,15].map(i => typeof i==='string' ? i : hex[a[o+i]]).join('');
}
