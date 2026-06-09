import { connect } from 'cloudflare:sockets';

const userID = '43ecf616-3672-4d91-af8d-a3035143b293';
const proxyIPs = ['cdn.cloudflare.net'];

let proxyIP = proxyIPs[Math.floor(Math.random() * proxyIPs.length)];

export default {
  async fetch(request, env, ctx) {
    const upgradeHeader = request.headers.get('Upgrade');
    if (!upgradeHeader || upgradeHeader !== 'websocket') {
      const url = new URL(request.url);
      if (url.pathname === '/') {
        return new Response('Node is running', { status: 200 });
      }
      if (url.pathname === `/${userID}`) {
        const vlessConfig = getVlessConfig(userID, request.headers.get('Host'));
        return new Response(vlessConfig, {
          status: 200,
          headers: { 'Content-Type': 'text/plain;charset=utf-8' }
        });
      }
      return new Response('Not found', { status: 404 });
    }
    return await vlessOverWSHandler(request);
  }
};

function getVlessConfig(userID, host) {
  return `vless://${userID}@${host}:443?encryption=none&security=tls&sni=${host}&fp=randomized&type=ws&host=${host}&path=%2F#${host}`;
}

async function vlessOverWSHandler(request) {
  const webSocketPair = new WebSocketPair();
  const [client, server] = Object.values(webSocketPair);
  server.accept();

  const earlyDataHeader = request.headers.get('sec-websocket-protocol') || '';
  const readableWebSocketStream = makeReadableWebSocketStream(server, earlyDataHeader);

  let remoteSocketWapper = { value: null };
  let udpStreamWrite = null;
  let isDns = false;

  readableWebSocketStream.pipeTo(new WritableStream({
    async write(chunk, controller) {
      if (isDns && udpStreamWrite) {
        return udpStreamWrite(chunk);
      }
      if (remoteSocketWapper.value) {
        const writer = remoteSocketWapper.value.writable.getWriter();
        await writer.write(chunk);
        writer.releaseLock();
        return;
      }

      const { hasError, message, portRemote = 443, addressRemote = '', rawDataIndex, vlessVersion = new Uint8Array([0, 0]), isUDP } = processVlessHeader(chunk, userID);
      if (hasError) return;

      if (isUDP && portRemote === 53) {
        isDns = true;
      } else if (isUDP) {
        return;
      }

      const vlessResponseHeader = new Uint8Array([vlessVersion[0], 0]);
      const rawClientData = chunk.slice(rawDataIndex);

      if (isDns) {
        const { write } = await handleUDPOutBound(server, vlessResponseHeader);
        udpStreamWrite = write;
        udpStreamWrite(rawClientData);
        return;
      }

      handleTCPOutBound(remoteSocketWapper, addressRemote, portRemote, rawClientData, server, vlessResponseHeader);
    }
  })).catch(() => {});

  return new Response(null, { status: 101, webSocket: client });
}

async function handleTCPOutBound(remoteSocket, addressRemote, portRemote, rawClientData, webSocket, vlessResponseHeader) {
  async function connectAndWrite(address, port) {
    const tcpSocket = connect({ hostname: address, port });
    remoteSocket.value = tcpSocket;
    const writer = tcpSocket.writable.getWriter();
    await writer.write(rawClientData);
    writer.releaseLock();
    return tcpSocket;
  }

  async function retry() {
    const tcpSocket = await connectAndWrite(proxyIP || addressRemote, portRemote);
    tcpSocket.closed.catch(() => {}).finally(() => safeCloseWebSocket(webSocket));
    remoteSocketToWS(tcpSocket, webSocket, vlessResponseHeader, null);
  }

  const tcpSocket = await connectAndWrite(addressRemote, portRemote);
  remoteSocketToWS(tcpSocket, webSocket, vlessResponseHeader, retry);
}

function makeReadableWebSocketStream(webSocketServer, earlyDataHeader) {
  let readableStreamCancel = false;
  const stream = new ReadableStream({
    start(controller) {
      webSocketServer.addEventListener('message', (event) => {
        if (readableStreamCancel) return;
        controller.enqueue(event.data);
      });
      webSocketServer.addEventListener('close', () => {
        safeCloseWebSocket(webSocketServer);
        if (readableStreamCancel) return;
        controller.close();
      });
      webSocketServer.addEventListener('error', (err) => {
        controller.error(err);
      });
      if (earlyDataHeader) {
        const { earlyData, error } = base64ToArrayBuffer(earlyDataHeader);
        if (error) { controller.error(error); } else if (earlyData) { controller.enqueue(earlyData); }
      }
    },
    cancel() { readableStreamCancel = true; safeCloseWebSocket(webSocketServer); }
  });
  return stream;
}

function processVlessHeader(vlessBuffer, userID) {
  if (vlessBuffer.byteLength < 24) return { hasError: true };
  const version = new Uint8Array(vlessBuffer.slice(0, 1));
  let isValidUser = false;
  let isUDP = false;
  const slicedBuffer = new Uint8Array(vlessBuffer.slice(1, 17));
  const slicedBufferString = stringify(slicedBuffer);
  if (stringify(slicedBuffer) === userID) isValidUser = true;
  if (!isValidUser) return { hasError: true, message: 'invalid user' };
  const optLength = new Uint8Array(vlessBuffer.slice(17, 18))[0];
  const command = new Uint8Array(vlessBuffer.slice(18 + optLength, 18 + optLength + 1))[0];
  if (command === 1) {} else if (command === 2) { isUDP = true; } else { return { hasError: true }; }
  const portIndex = 18 + optLength + 1;
  const portBuffer = vlessBuffer.slice(portIndex, portIndex + 2);
  const portRemote = new DataView(portBuffer).getUint16(0);
  let addressIndex = portIndex + 2;
  const addressBuffer = new Uint8Array(vlessBuffer.slice(addressIndex, addressIndex + 1));
  const addressType = addressBuffer[0];
  let addressLength = 0, addressValueIndex = addressIndex + 1, addressValue = '';
  switch (addressType) {
    case 1: addressLength = 4; addressValue = new Uint8Array(vlessBuffer.slice(addressValueIndex, addressValueIndex + addressLength)).join('.'); break;
    case 2: addressLength = new Uint8Array(vlessBuffer.slice(addressValueIndex, addressValueIndex + 1))[0]; addressValueIndex += 1; addressValue = new TextDecoder().decode(vlessBuffer.slice(addressValueIndex, addressValueIndex + addressLength)); break;
    case 3: addressLength = 16; const dataView = new DataView(vlessBuffer.slice(addressValueIndex, addressValueIndex + addressLength)); const ipv6 = []; for (let i = 0; i < 8; i++) { ipv6.push(dataView.getUint16(i * 2).toString(16)); } addressValue = ipv6.join(':'); break;
    default: return { hasError: true };
  }
  return { hasError: false, addressRemote: addressValue, addressType, portRemote, rawDataIndex: addressValueIndex + addressLength, vlessVersion: version, isUDP };
}

async function remoteSocketToWS(remoteSocket, webSocket, vlessResponseHeader, retry) {
  let hasIncomingData = false;
  await remoteSocket.readable.pipeTo(new WritableStream({
    async write(chunk, controller) {
      hasIncomingData = true;
      if (webSocket.readyState !== WebSocket.OPEN) { controller.error('webSocket is not open'); return; }
      if (vlessResponseHeader) { webSocket.send(await new Blob([vlessResponseHeader, chunk]).arrayBuffer()); vlessResponseHeader = null; }
      else { webSocket.send(chunk); }
    },
    close() {},
    abort() {}
  })).catch(() => {});
  if (!hasIncomingData && retry) { retry(); }
}

function base64ToArrayBuffer(base64Str) {
  if (!base64Str) return { error: null };
  try {
    base64Str = base64Str.replace(/-/g, '+').replace(/_/g, '/');
    const decode = atob(base64Str);
    const arryBuffer = Uint8Array.from(decode, (c) => c.charCodeAt(0));
    return { earlyData: arryBuffer.buffer, error: null };
  } catch (error) { return { error }; }
}

function safeCloseWebSocket(socket) {
  try { if (socket.readyState === WebSocket.OPEN || socket.readyState === WebSocket.CLOSING) { socket.close(); } } catch (error) {}
}

const byteToHex = [];
for (let i = 0; i < 256; ++i) { byteToHex.push((i + 256).toString(16).slice(1)); }
function stringify(arr, offset = 0) {
  return (byteToHex[arr[offset + 0]] + byteToHex[arr[offset + 1]] + byteToHex[arr[offset + 2]] + byteToHex[arr[offset + 3]] + '-' + byteToHex[arr[offset + 4]] + byteToHex[arr[offset + 5]] + '-' + byteToHex[arr[offset + 6]] + byteToHex[arr[offset + 7]] + '-' + byteToHex[arr[offset + 8]] + byteToHex[arr[offset + 9]] + '-' + byteToHex[arr[offset + 10]] + byteToHex[arr[offset + 11]] + byteToHex[arr[offset + 12]] + byteToHex[arr[offset + 13]] + byteToHex[arr[offset + 14]] + byteToHex[arr[offset + 15]]).toLowerCase();
}

async function handleUDPOutBound(webSocket, vlessResponseHeader) {
  let isVlessHeaderSent = false;
  const transformStream = new TransformStream({
    transform(chunk, controller) {
      for (let index = 0; index < chunk.byteLength;) {
        const lengthBuffer = chunk.slice(index, index + 2);
        const udpPakcetLength = new DataView(lengthBuffer).getUint16(0);
        const udpData = new Uint8Array(chunk.slice(index + 2, index + 2 + udpPakcetLength));
        index = index + 2 + udpPakcetLength;
        controller.enqueue(udpData);
      }
    }
  });
  transformStream.readable.pipeTo(new WritableStream({
    async write(chunk) {
      const resp = await fetch('https://1.1.1.1/dns-query', { method: 'POST', headers: { 'content-type': 'application/dns-message' }, body: chunk });
      const dnsQueryResult = await resp.arrayBuffer();
      const udpSize = dnsQueryResult.byteLength;
      const udpSizeBuffer = new Uint8Array([(udpSize >> 8) & 0xff, udpSize & 0xff]);
      if (webSocket.readyState === WebSocket.OPEN) {
        if (isVlessHeaderSent) { webSocket.send(await new Blob([udpSizeBuffer, dnsQueryResult]).arrayBuffer()); }
        else { webSocket.send(await new Blob([vlessResponseHeader, udpSizeBuffer, dnsQueryResult]).arrayBuffer()); isVlessHeaderSent = true; }
      }
    }
  })).catch(() => {});
  const writer = transformStream.writable.getWriter();
  return { write: (chunk) => writer.write(chunk) };
}
