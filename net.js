// 공통 멀티플레이 모듈 (PeerJS 기반, 방장 중심 스타형 연결)
// - 방장(host)이 방 코드를 만들고 게임 진행을 맡습니다. 참가자(client)는 방장과만 연결됩니다.
// - 사내 PeerServer가 있으면 DVNet.PEER_CONFIG 를 바꾸세요. 예: { host: 'peer.mycompany.com', port: 443, path: '/', secure: true }
(function () {
  const PEER_CONFIG = {};
  const HEARTBEAT_MS = 3000, HEARTBEAT_TIMEOUT = 10000;
  const CODE_CHARS ='ABCDEFGHJKMNPQRSTUVWXYZ23456789';
  const newCode = () => [...crypto.getRandomValues(new Uint8Array(6))].map(b => CODE_CHARS[b % CODE_CHARS.length]).join('');

  function emitter() {
    const h = {};
    return {
      on(ev, fn) { (h[ev] = h[ev] || []).push(fn); return this; },
      emit(ev, ...a) { (h[ev] || []).forEach(fn => { try { fn(...a); } catch (e) { console.error(e); } }); },
    };
  }

  function ensurePeer() {
    if (typeof Peer === 'undefined') throw new Error('PeerJS를 불러오지 못했습니다 (vendor/peerjs.min.js 확인)');
  }
  function errText(e) {
    const t = e && e.type;
    if (t === 'peer-unavailable') return '방을 찾을 수 없습니다. 코드를 확인하세요';
    if (t === 'network' || t === 'server-error' || t === 'socket-error') return '연결 서버에 접속할 수 없습니다 (사내 방화벽 확인)';
    if (t === 'browser-incompatible') return '이 브라우저는 WebRTC를 지원하지 않습니다';
    return '연결 오류: ' + (t || e);
  }

  // 방 만들기 → Promise<room>
  function createRoom(gameId, opts = {}) {
    ensurePeer();
    const max = opts.maxPlayers || 8;
    return new Promise((resolve, reject) => {
      const code = newCode();
      const peer = new Peer(`dvplan-${gameId}-${code}`, PEER_CONFIG);
      const room = Object.assign(emitter(), { code, members: new Map(), locked: false });
      room.send = (id, msg) => { const m = room.members.get(id); if (m && m.conn.open) try { m.conn.send(msg); } catch (e) {} };
      room.broadcast = msg => room.members.forEach((m, id) => room.send(id, msg));
      room.kick = id => { const m = room.members.get(id); if (m) { try { m.conn.close(); } catch (e) {} } };
      room.close = () => { try { peer.destroy(); } catch (e) {} };
      let opened = false;
      peer.on('open', () => { opened = true; resolve(room); });
      peer.on('error', e => {
        if (!opened && e.type === 'unavailable-id') { peer.destroy(); createRoom(gameId, opts).then(resolve, reject); return; }
        if (!opened) reject(new Error(errText(e))); else room.emit('error', errText(e));
      });
      // 생존 신호: 탭을 그냥 닫으면 WebRTC close 이벤트가 늦거나 오지 않으므로 직접 감지
      const lastSeen = new Map();
      const drop = id => { const m = room.members.get(id); if (!m) return; room.members.delete(id); lastSeen.delete(id); try { m.conn.close(); } catch (e) {} room.emit('leave', id); };
      const hb = setInterval(() => {
        const now = Date.now();
        room.members.forEach((m, id) => {
          if (now - (lastSeen.get(id) || now) > HEARTBEAT_TIMEOUT) drop(id);
          else room.send(id, { type: '__ping' });
        });
      }, HEARTBEAT_MS);
      const origClose = room.close;
      room.close = () => { clearInterval(hb); origClose(); };
      peer.on('connection', conn => {
        conn.on('data', msg => {
          if (!msg || typeof msg.type !== 'string') return;
          lastSeen.set(conn.peer, Date.now());
          if (msg.type === '__pong') return;
          if (msg.type === 'hello') {
            if (room.locked || room.members.size + 1 >= max) { try { conn.send({ type: 'reject', reason: room.locked ? '이미 게임이 시작되었습니다' : '방이 가득 찼습니다' }); } catch (e) {} setTimeout(() => conn.close(), 300); return; }
            const name = String(msg.name || '플레이어').slice(0, 12);
            room.members.set(conn.peer, { conn, name });
            try { conn.send({ type: 'welcome', id: conn.peer }); } catch (e) {}
            room.emit('join', conn.peer, name);
            return;
          }
          if (room.members.has(conn.peer)) room.emit('msg', conn.peer, msg);
        });
        conn.on('close', () => drop(conn.peer));
      });
    });
  }

  // 방 참가 → Promise<client>
  function joinRoom(gameId, code, name) {
    ensurePeer();
    code = String(code || '').trim().toUpperCase();
    return new Promise((resolve, reject) => {
      if (code.length !== 6) { reject(new Error('6자리 방 코드를 입력하세요')); return; }
      const peer = new Peer(PEER_CONFIG);
      const client = emitter();
      let done = false, closedByMe = false;
      const fail = msg => { if (!done) { done = true; reject(new Error(msg)); try { peer.destroy(); } catch (e) {} } };
      const timer = setTimeout(() => fail('연결 시간이 초과되었습니다 (사내 방화벽에서 WebRTC가 막혀 있을 수 있습니다)'), 20000);
      peer.on('error', e => { if (!done) fail(errText(e)); else client.emit('error', errText(e)); });
      peer.on('open', () => {
        const conn = peer.connect(`dvplan-${gameId}-${code}`, { reliable: true });
        client.send = msg => { if (conn.open) try { conn.send(msg); } catch (e) {} };
        let lastPing = Date.now(), lost = false;
        const hb = setInterval(() => {
          if (done && !lost && Date.now() - lastPing > HEARTBEAT_TIMEOUT) { lost = true; clearInterval(hb); try { conn.close(); } catch (e) {} if (!closedByMe) client.emit('close'); }
        }, HEARTBEAT_MS);
        client.close = () => { closedByMe = true; clearInterval(hb); try { conn.close(); } catch (e) {} try { peer.destroy(); } catch (e) {} };
        conn.on('open', () => client.send({ type: 'hello', name }));
        conn.on('data', msg => {
          if (!msg || typeof msg.type !== 'string') return;
          lastPing = Date.now();
          if (msg.type === '__ping') { client.send({ type: '__pong' }); return; }
          if (msg.type === 'reject') { clearTimeout(timer); fail(msg.reason); return; }
          if (msg.type === 'welcome') { clearTimeout(timer); done = true; client.id = msg.id; resolve(client); return; }
          client.emit('msg', msg);
        });
        conn.on('close', () => { clearInterval(hb); if (!done) fail('방장이 연결을 끊었습니다'); else if (!closedByMe && !lost) { lost = true; client.emit('close'); } });
      });
    });
  }

  async function sha256(text) {
    if (!(window.crypto && crypto.subtle)) return null;
    const buf = await crypto.subtle.digest('SHA-256', new TextEncoder().encode(text));
    return [...new Uint8Array(buf)].map(b => b.toString(16).padStart(2, '0')).join('');
  }
  const randHex = n => [...crypto.getRandomValues(new Uint8Array(n))].map(b => b.toString(16).padStart(2, '0')).join('');

  function nick() { try { return localStorage.getItem('dvplan-nick') || ''; } catch (e) { return ''; } }
  function setNick(v) { try { localStorage.setItem('dvplan-nick', v); } catch (e) {} }
  function joinLink(code) { return location.href.split('?')[0].split('#')[0] + '?join=' + code; }

  window.DVNet = { PEER_CONFIG, createRoom, joinRoom, sha256, randHex, nick, setNick, joinLink };
})();
