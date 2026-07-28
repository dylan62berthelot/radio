const express = require('express');
const http = require('http');
const path = require('path');
const cors = require('cors');
const { Server } = require('socket.io');

const app = express();
app.use(cors());
app.use(express.static(path.join(__dirname, 'public')));

const server = http.createServer(app);
const io = new Server(server, {
  cors: { origin: '*', methods: ['GET', 'POST'] },
  pingTimeout: 20000,
  pingInterval: 10000
});

// ---------- ÉTAT ----------
const users = {};     // socketId -> { callsign, channel, isTalking }
const channels = {};  // channelId -> { users: [socketId], talkingUser }

const MAX_PTT_MS = 60000;

function channelState(channelId) {
  const ch = channels[channelId];
  if (!ch) return { users: [], talkingUser: null };
  return {
    channelId,
    talkingUser: ch.talkingUser,
    talkingCallsign: ch.talkingUser ? users[ch.talkingUser]?.callsign : null,
    users: ch.users
      .filter(id => users[id])
      .map(id => ({
        id,
        callsign: users[id].callsign,
        isTalking: users[id].isTalking
      }))
  };
}

function broadcastChannel(channelId) {
  io.to(`ch:${channelId}`).emit('channel:users', channelState(channelId));
}

function leaveCurrentChannel(socket) {
  const user = users[socket.id];
  if (!user || user.channel == null) return;
  const chId = user.channel;
  const ch = channels[chId];
  if (ch) {
    ch.users = ch.users.filter(id => id !== socket.id);
    if (ch.talkingUser === socket.id) ch.talkingUser = null;
    io.to(`ch:${chId}`).emit('peer:left', { id: socket.id });
  }
  socket.leave(`ch:${chId}`);
  user.channel = null;
  broadcastChannel(chId);
}

// ---------- SOCKETS ----------
io.on('connection', (socket) => {
  console.log('[+] Connexion', socket.id);

  socket.on('user:join', ({ callsign }) => {
    const name = (callsign || 'UNIT').toString().slice(0, 16).toUpperCase();
    users[socket.id] = { callsign: name, channel: null, isTalking: false };
    socket.emit('user:joined', { id: socket.id, callsign: name });
    console.log(`[i] ${name} identifié (${socket.id})`);
  });

  socket.on('channel:join', ({ channelId }) => {
    const user = users[socket.id];
    if (!user) return socket.emit('error:msg', { msg: 'Non identifié' });

    leaveCurrentChannel(socket);

    const id = Number(channelId);
    if (!channels[id]) channels[id] = { users: [], talkingUser: null };

    channels[id].users.push(socket.id);
    user.channel = id;
    socket.join(`ch:${id}`);

    // Liste des pairs déjà présents -> pour créer les connexions WebRTC
    const peers = channels[id].users
      .filter(pid => pid !== socket.id && users[pid])
      .map(pid => ({ id: pid, callsign: users[pid].callsign }));

    socket.emit('channel:joined', { channelId: id, peers });
    socket.to(`ch:${id}`).emit('peer:joined', {
      id: socket.id,
      callsign: user.callsign
    });

    broadcastChannel(id);
    console.log(`[i] ${user.callsign} -> canal ${id}`);
  });

  // ---------- PTT ----------
  socket.on('ptt:start', () => {
    const user = users[socket.id];
    if (!user || user.channel == null) return;
    const ch = channels[user.channel];
    if (!ch) return;

    if (ch.talkingUser && ch.talkingUser !== socket.id) {
      return socket.emit('ptt:denied', {
        reason: 'CANAL OCCUPE',
        by: users[ch.talkingUser]?.callsign || '???'
      });
    }

    ch.talkingUser = socket.id;
    user.isTalking = true;

    socket.emit('ptt:granted');
    io.to(`ch:${user.channel}`).emit('ptt:active', {
      userId: socket.id,
      callsign: user.callsign
    });

    clearTimeout(user.pttTimeout);
    user.pttTimeout = setTimeout(() => {
      if (ch.talkingUser === socket.id) {
        ch.talkingUser = null;
        user.isTalking = false;
        socket.emit('ptt:timeout');
        io.to(`ch:${user.channel}`).emit('ptt:released', {
          userId: socket.id, callsign: user.callsign
        });
        broadcastChannel(user.channel);
      }
    }, MAX_PTT_MS);
  });

  socket.on('ptt:stop', () => {
    const user = users[socket.id];
    if (!user || user.channel == null) return;
    const ch = channels[user.channel];
    clearTimeout(user.pttTimeout);
    if (ch && ch.talkingUser === socket.id) ch.talkingUser = null;
    user.isTalking = false;
    io.to(`ch:${user.channel}`).emit('ptt:released', {
      userId: socket.id, callsign: user.callsign
    });
    broadcastChannel(user.channel);
  });

  // ---------- SIGNALISATION WEBRTC ----------
  socket.on('webrtc:offer', ({ targetId, offer }) => {
    io.to(targetId).emit('webrtc:offer', { fromId: socket.id, offer });
  });
  socket.on('webrtc:answer', ({ targetId, answer }) => {
    io.to(targetId).emit('webrtc:answer', { fromId: socket.id, answer });
  });
  socket.on('webrtc:ice', ({ targetId, candidate }) => {
    io.to(targetId).emit('webrtc:ice', { fromId: socket.id, candidate });
  });

  // ---------- URGENCE ----------
  socket.on('emergency:activate', () => {
    const user = users[socket.id];
    if (!user) return;
    console.log(`[!!!] URGENCE par ${user.callsign}`);
    io.emit('emergency:alert', {
      userId: socket.id,
      callsign: user.callsign,
      timestamp: new Date().toISOString()
    });
  });

  socket.on('emergency:cancel', () => {
    const user = users[socket.id];
    io.emit('emergency:cancelled', { callsign: user?.callsign || '???' });
  });

  // ---------- DÉCONNEXION ----------
  socket.on('disconnect', () => {
    const user = users[socket.id];
    if (user) {
      clearTimeout(user.pttTimeout);
      leaveCurrentChannel(socket);
      console.log(`[-] ${user.callsign} déconnecté`);
    }
    delete users[socket.id];
  });
});

// ---------- ROUTES ----------
app.get('/health', (req, res) => {
  res.json({
    status: 'OK',
    uptime: Math.round(process.uptime()),
    users: Object.keys(users).length,
    channels: Object.keys(channels).length
  });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log(`🎙️  Serveur radio actif sur ${PORT}`));
