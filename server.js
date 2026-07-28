var express = require('express');
var http = require('http');
var path = require('path');
var cors = require('cors');
var socketio = require('socket.io');

var app = express();
app.use(cors());
app.use(express.static(path.join(__dirname, 'public')));

var server = http.createServer(app);
var io = new socketio.Server(server, {
  cors: { origin: '*', methods: ['GET', 'POST'] },
  pingTimeout: 25000,
  pingInterval: 10000
});

// ========== ÉTAT ==========
var users = {};
var channels = {};
var channelPins = {};
var whisperRooms = {};
var MAX_PTT = 60000;
var emergencyActive = false;
var emergencyBy = null;

// ========== FONCTIONS ==========
function getChannelState(chId) {
  var ch = channels[chId];
  if (!ch) return { channelId: chId, users: [], talkingUser: null, talkingCallsign: null, locked: false };
  var list = [];
  for (var i = 0; i < ch.users.length; i++) {
    var uid = ch.users[i];
    if (users[uid]) {
      list.push({
        id: uid,
        callsign: users[uid].callsign,
        isTalking: users[uid].isTalking,
        status: users[uid].status || '',
        location: users[uid].location || null
      });
    }
  }
  return {
    channelId: chId,
    users: list,
    talkingUser: ch.talkingUser,
    talkingCallsign: ch.talkingUser && users[ch.talkingUser] ? users[ch.talkingUser].callsign : null,
    locked: !!channelPins[chId]
  };
}

function broadcastChannel(chId) {
  io.to('ch:' + chId).emit('channel:users', getChannelState(chId));
}

function leaveChannel(socket) {
  var user = users[socket.id];
  if (!user || user.channel === null) return;
  var chId = user.channel;
  var ch = channels[chId];
  if (ch) {
    var idx = ch.users.indexOf(socket.id);
    if (idx > -1) ch.users.splice(idx, 1);
    if (ch.talkingUser === socket.id) ch.talkingUser = null;
    socket.to('ch:' + chId).emit('peer:left', { id: socket.id });
  }
  socket.leave('ch:' + chId);
  user.channel = null;
  broadcastChannel(chId);
}

function getAllChannelsStatus() {
  var result = [];
  for (var i = 1; i <= 16; i++) {
    var ch = channels[i];
    result.push({
      id: i,
      userCount: ch ? ch.users.length : 0,
      talkingUser: ch ? ch.talkingUser : null,
      talkingCallsign: ch && ch.talkingUser && users[ch.talkingUser] ? users[ch.talkingUser].callsign : null,
      locked: !!channelPins[i]
    });
  }
  return result;
}

function getWhisperRoomId(id1, id2) {
  return [id1, id2].sort().join('_');
}

// ========== SOCKET ==========
io.on('connection', function(socket) {
  console.log('[+] ' + socket.id);

  // Identification
  socket.on('user:join', function(data) {
    var callsign = String(data.callsign || 'UNIT').slice(0, 16).toUpperCase();
    var status = String(data.status || '').slice(0, 30);
    users[socket.id] = {
      callsign: callsign,
      status: status,
      channel: null,
      isTalking: false,
      isWhispering: false,
      pttTimeout: null,
      location: null
    };
    socket.emit('user:joined', { id: socket.id, callsign: callsign });
    if (emergencyActive) {
      socket.emit('emergency:alert', { userId: null, callsign: emergencyBy || '???', timestamp: new Date().toISOString() });
    }
    console.log('[i] ' + callsign);
  });

  // Statut personnalisé
  socket.on('user:status', function(data) {
    var user = users[socket.id];
    if (!user) return;
    user.status = String(data.status || '').slice(0, 30);
    if (user.channel !== null) broadcastChannel(user.channel);
  });

  // Localisation
  socket.on('user:location', function(data) {
    var user = users[socket.id];
    if (!user) return;
    user.location = { lat: data.lat, lng: data.lng, accuracy: data.accuracy };
    if (user.channel !== null) {
      io.to('ch:' + user.channel).emit('user:location', {
        id: socket.id,
        callsign: user.callsign,
        lat: data.lat,
        lng: data.lng
      });
    }
  });

  // Rejoindre canal
  socket.on('channel:join', function(data) {
    var user = users[socket.id];
    if (!user) return socket.emit('error:msg', { msg: 'Non identifie' });

    var id = parseInt(data.channelId, 10);
    if (isNaN(id) || id < 1 || id > 16) id = 1;

    // Vérifier PIN
    if (channelPins[id]) {
      if (data.pin !== channelPins[id]) {
        return socket.emit('channel:pin_required', { channelId: id });
      }
    }

    leaveChannel(socket);
    if (!channels[id]) channels[id] = { users: [], talkingUser: null };
    channels[id].users.push(socket.id);
    user.channel = id;
    socket.join('ch:' + id);

    var peerList = [];
    for (var i = 0; i < channels[id].users.length; i++) {
      var pid = channels[id].users[i];
      if (pid !== socket.id && users[pid]) {
        peerList.push({ id: pid, callsign: users[pid].callsign, status: users[pid].status || '' });
      }
    }

    socket.emit('channel:joined', { channelId: id, peers: peerList });
    socket.to('ch:' + id).emit('peer:joined', { id: socket.id, callsign: user.callsign, status: user.status });
    broadcastChannel(id);
    io.emit('channels:status', getAllChannelsStatus());
  });

  // Verrouiller canal avec PIN
  socket.on('channel:set_pin', function(data) {
    var user = users[socket.id];
    if (!user || user.channel === null) return;
    var pin = String(data.pin || '').replace(/\D/g, '').slice(0, 4);
    if (pin.length === 4) {
      channelPins[user.channel] = pin;
      io.to('ch:' + user.channel).emit('channel:locked', { channelId: user.channel, by: user.callsign });
    } else if (pin === '') {
      delete channelPins[user.channel];
      io.to('ch:' + user.channel).emit('channel:unlocked', { channelId: user.channel, by: user.callsign });
    }
    io.emit('channels:status', getAllChannelsStatus());
  });

  // Scan
  socket.on('channels:scan', function() {
    socket.emit('channels:status', getAllChannelsStatus());
  });

  // ===== PTT =====
  socket.on('ptt:start', function() {
    var user = users[socket.id];
    if (!user || user.channel === null) return;
    var ch = channels[user.channel];
    if (!ch) return;
    if (ch.talkingUser && ch.talkingUser !== socket.id) {
      var blocker = users[ch.talkingUser];
      return socket.emit('ptt:denied', { reason: 'CANAL OCCUPE', by: blocker ? blocker.callsign : '???' });
    }
    ch.talkingUser = socket.id;
    user.isTalking = true;
    socket.emit('ptt:granted');
    io.to('ch:' + user.channel).emit('ptt:active', { userId: socket.id, callsign: user.callsign });
    broadcastChannel(user.channel);
    if (user.pttTimeout) clearTimeout(user.pttTimeout);
    user.pttTimeout = setTimeout(function() {
      if (ch.talkingUser === socket.id) {
        ch.talkingUser = null; user.isTalking = false;
        socket.emit('ptt:timeout');
        io.to('ch:' + user.channel).emit('ptt:released', { userId: socket.id, callsign: user.callsign });
        broadcastChannel(user.channel);
      }
    }, MAX_PTT);
  });

  socket.on('ptt:stop', function() {
    var user = users[socket.id];
    if (!user || user.channel === null) return;
    var ch = channels[user.channel];
    if (user.pttTimeout) clearTimeout(user.pttTimeout);
    if (ch && ch.talkingUser === socket.id) ch.talkingUser = null;
    user.isTalking = false;
    io.to('ch:' + user.channel).emit('ptt:released', { userId: socket.id, callsign: user.callsign });
    broadcastChannel(user.channel);
  });

  // ===== WHISPER (appel privé) =====
  socket.on('whisper:start', function(data) {
    var user = users[socket.id];
    var target = users[data.targetId];
    if (!user || !target) return;
    var roomId = getWhisperRoomId(socket.id, data.targetId);
    socket.join('whisper:' + roomId);
    user.isWhispering = true;
    user.whisperRoom = roomId;
    user.whisperTarget = data.targetId;
    io.to(data.targetId).emit('whisper:incoming', {
      fromId: socket.id,
      callsign: user.callsign,
      roomId: roomId
    });
    socket.emit('whisper:started', { targetId: data.targetId, callsign: target.callsign, roomId: roomId });
  });

  socket.on('whisper:accept', function(data) {
    var user = users[socket.id];
    if (!user) return;
    socket.join('whisper:' + data.roomId);
    user.isWhispering = true;
    user.whisperRoom = data.roomId;
    io.to('whisper:' + data.roomId).emit('whisper:connected', { roomId: data.roomId });
  });

  socket.on('whisper:stop', function() {
    var user = users[socket.id];
    if (!user || !user.whisperRoom) return;
    io.to('whisper:' + user.whisperRoom).emit('whisper:ended', { callsign: user.callsign });
    socket.leave('whisper:' + user.whisperRoom);
    user.isWhispering = false;
    user.whisperRoom = null;
    user.whisperTarget = null;
  });

  socket.on('whisper:ptt:start', function() {
    var user = users[socket.id];
    if (!user || !user.whisperRoom) return;
    socket.to('whisper:' + user.whisperRoom).emit('whisper:ptt:active', {
      fromId: socket.id, callsign: user.callsign
    });
  });

  socket.on('whisper:ptt:stop', function() {
    var user = users[socket.id];
    if (!user || !user.whisperRoom) return;
    socket.to('whisper:' + user.whisperRoom).emit('whisper:ptt:released', { fromId: socket.id });
  });

  // ===== WEBRTC CANAL =====
  socket.on('webrtc:offer', function(data) {
    io.to(data.targetId).emit('webrtc:offer', { fromId: socket.id, offer: data.offer });
  });
  socket.on('webrtc:answer', function(data) {
    io.to(data.targetId).emit('webrtc:answer', { fromId: socket.id, answer: data.answer });
  });
  socket.on('webrtc:ice', function(data) {
    io.to(data.targetId).emit('webrtc:ice', { fromId: socket.id, candidate: data.candidate });
  });

  // ===== WEBRTC WHISPER =====
  socket.on('whisper:offer', function(data) {
    io.to(data.targetId).emit('whisper:offer', { fromId: socket.id, offer: data.offer });
  });
  socket.on('whisper:answer', function(data) {
    io.to(data.targetId).emit('whisper:answer', { fromId: socket.id, answer: data.answer });
  });
  socket.on('whisper:ice', function(data) {
    io.to(data.targetId).emit('whisper:ice', { fromId: socket.id, candidate: data.candidate });
  });

  // ===== SOUNDBOARD =====
  socket.on('soundboard:play', function(data) {
    var user = users[socket.id];
    if (!user || user.channel === null) return;
    io.to('ch:' + user.channel).emit('soundboard:play', {
      sound: data.sound,
      callsign: user.callsign
    });
  });

  // ===== REPLAY =====
  socket.on('replay:store', function(data) {
    var user = users[socket.id];
    if (!user || user.channel === null) return;
    socket.to('ch:' + user.channel).emit('replay:available', {
      fromId: socket.id,
      callsign: user.callsign,
      duration: data.duration
    });
  });

  // ===== URGENCE =====
  socket.on('emergency:activate', function() {
    var user = users[socket.id];
    if (!user) return;
    emergencyActive = true;
    emergencyBy = user.callsign;
    io.emit('emergency:alert', { userId: socket.id, callsign: user.callsign, timestamp: new Date().toISOString() });
  });
  socket.on('emergency:cancel', function() {
    var user = users[socket.id];
    emergencyActive = false; emergencyBy = null;
    io.emit('emergency:cancelled', { callsign: user ? user.callsign : '???' });
  });

  // ===== TEXTE =====
  socket.on('text:send', function(data) {
    var user = users[socket.id];
    if (!user || user.channel === null) return;
    io.to('ch:' + user.channel).emit('text:received', {
      callsign: user.callsign,
      message: String(data.message).slice(0, 200),
      timestamp: new Date().toISOString()
    });
  });

  // ===== TRANSCRIPTION =====
  socket.on('transcript:send', function(data) {
    var user = users[socket.id];
    if (!user || user.channel === null) return;
    io.to('ch:' + user.channel).emit('transcript:received', {
      callsign: user.callsign,
      text: String(data.text || '').slice(0, 300),
      timestamp: new Date().toISOString()
    });
  });

  // ===== DÉCONNEXION =====
  socket.on('disconnect', function() {
    var user = users[socket.id];
    if (user) {
      if (user.pttTimeout) clearTimeout(user.pttTimeout);
      if (user.whisperRoom) {
        io.to('whisper:' + user.whisperRoom).emit('whisper:ended', { callsign: user.callsign });
      }
      leaveChannel(socket);
      console.log('[-] ' + user.callsign);
    }
    delete users[socket.id];
    io.emit('channels:status', getAllChannelsStatus());
  });
});

// ========== ROUTES ==========
app.get('/health', function(req, res) {
  res.json({
    status: 'OK', uptime: Math.round(process.uptime()),
    users: Object.keys(users).length,
    channels: Object.keys(channels).length,
    emergency: emergencyActive
  });
});

app.get('/', function(req, res) {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

var PORT = process.env.PORT || 3000;
server.listen(PORT, function() {
  console.log('=================================');
  console.log('  RADIO PTT SERVER v3.0');
  console.log('  Port: ' + PORT);
  console.log('=================================');
});
