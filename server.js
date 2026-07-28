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

// ========== ÉTAT GLOBAL ==========
var users = {};
var channels = {};
var MAX_PTT = 60000;
var emergencyActive = false;
var emergencyBy = null;

// ========== FONCTIONS ==========
function getChannelState(chId) {
  var ch = channels[chId];
  if (!ch) return { channelId: chId, users: [], talkingUser: null, talkingCallsign: null };
  var list = [];
  for (var i = 0; i < ch.users.length; i++) {
    var uid = ch.users[i];
    if (users[uid]) {
      list.push({
        id: uid,
        callsign: users[uid].callsign,
        isTalking: users[uid].isTalking
      });
    }
  }
  return {
    channelId: chId,
    users: list,
    talkingUser: ch.talkingUser,
    talkingCallsign: ch.talkingUser && users[ch.talkingUser] ? users[ch.talkingUser].callsign : null
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
      talkingCallsign: ch && ch.talkingUser && users[ch.talkingUser] ? users[ch.talkingUser].callsign : null
    });
  }
  return result;
}

// ========== CONNEXIONS SOCKET ==========
io.on('connection', function(socket) {
  console.log('[+] Connexion: ' + socket.id);

  // Identification
  socket.on('user:join', function(data) {
    var callsign = String(data.callsign || 'UNIT').slice(0, 16).toUpperCase();
    users[socket.id] = {
      callsign: callsign,
      channel: null,
      isTalking: false,
      pttTimeout: null
    };
    socket.emit('user:joined', { id: socket.id, callsign: callsign });
    console.log('[i] ' + callsign + ' identifie (' + socket.id + ')');

    // Envoyer l'état d'urgence si actif
    if (emergencyActive) {
      socket.emit('emergency:alert', {
        userId: null,
        callsign: emergencyBy || '???',
        timestamp: new Date().toISOString()
      });
    }
  });

  // Rejoindre un canal
  socket.on('channel:join', function(data) {
    var user = users[socket.id];
    if (!user) return socket.emit('error:msg', { msg: 'Non identifie' });

    leaveChannel(socket);

    var id = parseInt(data.channelId, 10);
    if (isNaN(id) || id < 1 || id > 16) id = 1;

    if (!channels[id]) channels[id] = { users: [], talkingUser: null };
    channels[id].users.push(socket.id);
    user.channel = id;
    socket.join('ch:' + id);

    var peerList = [];
    for (var i = 0; i < channels[id].users.length; i++) {
      var pid = channels[id].users[i];
      if (pid !== socket.id && users[pid]) {
        peerList.push({ id: pid, callsign: users[pid].callsign });
      }
    }

    socket.emit('channel:joined', { channelId: id, peers: peerList });
    socket.to('ch:' + id).emit('peer:joined', { id: socket.id, callsign: user.callsign });
    broadcastChannel(id);
    io.emit('channels:status', getAllChannelsStatus());
    console.log('[i] ' + user.callsign + ' -> canal ' + id);
  });

  // Scan canaux
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
      return socket.emit('ptt:denied', {
        reason: 'CANAL OCCUPE',
        by: blocker ? blocker.callsign : '???'
      });
    }

    ch.talkingUser = socket.id;
    user.isTalking = true;

    socket.emit('ptt:granted');
    io.to('ch:' + user.channel).emit('ptt:active', {
      userId: socket.id,
      callsign: user.callsign
    });
    broadcastChannel(user.channel);

    if (user.pttTimeout) clearTimeout(user.pttTimeout);
    user.pttTimeout = setTimeout(function() {
      if (ch.talkingUser === socket.id) {
        ch.talkingUser = null;
        user.isTalking = false;
        socket.emit('ptt:timeout');
        io.to('ch:' + user.channel).emit('ptt:released', {
          userId: socket.id,
          callsign: user.callsign
        });
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
    io.to('ch:' + user.channel).emit('ptt:released', {
      userId: socket.id,
      callsign: user.callsign
    });
    broadcastChannel(user.channel);
  });

  // ===== WEBRTC =====
  socket.on('webrtc:offer', function(data) {
    io.to(data.targetId).emit('webrtc:offer', { fromId: socket.id, offer: data.offer });
  });
  socket.on('webrtc:answer', function(data) {
    io.to(data.targetId).emit('webrtc:answer', { fromId: socket.id, answer: data.answer });
  });
  socket.on('webrtc:ice', function(data) {
    io.to(data.targetId).emit('webrtc:ice', { fromId: socket.id, candidate: data.candidate });
  });

  // ===== URGENCE =====
  socket.on('emergency:activate', function() {
    var user = users[socket.id];
    if (!user) return;
    emergencyActive = true;
    emergencyBy = user.callsign;
    console.log('[!!!] URGENCE par ' + user.callsign);
    io.emit('emergency:alert', {
      userId: socket.id,
      callsign: user.callsign,
      timestamp: new Date().toISOString()
    });
  });

  socket.on('emergency:cancel', function() {
    var user = users[socket.id];
    emergencyActive = false;
    emergencyBy = null;
    io.emit('emergency:cancelled', {
      callsign: user ? user.callsign : '???'
    });
  });

  // ===== MESSAGE TEXTE =====
  socket.on('text:send', function(data) {
    var user = users[socket.id];
    if (!user || user.channel === null) return;
    io.to('ch:' + user.channel).emit('text:received', {
      callsign: user.callsign,
      message: String(data.message).slice(0, 200),
      timestamp: new Date().toISOString()
    });
  });

  // ===== DÉCONNEXION =====
  socket.on('disconnect', function() {
    var user = users[socket.id];
    if (user) {
      if (user.pttTimeout) clearTimeout(user.pttTimeout);
      leaveChannel(socket);
      console.log('[-] ' + user.callsign + ' deconnecte');
    }
    delete users[socket.id];
    io.emit('channels:status', getAllChannelsStatus());
  });
});

// ========== ROUTES ==========
app.get('/health', function(req, res) {
  res.json({
    status: 'OK',
    uptime: Math.round(process.uptime()),
    users: Object.keys(users).length,
    channels: Object.keys(channels).length,
    emergency: emergencyActive
  });
});

app.get('/', function(req, res) {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

// ========== DÉMARRAGE ==========
var PORT = process.env.PORT || 3000;
server.listen(PORT, function() {
  console.log('=================================');
  console.log('  RADIO PTT SERVER v2.0');
  console.log('  Port: ' + PORT);
  console.log('  Status: OPERATIONAL');
  console.log('=================================');
});
