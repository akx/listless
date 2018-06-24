const WebSocket = require('ws');

const ws = new WebSocket('ws://localhost:8765/');

ws.on('open', function open() {
  const a = [];
  for(var i = 0; i < 50; i++) {
    a.push({'v': 'put', 'db': 'aaa', 'message': i});
  }
  ws.send(JSON.stringify(a));
});

ws.on('message', function incoming(data) {
  console.log(data);
});

setTimeout(() => {
  ws.send(JSON.stringify({'v': 'get', 'db': 'aaa'}));
}, 1200);
