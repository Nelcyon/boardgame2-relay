// Boardgame WebSocket Relay Server for Deno Deploy
// Handles room creation, joining via room codes, and message relaying between up to 4 players.

const MAX_PLAYERS = 4;
const HEARTBEAT_INTERVAL_MS = 20_000; // server pings clients every 20s
const HEARTBEAT_TIMEOUT_MS = 60_000;  // consider dead after 60s of silence

// Room state: room code → array of sockets in the room
const rooms = new Map<string, WebSocket[]>();
// Socket → room code
const socketToRoom = new Map<WebSocket, string>();
// Socket → stable player number (1–4, never changes even if others disconnect)
const socketToPlayer = new Map<WebSocket, number>();
// Socket → last activity timestamp (ms)
const socketLastActivity = new Map<WebSocket, number>();

function generateRoomCode(): string {
  const chars = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"; // no ambiguous chars (0/O, 1/I)
  let code = "";
  for (let i = 0; i < 6; i++) {
    code += chars[Math.floor(Math.random() * chars.length)];
  }
  return code;
}

function getPlayersInRoom(roomCode: string): number[] {
  const sockets = rooms.get(roomCode);
  if (!sockets) return [];
  const nums: number[] = [];
  for (const ws of sockets) {
    const n = socketToPlayer.get(ws);
    if (n !== undefined) nums.push(n);
  }
  return nums.sort((a, b) => a - b);
}

function nextAvailablePlayerNum(roomCode: string): number {
  const taken = new Set(getPlayersInRoom(roomCode));
  for (let i = 1; i <= MAX_PLAYERS; i++) {
    if (!taken.has(i)) return i;
  }
  return 0; // room full
}

function broadcast(roomCode: string, message: string, exclude?: WebSocket) {
  const sockets = rooms.get(roomCode);
  if (!sockets) return;
  for (const ws of sockets) {
    if (ws !== exclude && ws.readyState === WebSocket.OPEN) {
      ws.send(message);
    }
  }
}

function removeFromRoom(ws: WebSocket) {
  const roomCode = socketToRoom.get(ws);
  if (!roomCode) return;

  const playerNum = socketToPlayer.get(ws) ?? 0;

  const sockets = rooms.get(roomCode);
  if (sockets) {
    const idx = sockets.indexOf(ws);
    if (idx !== -1) sockets.splice(idx, 1);

    if (sockets.length === 0) {
      rooms.delete(roomCode);
    } else {
      broadcast(roomCode, JSON.stringify({
        type: "player_disconnected",
        player: playerNum,
        players: getPlayersInRoom(roomCode),
      }));
    }
  }
  socketToRoom.delete(ws);
  socketToPlayer.delete(ws);
}

function handleWebSocket(ws: WebSocket, url: URL) {
  const action = url.searchParams.get("action"); // "host" or "join"
  const roomCode = url.searchParams.get("room")?.toUpperCase();

  ws.onopen = () => {
    socketLastActivity.set(ws, Date.now());
    if (action === "host") {
      // Create a new room
      let code = generateRoomCode();
      while (rooms.has(code)) code = generateRoomCode();

      rooms.set(code, [ws]);
      socketToRoom.set(ws, code);
      socketToPlayer.set(ws, 1);

      ws.send(JSON.stringify({
        type: "room_created",
        room: code,
        player: 1,
        players: [1],
      }));

    } else if (action === "join" && roomCode) {
      // Join existing room
      const sockets = rooms.get(roomCode);

      if (!sockets) {
        ws.send(JSON.stringify({ type: "error", message: "Room not found" }));
        ws.close();
        return;
      }
      if (sockets.length >= MAX_PLAYERS) {
        ws.send(JSON.stringify({ type: "error", message: "Room is full" }));
        ws.close();
        return;
      }

      const playerNum = nextAvailablePlayerNum(roomCode);
      sockets.push(ws);
      socketToRoom.set(ws, roomCode);
      socketToPlayer.set(ws, playerNum);

      const playerList = getPlayersInRoom(roomCode);

      // Notify joiner with their number and who else is in the room
      ws.send(JSON.stringify({
        type: "room_joined",
        room: roomCode,
        player: playerNum,
        players: playerList,
      }));

      // Notify all existing players that someone new joined
      broadcast(roomCode, JSON.stringify({
        type: "player_joined",
        player: playerNum,
        players: playerList,
      }), ws);

    } else {
      ws.send(JSON.stringify({ type: "error", message: "Invalid action. Use ?action=host or ?action=join&room=CODE" }));
      ws.close();
    }
  };

  ws.onmessage = (event) => {
    socketLastActivity.set(ws, Date.now());
    const room = socketToRoom.get(ws);
    if (!room) return;

    // Handle heartbeat pings from client — respond with pong, don't relay
    try {
      const msg = JSON.parse(event.data as string);
      if (msg && msg.type === "ping") {
        if (ws.readyState === WebSocket.OPEN) {
          ws.send(JSON.stringify({ type: "pong" }));
        }
        return;
      }
    } catch (_) {
      // Not JSON — relay as-is
    }

    // Relay message to all other players in the room
    broadcast(room, event.data as string, ws);
  };

  ws.onclose = () => {
    socketLastActivity.delete(ws);
    removeFromRoom(ws);
  };
  ws.onerror = () => {
    socketLastActivity.delete(ws);
    removeFromRoom(ws);
  };
}

// Server-side heartbeat: periodically ping all connected sockets and close dead ones
setInterval(() => {
  const now = Date.now();
  for (const [ws, lastActive] of socketLastActivity) {
    if (ws.readyState !== WebSocket.OPEN) {
      socketLastActivity.delete(ws);
      removeFromRoom(ws);
      continue;
    }
    if (now - lastActive > HEARTBEAT_TIMEOUT_MS) {
      console.log(`[Relay] Closing dead connection (player ${socketToPlayer.get(ws) ?? "?"}, silent for ${Math.round((now - lastActive) / 1000)}s)`);
      socketLastActivity.delete(ws);
      try { ws.close(4000, "heartbeat timeout"); } catch (_) { /* ignore */ }
      removeFromRoom(ws);
      continue;
    }
    // Send server-side ping to keep connection alive through proxies/NATs
    try {
      ws.send(JSON.stringify({ type: "heartbeat" }));
    } catch (_) {
      socketLastActivity.delete(ws);
      removeFromRoom(ws);
    }
  }
}, HEARTBEAT_INTERVAL_MS);

Deno.serve({ port: 8000 }, (req: Request) => {
  const url = new URL(req.url);

  // Health check
  if (url.pathname === "/ping") {
    return new Response("pong", { status: 200 });
  }

  // WebSocket upgrade
  if (req.headers.get("upgrade")?.toLowerCase() === "websocket") {
    const { socket, response } = Deno.upgradeWebSocket(req);
    handleWebSocket(socket, url);
    return response;
  }

  return new Response("Boardgame Relay Server. Connect via WebSocket.", { status: 200 });
});
