// Boardgame WebSocket Relay Server for Deno Deploy
// Handles room creation, joining via room codes, and message relaying between 2 players.

const rooms = new Map<string, WebSocket[]>();
const socketToRoom = new Map<WebSocket, string>();

function generateRoomCode(): string {
  const chars = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"; // no ambiguous chars (0/O, 1/I)
  let code = "";
  for (let i = 0; i < 6; i++) {
    code += chars[Math.floor(Math.random() * chars.length)];
  }
  return code;
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

  const sockets = rooms.get(roomCode);
  if (sockets) {
    const idx = sockets.indexOf(ws);
    const playerNum = idx + 1;
    sockets.splice(idx, 1);

    if (sockets.length === 0) {
      rooms.delete(roomCode);
    } else {
      broadcast(roomCode, JSON.stringify({
        type: "player_disconnected",
        player: playerNum,
      }));
    }
  }
  socketToRoom.delete(ws);
}

function handleWebSocket(ws: WebSocket, url: URL) {
  const action = url.searchParams.get("action"); // "host" or "join"
  const roomCode = url.searchParams.get("room")?.toUpperCase();

  ws.onopen = () => {
    if (action === "host") {
      // Create a new room
      let code = generateRoomCode();
      while (rooms.has(code)) code = generateRoomCode();

      rooms.set(code, [ws]);
      socketToRoom.set(ws, code);

      ws.send(JSON.stringify({
        type: "room_created",
        room: code,
        player: 1,
      }));

    } else if (action === "join" && roomCode) {
      // Join existing room
      const sockets = rooms.get(roomCode);

      if (!sockets) {
        ws.send(JSON.stringify({ type: "error", message: "Room not found" }));
        ws.close();
        return;
      }
      if (sockets.length >= 2) {
        ws.send(JSON.stringify({ type: "error", message: "Room is full" }));
        ws.close();
        return;
      }

      sockets.push(ws);
      socketToRoom.set(ws, roomCode);

      // Notify joiner
      ws.send(JSON.stringify({
        type: "room_joined",
        room: roomCode,
        player: 2,
      }));

      // Notify host that player 2 joined
      broadcast(roomCode, JSON.stringify({
        type: "player_joined",
        player: 2,
      }), ws);

    } else {
      ws.send(JSON.stringify({ type: "error", message: "Invalid action. Use ?action=host or ?action=join&room=CODE" }));
      ws.close();
    }
  };

  ws.onmessage = (event) => {
    const room = socketToRoom.get(ws);
    if (!room) return;

    // Relay message to the other player in the room
    broadcast(room, event.data as string, ws);
  };

  ws.onclose = () => removeFromRoom(ws);
  ws.onerror = () => removeFromRoom(ws);
}

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
