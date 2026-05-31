<!DOCTYPE html>
<html lang="ru">
<head>
    <meta charset="UTF-8">
    <title>Cloud Chat</title>
    <style>
        body { background: #0f172a; color: #f8fafc; font-family: 'Segoe UI', sans-serif; display: flex; justify-content: center; align-items: center; min-height: 100vh; margin: 0; }
        .container { background: #1e293b; width: 100%; max-width: 400px; height: 80vh; border-radius: 20px; display: flex; flex-direction: column; overflow: hidden; box-shadow: 0 10px 25px rgba(0,0,0,0.5); }
        #auth-screen { display: flex; flex-direction: column; justify-content: center; align-items: center; padding: 20px; height: 100%; }
        #chat-screen { display: none; flex-direction: column; height: 100%; }
        #messages { flex-grow: 1; overflow-y: auto; padding: 15px; display: flex; flex-direction: column; gap: 8px; }
        .msg { padding: 10px; border-radius: 12px; background: #334155; }
        .my { align-self: flex-end; background: #38bdf8; color: #0f172a; }
        .input-area { padding: 10px; background: #0f172a; display: flex; gap: 5px; }
        input { flex-grow: 1; padding: 10px; border-radius: 8px; border: none; background: #1e293b; color: white; }
        button { padding: 10px 15px; border-radius: 8px; border: none; background: #38bdf8; font-weight: bold; cursor: pointer; }
        .emojis { padding: 5px; background: #1e293b; display: flex; gap: 5px; flex-wrap: wrap; justify-content: center; }
    </style>
</head>
<body>
<div class="container">
    <div id="auth-screen">
        <input type="text" id="username" placeholder="Твой никнейм...">
        <button onclick="join()" style="margin-top:10px; width:100%">Войти</button>
    </div>
    <div id="chat-screen">
        <div id="messages"></div>
        <div class="emojis" id="emoji-bar"></div>
        <div class="input-area">
            <input type="text" id="msg" placeholder="Сообщение...">
            <button onclick="send()">➤</button>
        </div>
    </div>
</div>
<script>
    let ws;
    const emojis = ["😊", "😂", "🔥", "👍", "❤️", "🤔", "🎉", "😎"];
    emojis.forEach(e => {
        let s = document.createElement("span");
        s.innerText = e; s.style.cursor = "pointer";
        s.onclick = () => document.getElementById("msg").value += e;
        document.getElementById("emoji-bar").appendChild(s);
    });

    function join() {
        const u = document.getElementById("username").value;
        if(!u) return;
        ws = new WebSocket(`${window.location.protocol === 'https:' ? 'wss://' : 'ws://'}${window.location.host}/ws/${u}`);
        ws.onopen = () => { document.getElementById("auth-screen").style.display = "none"; document.getElementById("chat-screen").style.display = "flex"; };
        ws.onmessage = (e) => {
            const div = document.createElement("div");
            div.className = "msg";
            div.innerText = e.data;
            document.getElementById("messages").appendChild(div);
            document.getElementById("messages").scrollTop = 9999;
        };
    }
    function send() {
        const m = document.getElementById("msg");
        if(m.value) { ws.send(m.value); m.value = ""; }
    }
</script>
</body>
</html>
