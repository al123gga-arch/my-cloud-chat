<!DOCTYPE html>
<html lang="ru">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0, viewport-fit=cover, user-scalable=no">
    <title>BurmaldaMessenger</title>
    <style>
        * { box-sizing: border-box; -webkit-tap-highlight-color: transparent; }
        :root {
            --bg: #050505;
            --surface: #000;
            --surface-light: #111;
            --border: #333;
            --text: #fff;
            --accent: #00d2ff;
            --accent-grad: linear-gradient(90deg, #00d2ff, #8800ff);
            --message-bg: #1a1a1a;
            --message-other: #1e1e1e;
            --system: #888;
        }
        [data-theme="light"] {
            --bg: #f0f2f5;
            --surface: #fff;
            --surface-light: #f5f5f5;
            --border: #ddd;
            --text: #111;
            --message-bg: #e4e6eb;
            --message-other: #dcf8c5;
            --system: #65676b;
        }
        body {
            margin: 0;
            background: var(--bg);
            color: var(--text);
            font-family: system-ui, -apple-system, 'Segoe UI', Roboto, sans-serif;
            display: flex;
            justify-content: center;
            align-items: center;
            min-height: 100vh;
            transition: background 0.2s;
        }
        .win {
            background: var(--surface);
            display: flex;
            flex-direction: column;
            position: relative;
            overflow: hidden;
        }
        @media (max-width: 600px) {
            .win { width: 100vw; height: 100vh; border-radius: 0; }
            body { background: var(--bg); }
        }
        @media (min-width: 601px) {
            .win { width: 500px; height: 700px; border-radius: 28px; box-shadow: 0 10px 30px rgba(0,0,0,0.3); }
        }
        .title {
            font-size: 32px;
            font-weight: bold;
            margin: 30px 0 20px 0;
            text-align: center;
            background: linear-gradient(45deg, #ff0055, #8800ff, #00d2ff);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
        }
        .auth-container {
            padding: 20px;
            display: flex;
            flex-direction: column;
            gap: 18px;
            width: 100%;
            max-width: 320px;
            margin: 0 auto;
        }
        .input-field {
            background: var(--surface-light);
            border: 1px solid var(--border);
            border-radius: 20px;
            padding: 14px 18px;
            color: var(--text);
            font-size: 16px;
            outline: none;
            transition: 0.2s;
            width: 100%;
        }
        .input-field:focus { border-color: var(--accent); }
        .btn {
            background: var(--accent-grad);
            border: none;
            border-radius: 40px;
            padding: 12px;
            font-weight: bold;
            font-size: 16px;
            color: white;
            cursor: pointer;
            transition: 0.2s;
        }
        .btn-secondary {
            background: transparent;
            border: 1px solid var(--accent);
            color: var(--accent);
        }
        #chat {
            display: none;
            flex-direction: column;
            height: 100%;
        }
        #msgs {
            flex: 1;
            overflow-y: auto;
            padding: 12px;
            display: flex;
            flex-direction: column;
            gap: 8px;
        }
        .message-wrapper {
            display: flex;
            flex-direction: column;
            max-width: 85%;
            position: relative;
        }
        .my-wrapper { align-self: flex-end; }
        .other-wrapper { align-self: flex-start; }
        .message {
            background: var(--message-bg);
            padding: 8px 12px;
            border-radius: 18px;
            word-wrap: break-word;
            font-size: 15px;
            cursor: pointer;
            transition: 0.1s;
        }
        .my-message { background: var(--accent); color: #000; border-bottom-right-radius: 4px; }
        .other-message { background: var(--message-other); border-bottom-left-radius: 4px; }
        .sender-name {
            font-size: 12px;
            font-weight: bold;
            color: var(--accent);
            margin-bottom: 2px;
        }
        .system {
            color: var(--system);
            font-size: 12px;
            text-align: center;
            margin: 4px 0;
        }
        .reactions {
            display: flex;
            gap: 5px;
            margin-top: 4px;
            font-size: 14px;
        }
        .reaction {
            background: var(--surface-light);
            border-radius: 20px;
            padding: 2px 6px;
            cursor: pointer;
        }
        .typing-indicator {
            font-size: 12px;
            color: var(--system);
            padding: 5px 12px;
            font-style: italic;
        }
        .input-area {
            display: flex;
            gap: 8px;
            padding: 10px;
            background: var(--surface-light);
            border-top: 1px solid var(--border);
            align-items: center;
        }
        .emoji-btn, .theme-toggle, .logout-btn, .call-btn {
            background: none;
            border: none;
            font-size: 24px;
            cursor: pointer;
            padding: 8px;
            border-radius: 30px;
            transition: 0.1s;
        }
        .message-input {
            flex: 1;
            background: var(--surface);
            border: 1px solid var(--border);
            border-radius: 30px;
            padding: 10px 16px;
            color: var(--text);
            font-size: 15px;
            outline: none;
        }
        .send-btn {
            background: var(--accent);
            border: none;
            border-radius: 30px;
            padding: 8px 16px;
            font-weight: bold;
            color: black;
            cursor: pointer;
        }
        .error {
            color: #ff4466;
            font-size: 13px;
            text-align: center;
            margin-top: 5px;
        }
        .switch-form {
            text-align: center;
            margin-top: 10px;
            font-size: 14px;
        }
        .switch-form span {
            color: var(--accent);
            cursor: pointer;
            text-decoration: underline;
        }
        .call-modal {
            position: fixed;
            bottom: 100px;
            left: 20px;
            background: var(--surface);
            border-radius: 16px;
            padding: 10px;
            z-index: 100;
            border: 1px solid var(--border);
        }
        video { width: 200px; border-radius: 12px; }
        .header-buttons {
            display: flex;
            justify-content: space-between;
            padding: 8px 12px;
            border-bottom: 1px solid var(--border);
        }
    </style>
</head>
<body>
<div class="win" id="app">
    <div id="authForm">
        <div class="title">BurmaldaMessenger</div>
        <div class="auth-container">
            <input type="text" id="loginUsername" class="input-field" placeholder="Никнейм" autocomplete="username">
            <input type="password" id="loginPassword" class="input-field" placeholder="Пароль (мин. 8 символов)" autocomplete="current-password">
            <button id="loginBtn" class="btn">Войти</button>
            <div class="switch-form">Нет аккаунта? <span id="showRegister">Зарегистрироваться</span></div>
            <div id="authError" class="error"></div>
        </div>
    </div>
    <div id="registerForm" style="display: none;">
        <div class="title">Регистрация</div>
        <div class="auth-container">
            <input type="text" id="regUsername" class="input-field" placeholder="Уникальный ник" autocomplete="off">
            <input type="password" id="regPassword" class="input-field" placeholder="Пароль (≥8 символов)" autocomplete="new-password">
            <button id="registerBtn" class="btn">Создать аккаунт</button>
            <div class="switch-form">Уже есть аккаунт? <span id="showLogin">Войти</span></div>
            <div id="regError" class="error"></div>
        </div>
    </div>
    <div id="chat">
        <div class="header-buttons">
            <button id="themeToggle" class="theme-toggle">🌙</button>
            <button id="logoutBtn" class="logout-btn" style="font-size: 18px;">🚪 Выйти</button>
            <button id="callBtn" class="call-btn">📞</button>
        </div>
        <div id="msgs"></div>
        <div id="typingStatus" class="typing-indicator"></div>
        <div class="input-area">
            <button id="emojiBtn" class="emoji-btn">😊</button>
            <input type="text" id="messageInput" class="message-input" placeholder="Сообщение..." autocomplete="off">
            <button id="sendBtn" class="send-btn">➤</button>
        </div>
    </div>
</div>
<script>
    // ---------- Глобальные ----------
    let ws = null;
    let currentUser = null;
    let unreadCount = 0;
    let originalTitle = document.title;
    let typingTimeout = null;
    let messagesMap = new Map(); // id -> DOM элемент
    let callPeer = null;
    let localStream = null;

    // DOM
    const authForm = document.getElementById('authForm');
    const registerForm = document.getElementById('registerForm');
    const chatDiv = document.getElementById('chat');
    const msgsDiv = document.getElementById('msgs');
    const messageInput = document.getElementById('messageInput');
    const sendBtn = document.getElementById('sendBtn');
    const logoutBtn = document.getElementById('logoutBtn');
    const themeToggle = document.getElementById('themeToggle');
    const callBtn = document.getElementById('callBtn');

    // ---------- Тема ----------
    function setTheme(theme) {
        document.documentElement.setAttribute('data-theme', theme);
        localStorage.setItem('theme', theme);
        themeToggle.innerText = theme === 'dark' ? '🌙' : '☀️';
    }
    const savedTheme = localStorage.getItem('theme') || 'dark';
    setTheme(savedTheme);
    themeToggle.onclick = () => {
        const newTheme = document.documentElement.getAttribute('data-theme') === 'dark' ? 'light' : 'dark';
        setTheme(newTheme);
    };

    // ---------- Переключение форм ----------
    document.getElementById('showRegister').onclick = () => { authForm.style.display = 'none'; registerForm.style.display = 'flex'; clearErrors(); };
    document.getElementById('showLogin').onclick = () => { registerForm.style.display = 'none'; authForm.style.display = 'flex'; clearErrors(); };
    function clearErrors() { document.getElementById('authError').innerText = ''; document.getElementById('regError').innerText = ''; }

    function showAuthError(msg) { document.getElementById('authError').innerText = msg; }
    function showRegError(msg) { document.getElementById('regError').innerText = msg; }

    // ---------- Логин ----------
    document.getElementById('loginBtn').onclick = async () => {
        const username = document.getElementById('loginUsername').value.trim();
        const password = document.getElementById('loginPassword').value;
        if (!username || !password) { showAuthError('Заполните оба поля'); return; }
        try {
            const res = await fetch('/login', {
                method: 'POST',
                headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
                body: `username=${encodeURIComponent(username)}&password=${encodeURIComponent(password)}`
            });
            const data = await res.json();
            if (data.ok) {
                currentUser = data.username;
                localStorage.setItem('burmalda_user', currentUser);
                connectWebSocket();
            } else {
                showAuthError(data.error || 'Ошибка входа');
            }
        } catch(e) {
            showAuthError('Ошибка соединения с сервером');
        }
    };

    // ---------- Регистрация ----------
    document.getElementById('registerBtn').onclick = async () => {
        const username = document.getElementById('regUsername').value.trim();
        const password = document.getElementById('regPassword').value;
        if (!username || !password) { showRegError('Заполните оба поля'); return; }
        if (password.length < 8) { showRegError('Пароль должен быть минимум 8 символов'); return; }
        try {
            const res = await fetch('/register', {
                method: 'POST',
                headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
                body: `username=${encodeURIComponent(username)}&password=${encodeURIComponent(password)}`
            });
            const data = await res.json();
            if (data.ok) {
                registerForm.style.display = 'none';
                authForm.style.display = 'flex';
                document.getElementById('loginUsername').value = username;
                document.getElementById('loginPassword').value = password;
                showAuthError('Регистрация успешна! Теперь войдите.');
            } else {
                showRegError(data.error || 'Ошибка регистрации');
            }
        } catch(e) {
            showRegError('Ошибка соединения с сервером');
        }
    };

    // ---------- WebSocket ----------
    function connectWebSocket() {
        const protocol = window.location.protocol === 'https:' ? 'wss://' : 'ws://';
        ws = new WebSocket(`${protocol}${window.location.host}/ws/${encodeURIComponent(currentUser)}`);
        ws.onopen = () => {
            authForm.style.display = 'none';
            registerForm.style.display = 'none';
            chatDiv.style.display = 'flex';
            msgsDiv.innerHTML = '';
            messagesMap.clear();
            unreadCount = 0;
            document.title = originalTitle;
        };
        ws.onmessage = (e) => {
            const msg = JSON.parse(e.data);
            handleMessage(msg);
        };
        ws.onclose = () => {
            if (chatDiv.style.display === 'flex') {
                alert('Соединение потеряно. Обновите страницу.');
                logout();
            }
        };
    }

    function handleMessage(msg) {
        if (msg.type === 'history') {
            renderMessage(msg.data);
        } else if (msg.type === 'message') {
            renderMessage(msg.data);
            if (document.hidden) {
                unreadCount++;
                document.title = `(${unreadCount}) ${originalTitle}`;
            }
        } else if (msg.type === 'system') {
            const div = document.createElement('div');
            div.className = 'system';
            div.innerText = msg.text;
            msgsDiv.appendChild(div);
            msgsDiv.scrollTop = msgsDiv.scrollHeight;
        } else if (msg.type === 'typing') {
            const typingDiv = document.getElementById('typingStatus');
            const others = msg.users.filter(u => u !== currentUser);
            if (others.length) {
                typingDiv.innerText = others.join(', ') + ' печатает...';
            } else {
                typingDiv.innerText = '';
            }
        } else if (msg.type === 'delete') {
            const el = messagesMap.get(msg.msg_id);
            if (el) el.remove();
            messagesMap.delete(msg.msg_id);
        } else if (msg.type === 'update_reactions') {
            const wrapper = messagesMap.get(msg.msg_id);
            if (wrapper) {
                let reactionsDiv = wrapper.querySelector('.reactions');
                if (!reactionsDiv) {
                    reactionsDiv = document.createElement('div');
                    reactionsDiv.className = 'reactions';
                    wrapper.appendChild(reactionsDiv);
                }
                reactionsDiv.innerHTML = '';
                for (const [emoji, users] of Object.entries(msg.reactions)) {
                    const span = document.createElement('span');
                    span.className = 'reaction';
                    span.innerText = `${emoji} ${users.length}`;
                    span.onclick = (e) => { e.stopPropagation(); sendReaction(msg.msg_id, emoji); };
                    reactionsDiv.appendChild(span);
                }
            }
        }
    }

    function renderMessage(data) {
        const wrapper = document.createElement('div');
        wrapper.className = `message-wrapper ${data.sender === currentUser ? 'my-wrapper' : 'other-wrapper'}`;
        const messageDiv = document.createElement('div');
        messageDiv.className = `message ${data.sender === currentUser ? 'my-message' : 'other-message'}`;
        if (data.sender !== currentUser) {
            const nameSpan = document.createElement('div');
            nameSpan.className = 'sender-name';
            nameSpan.innerText = data.sender;
            messageDiv.appendChild(nameSpan);
        }
        const textSpan = document.createElement('span');
        textSpan.innerText = data.text;
        messageDiv.appendChild(textSpan);
        wrapper.appendChild(messageDiv);

        // Реакции
        if (data.reactions && Object.keys(data.reactions).length) {
            const reactionsDiv = document.createElement('div');
            reactionsDiv.className = 'reactions';
            for (const [emoji, users] of Object.entries(data.reactions)) {
                const span = document.createElement('span');
                span.className = 'reaction';
                span.innerText = `${emoji} ${users.length}`;
                span.onclick = (e) => { e.stopPropagation(); sendReaction(data.id, emoji); };
                reactionsDiv.appendChild(span);
            }
            wrapper.appendChild(reactionsDiv);
        }

        // Контекстное меню / долгое нажатие
        let pressTimer;
        messageDiv.addEventListener('touchstart', (e) => {
            pressTimer = setTimeout(() => {
                if (data.sender === currentUser) showDeleteMenu(data.id);
            }, 500);
        });
        messageDiv.addEventListener('touchend', () => clearTimeout(pressTimer));
        messageDiv.addEventListener('touchmove', () => clearTimeout(pressTimer));
        messageDiv.addEventListener('contextmenu', (e) => {
            e.preventDefault();
            if (data.sender === currentUser) showDeleteMenu(data.id);
        });

        // Клик для реакций
        messageDiv.addEventListener('click', (e) => {
            if (e.target === messageDiv || e.target === textSpan) showReactionPicker(data.id);
        });

        msgsDiv.appendChild(wrapper);
        msgsDiv.scrollTop = msgsDiv.scrollHeight;
        messagesMap.set(data.id, wrapper);
    }

    function showDeleteMenu(msgId) {
        const choice = confirm('Удалить сообщение?\n"OK" — удалить у всех\n"Отмена" — только у себя');
        if (choice) {
            ws.send(JSON.stringify({ type: 'delete', msg_id: msgId }));
        } else {
            const el = messagesMap.get(msgId);
            if (el) el.remove();
            messagesMap.delete(msgId);
        }
    }

    function showReactionPicker(msgId) {
        const emojis = ['😍','❤️','😂','🤣','😡','👿','👍','👎','🎉','🔥','✅','😭','💀','✌️','😘','🥲'];
        let picker = document.createElement('div');
        picker.style.position = 'fixed';
        picker.style.bottom = '80px';
        picker.style.left = '20px';
        picker.style.background = 'var(--surface)';
        picker.style.borderRadius = '20px';
        picker.style.padding = '8px';
        picker.style.display = 'grid';
        picker.style.gridTemplateColumns = 'repeat(4, 1fr)';
        picker.style.gap = '6px';
        picker.style.border = '1px solid var(--border)';
        picker.style.zIndex = '200';
        emojis.forEach(emo => {
            const btn = document.createElement('button');
            btn.textContent = emo;
            btn.style.fontSize = '28px';
            btn.style.background = 'none';
            btn.style.border = 'none';
            btn.style.cursor = 'pointer';
            btn.onclick = () => {
                sendReaction(msgId, emo);
                picker.remove();
            };
            picker.appendChild(btn);
        });
        document.body.appendChild(picker);
        setTimeout(() => picker.remove(), 5000);
    }

    function sendReaction(msgId, emoji) {
        ws.send(JSON.stringify({ type: 'react', msg_id: msgId, emoji: emoji }));
    }

    // ---------- Отправка сообщений и статус печатает ----------
    function sendMessage() {
        const text = messageInput.value.trim();
        if (text && ws && ws.readyState === WebSocket.OPEN) {
            ws.send(JSON.stringify({ type: 'text', text: text }));
            messageInput.value = '';
            clearTyping();
        }
    }
    function startTyping() {
        if (ws && ws.readyState === WebSocket.OPEN) {
            ws.send(JSON.stringify({ type: 'typing', typing: true }));
            if (typingTimeout) clearTimeout(typingTimeout);
            typingTimeout = setTimeout(clearTyping, 2000);
        }
    }
    function clearTyping() {
        if (ws && ws.readyState === WebSocket.OPEN) {
            ws.send(JSON.stringify({ type: 'typing', typing: false }));
        }
    }
    messageInput.addEventListener('input', startTyping);
    messageInput.addEventListener('keypress', (e) => { if (e.key === 'Enter') sendMessage(); });
    sendBtn.onclick = sendMessage;

    // Выход
    function logout() {
        if (ws) ws.close();
        localStorage.removeItem('burmalda_user');
        currentUser = null;
        chatDiv.style.display = 'none';
        authForm.style.display = 'flex';
        registerForm.style.display = 'none';
        document.getElementById('loginPassword').value = '';
        document.getElementById('loginUsername').value = '';
        document.getElementById('regPassword').value = '';
        document.getElementById('regUsername').value = '';
        clearErrors();
    }
    logoutBtn.onclick = logout;

    // Эмодзи-пикер в поле ввода
    document.getElementById('emojiBtn').onclick = () => {
        const picker = document.createElement('div');
        picker.style.position = 'fixed';
        picker.style.bottom = '80px';
        picker.style.left = '20px';
        picker.style.background = 'var(--surface)';
        picker.style.borderRadius = '20px';
        picker.style.padding = '8px';
        picker.style.display = 'grid';
        picker.style.gridTemplateColumns = 'repeat(5, 1fr)';
        picker.style.gap = '6px';
        picker.style.border = '1px solid var(--border)';
        const emojis = ['😀','😂','😍','😎','😢','🔥','❤️','👍','🎉','🤔','👀','✅','⭐','🍕','💀'];
        emojis.forEach(emo => {
            const btn = document.createElement('button');
            btn.textContent = emo;
            btn.style.fontSize = '28px';
            btn.style.background = 'none';
            btn.style.border = 'none';
            btn.style.cursor = 'pointer';
            btn.onclick = () => {
                messageInput.value += emo;
                picker.remove();
            };
            picker.appendChild(btn);
        });
        document.body.appendChild(picker);
        setTimeout(() => picker.remove(), 5000);
    };

    // ---------- WebRTC звонок (упрощённый) ----------
    callBtn.onclick = async () => {
        if (!navigator.mediaDevices) { alert('Ваш браузер не поддерживает звонки'); return; }
        try {
            localStream = await navigator.mediaDevices.getUserMedia({ video: true, audio: true });
            const modal = document.createElement('div');
            modal.className = 'call-modal';
            modal.innerHTML = `
                <video id="localVideo" autoplay muted style="width:200px; border-radius:12px;"></video>
                <video id="remoteVideo" autoplay style="width:200px; border-radius:12px;"></video>
                <button id="hangup" style="margin-top:8px;">Завершить</button>
            `;
            document.body.appendChild(modal);
            document.getElementById('localVideo').srcObject = localStream;
            callPeer = new RTCPeerConnection({ iceServers: [{ urls: 'stun:stun.l.google.com:19302' }] });
            localStream.getTracks().forEach(track => callPeer.addTrack(track, localStream));
            callPeer.ontrack = event => { document.getElementById('remoteVideo').srcObject = event.streams[0]; };
            callPeer.onicecandidate = event => {
                if (event.candidate) ws.send(JSON.stringify({ type: 'call_ice', target: null, candidate: event.candidate }));
            };
            const offer = await callPeer.createOffer();
            await callPeer.setLocalDescription(offer);
            ws.send(JSON.stringify({ type: 'call_offer', target: null, offer: offer }));
            document.getElementById('hangup').onclick = () => {
                if (callPeer) callPeer.close();
                if (localStream) localStream.getTracks().forEach(t => t.stop());
                modal.remove();
            };
        } catch(e) { alert('Не удалось получить доступ к камере/микрофону'); }
    };

    // Непрочитанные при переключении вкладки
    document.addEventListener('visibilitychange', () => {
        if (!document.hidden) {
            unreadCount = 0;
            document.title = originalTitle;
        }
    });
</script>
</body>
</html>
