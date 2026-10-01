WHATSAPP GROUP CREATOR PRO — FINAL

This build keeps the fast creation path simple and separates invite-link recovery from group creation.

FIXED SETTINGS
- Maximum 25 groups per batch.
- Maximum 100 created groups per day for each connected WhatsApp account.
- Daily usage is keyed by the WhatsApp account number and persisted in MongoDB when configured.
- Fixed 5-second gap between group-creation requests. No random jitter.
- Starting another batch shortly after the previous one only shows a soft warning. It does not block the user.
- A failed invite-link lookup never recreates the group. The created group is recorded as success and the link is recovered separately in the background.
- If a link still cannot be recovered, the UI provides a "Get link" action that only requests the link; it never creates another group.

FILES
- server.js
- public/index.html
- package.json
- .env.example
- .gitignore

RUN
1. npm install
2. Copy .env.example to .env and set ACCESS_KEY.
3. Set MONGODB_URI (or MONGODB_URL) if daily limits and history must survive redeploys.
4. npm start
5. Open http://localhost:3000

IMPORTANT
Keep the existing auth/ directory if you want the same linked WhatsApp session. Do not upload node_modules or .env to GitHub.
