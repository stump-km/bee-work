package main

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

/* ============ 1. Bus：MCP Client（Bearer Token，断线重连） ============ */

type authRT struct {
	tok  string
	base http.RoundTripper
}

func (a authRT) RoundTrip(r *http.Request) (*http.Response, error) {
	r = r.Clone(r.Context())
	r.Header.Set("Authorization", "Bearer "+a.tok)
	return a.base.RoundTrip(r)
}

type Bus struct {
	url, tok string
	mu       sync.Mutex
	cs       *mcp.ClientSession
}

func (b *Bus) sess() (*mcp.ClientSession, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.cs != nil {
		return b.cs, nil
	}
	// 不给 http.Client 设整体 Timeout：Streamable HTTP 可能有长连接的 SSE 流，
	// 整体超时会把它定期掐断。改为只限制"等响应头"的时间。
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.ResponseHeaderTimeout = 30 * time.Second
	c := mcp.NewClient(&mcp.Implementation{Name: "chat-gateway", Version: "0.2.0"}, nil)
	// Connect 用 Background，避免调用方的 ctx 取消时把整个会话一起关掉
	cs, err := c.Connect(context.Background(), &mcp.StreamableClientTransport{
		Endpoint:   b.url,
		HTTPClient: &http.Client{Transport: authRT{tok: b.tok, base: tr}},
	}, nil)
	if err != nil {
		return nil, err
	}
	b.cs = cs
	return cs, nil
}

func (b *Bus) reset() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.cs != nil {
		b.cs.Close()
		b.cs = nil
	}
}

// 结构化输出优先；没有就退回文本内容（假定是 JSON）
func resultJSON(res *mcp.CallToolResult) string {
	if res.StructuredContent != nil {
		if raw, err := json.Marshal(res.StructuredContent); err == nil {
			return string(raw)
		}
	}
	return resultText(res)
}

func resultText(res *mcp.CallToolResult) string {
	var sb strings.Builder
	for _, c := range res.Content {
		if t, ok := c.(*mcp.TextContent); ok {
			sb.WriteString(t.Text)
		}
	}
	return sb.String()
}

func (b *Bus) CallTool(ctx context.Context, name string, args map[string]any) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	var last error
	for i := 0; i < 2; i++ { // 传输层失败：重连后重试一次
		cs, err := b.sess()
		if err != nil {
			last = err
			continue
		}
		res, err := cs.CallTool(ctx, &mcp.CallToolParams{Name: name, Arguments: args})
		if err != nil {
			last = err
			b.reset()
			continue
		}
		if res.IsError { // 工具自己报错，不是连接问题，不重试
			return "", fmt.Errorf("tool %s: %s", name, resultText(res))
		}
		return resultJSON(res), nil
	}
	return "", last
}

func (b *Bus) ReadState(ctx context.Context) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	var last error
	for i := 0; i < 2; i++ {
		cs, err := b.sess()
		if err != nil {
			last = err
			continue
		}
		r, err := cs.ReadResource(ctx, &mcp.ReadResourceParams{URI: "bus://state"})
		if err != nil {
			last = err
			b.reset()
			continue
		}
		if len(r.Contents) == 0 {
			return "", fmt.Errorf("empty bus://state")
		}
		return r.Contents[0].Text, nil
	}
	return "", last
}

/* ============ 2. 与 bus 真实格式对应的类型与解析 ============ */

// post_task 输出：{id, status, target_role}
type PostResult struct {
	ID         string `json:"id"`
	Status     string `json:"status"`
	TargetRole string `json:"target_role"`
}

// post_task 输入：{session_id?, title, target_role, payload, artifact_key?, created_by?}
func (b *Bus) PostTask(ctx context.Context, sid, role, title, payload, artifactKey string) (*PostResult, error) {
	args := map[string]any{
		"title":        title,
		"target_role":  role,
		"payload":      payload,
		"artifact_key": artifactKey,
		"created_by":   "chat-gateway",
	}
	if sid != "" {
		args["session_id"] = sid
	}
	out, err := b.CallTool(ctx, "post_task", args)
	if err != nil {
		return nil, err
	}
	var pr PostResult
	if err := json.Unmarshal([]byte(out), &pr); err != nil {
		return nil, fmt.Errorf("parse post_task result %q: %w", trunc(out, 200), err)
	}
	if pr.ID == "" {
		return nil, fmt.Errorf("post_task returned no id: %q", trunc(out, 200))
	}
	return &pr, nil
}

// get_artifact 输出：{found, artifact_key, content, updated_at}
type Artifact struct {
	Found       bool   `json:"found"`
	ArtifactKey string `json:"artifact_key"`
	Content     string `json:"content"`
	UpdatedAt   string `json:"updated_at"`
}

func (b *Bus) GetArtifact(ctx context.Context, key string) (*Artifact, error) {
	out, err := b.CallTool(ctx, "get_artifact", map[string]any{"artifact_key": key})
	if err != nil {
		return nil, err
	}
	var a Artifact
	if err := json.Unmarshal([]byte(out), &a); err != nil {
		return nil, fmt.Errorf("parse get_artifact result: %w", err)
	}
	return &a, nil
}

// bus://state 里的任务行。故意不声明 payload：state 里每行都带完整 payload，
// 网关不需要它，不声明则 json 解析时直接跳过，不分配内存。
// session_id、lease_expires 为空时会被 bus 省略，缺失即得到空字符串。
type Task struct {
	ID           string `json:"id"`
	Title        string `json:"title"`
	TargetRole   string `json:"target_role"`
	CreatedBy    string `json:"created_by"`
	ClaimedBy    string `json:"claimed_by"`
	Status       string `json:"status"` // pending / in_progress / completed / failed
	Summary      string `json:"summary"`
	ArtifactKey  string `json:"artifact_key"`
	SessionID    string `json:"session_id"`
	LeaseExpires string `json:"lease_expires"`
	UpdatedAt    string `json:"updated_at"`
}

const (
	stPending    = "pending"
	stInProgress = "in_progress"
	stCompleted  = "completed"
	stFailed     = "failed"
)

func terminal(st string) bool { return st == stCompleted || st == stFailed }

// 格式：{"tasks":[...], "artifact_keys":[...]}；artifact_keys 网关用不到，忽略。
func parseState(s string) ([]Task, error) {
	var st struct {
		Tasks []Task `json:"tasks"`
	}
	if err := json.NewDecoder(strings.NewReader(s)).Decode(&st); err != nil {
		return nil, fmt.Errorf("parse bus://state: %w", err)
	}
	return st.Tasks, nil
}

/* ============ 3. Hub：全局唯一轮询协程，按任务 id 分发 ============ */

type Hub struct {
	bus  *Bus
	mu   sync.Mutex
	w    map[string]chan Task
	t0   map[string]time.Time
	last map[string]Task // 每个在等任务最近一次看到的状态，用于超时诊断
}

func NewHub(b *Bus) *Hub {
	return &Hub{bus: b, w: map[string]chan Task{}, t0: map[string]time.Time{}, last: map[string]Task{}}
}

func (h *Hub) Watch(id string) <-chan Task {
	ch := make(chan Task, 1)
	h.mu.Lock()
	h.w[id], h.t0[id] = ch, time.Now()
	h.mu.Unlock()
	return ch
}

func (h *Hub) Last(id string) (Task, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	t, ok := h.last[id]
	return t, ok
}

func (h *Hub) Unwatch(id string) {
	h.mu.Lock()
	delete(h.w, id)
	delete(h.t0, id)
	delete(h.last, id)
	h.mu.Unlock()
}

func (h *Hub) Run(ctx context.Context) {
	var lastWarn time.Time
	for ctx.Err() == nil {
		h.mu.Lock()
		n := len(h.w)
		var oldest time.Time
		for _, t := range h.t0 {
			if oldest.IsZero() || t.Before(oldest) {
				oldest = t
			}
		}
		h.mu.Unlock()

		if n == 0 {
			time.Sleep(500 * time.Millisecond)
			continue
		}

		txt, err := h.bus.ReadState(ctx)
		if err != nil {
			log.Println("poll state:", err)
		} else {
			if len(txt) > 2<<20 && time.Since(lastWarn) > time.Minute {
				log.Printf("WARN bus://state 已达 %d 字节，建议在 bus 端裁剪", len(txt))
				lastWarn = time.Now()
			}
			tasks, perr := parseState(txt)
			if perr != nil {
				log.Println(perr)
			}
			h.mu.Lock()
			for _, t := range tasks {
				ch, ok := h.w[t.ID]
				if !ok {
					continue
				}
				h.last[t.ID] = t
				if terminal(t.Status) {
					ch <- t // 缓冲为 1，不会阻塞
					delete(h.w, t.ID)
					delete(h.t0, t.ID)
					delete(h.last, t.ID)
				}
			}
			h.mu.Unlock()
		}

		age := time.Since(oldest)
		switch {
		case age < 15*time.Second:
			time.Sleep(time.Second)
		case age < 60*time.Second:
			time.Sleep(2 * time.Second)
		default:
			time.Sleep(5 * time.Second)
		}
	}
}

/* ============ 4. Gateway：选角色 → 派发 → 等待 → 整理 ============ */

type Turn struct{ Role, Text string } // "user" / "assistant"

type Job struct {
	ID, TaskID, Status, Reply string // Status: running / done / failed / timeout
	sessionID                string // owning web session; never exposed in API responses
}

type Gateway struct {
	bus     *Bus
	hub     *Hub
	mu      sync.Mutex
	hist    map[string][]Turn
	jobs    map[string]*Job
	running map[string]int
}

var roleTimeout = map[string]time.Duration{
	"deepseek": 90 * time.Second, "researcher": 300 * time.Second, "coder": 900 * time.Second, "claude": 300 * time.Second,
}

func rid() string { b := make([]byte, 8); rand.Read(b); return hex.EncodeToString(b) }

var unsafeKey = regexp.MustCompile(`[^a-zA-Z0-9_-]+`)

func safeKey(s string) string { return unsafeKey.ReplaceAllString(s, "-") }

func pickRole(text string) (string, string) {
	switch {
	case strings.HasPrefix(text, "/claude "):
		return "claude", strings.TrimPrefix(text, "/claude ")
	case strings.HasPrefix(text, "/code "):
		return "coder", strings.TrimPrefix(text, "/code ")
	case strings.HasPrefix(text, "/research "):
		return "researcher", strings.TrimPrefix(text, "/research ")
	}
	return "deepseek", text
}

func (g *Gateway) prompt(sid, msg string) string {
	g.mu.Lock()
	defer g.mu.Unlock()
	h := g.hist[sid]
	if len(h) > 12 { // 最近 6 轮
		h = h[len(h)-12:]
	}
	var sb strings.Builder
	if len(h) > 0 {
		sb.WriteString("以下是此前的对话历史，仅供参考：\n")
		for _, t := range h {
			sb.WriteString(t.Role + ": " + t.Text + "\n")
		}
		sb.WriteString("\n当前用户消息：\n")
	}
	sb.WriteString(msg)
	return sb.String()
}

func (g *Gateway) Job(id string) *Job {
	g.mu.Lock()
	defer g.mu.Unlock()
	if j := g.jobs[id]; j != nil {
		c := *j
		return &c
	}
	return nil
}

// JobForSession returns a job only to the session that created it. A nil
// result intentionally covers both unknown IDs and jobs owned by another
// session, preventing job-ID enumeration across authenticated sessions.
func (g *Gateway) JobForSession(id, sid string) *Job {
	g.mu.Lock()
	defer g.mu.Unlock()
	j := g.jobs[id]
	if j == nil || sid == "" || j.sessionID != sid {
		return nil
	}
	c := *j
	return &c
}

func (g *Gateway) set(j *Job, status, reply string) {
	g.mu.Lock()
	j.Status, j.Reply = status, reply
	g.mu.Unlock()
}

func (g *Gateway) Submit(sid, text string, done func(Job)) (string, error) {
	text = strings.TrimSpace(text)
	if text == "" || len([]rune(text)) > 4000 {
		return "", fmt.Errorf("消息为空或过长（上限 4000 字）")
	}
	g.mu.Lock()
	if g.running[sid] >= 2 {
		g.mu.Unlock()
		return "", fmt.Errorf("还有任务在处理中，请稍后再发")
	}
	g.running[sid]++
	j := &Job{ID: rid(), Status: "running", sessionID: sid}
	g.jobs[j.ID] = j
	g.mu.Unlock()

	role, msg := pickRole(text)
	go func() {
		defer func() { g.mu.Lock(); g.running[sid]--; g.mu.Unlock() }()
		g.run(sid, role, msg, j)
		if done != nil {
			done(*g.Job(j.ID))
		}
	}()
	return j.ID, nil
}

func (g *Gateway) run(sid, role, msg string, j *Job) {
	ctx := context.Background()
	akey := "chat-" + safeKey(sid) + "-" + rid() // 预设 artifact_key，worker 的 full_content 会存到这里
	title := cut(strings.ReplaceAll(msg, "\n", " "), 60)

	pr, err := g.bus.PostTask(ctx, sid, role, title, g.prompt(sid, msg), akey)
	if err != nil {
		log.Println("post_task failed:", err)
		g.set(j, "failed", "任务派发失败，请稍后再试。")
		return
	}
	g.mu.Lock()
	j.TaskID = pr.ID
	g.mu.Unlock()
	ch := g.hub.Watch(pr.ID) // 先 post 再 watch：state 保留终态，不会漏

	to := roleTimeout[role]
	if to == 0 {
		to = 120 * time.Second
	}
	select {
	case t := <-ch:
		if t.Status == stFailed {
			g.set(j, "failed", "任务失败："+trunc(t.Summary, 500))
			return
		}
		reply := t.Summary
		key := akey
		if t.ArtifactKey != "" {
			key = t.ArtifactKey
		}
		if a, err := g.bus.GetArtifact(ctx, key); err != nil {
			log.Println("get_artifact failed:", pr.ID, err)
		} else if a.Found && strings.TrimSpace(a.Content) != "" {
			reply = a.Content
		}
		if strings.TrimSpace(reply) == "" {
			reply = "（worker 完成了任务，但没有返回内容）"
		}
		g.mu.Lock()
		g.hist[sid] = append(g.hist[sid], Turn{"user", msg}, Turn{"assistant", trunc(reply, 1500)})
		g.mu.Unlock()
		g.set(j, "done", trunc(reply, 12000))
	case <-time.After(to):
		last, seen := g.hub.Last(pr.ID)
		g.hub.Unwatch(pr.ID)
		g.set(j, "timeout", timeoutMsg(pr.ID, role, last, seen))
	}
}

func timeoutMsg(id, role string, t Task, seen bool) string {
	switch {
	case seen && t.Status == stPending:
		return fmt.Sprintf("任务已派发但没有 %s worker 认领（task=%s），worker 可能离线。", role, id)
	case seen && t.Status == stInProgress:
		return fmt.Sprintf("worker（%s）已认领但超时仍未完成（task=%s），可能任务较重。", t.ClaimedBy, id)
	default:
		return fmt.Sprintf("等待超时（task=%s），暂时无法确认任务状态。", id)
	}
}

func trunc(s string, n int) string {
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return string(r[:n]) + "…（内容过长已截断）"
}

func cut(s string, n int) string {
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return string(r[:n])
}

/* ============ 5. Telegram 接入（v1 未启用） ============ */

// v1 为 web-only：Telegram 接入已移除（无 Bot Token）。
// 结构保留，后续拿到 BotFather token 后可在此处加回长轮询 + 白名单接入。

/* ============ 6. 网页对话接口 ============ */

type Web struct {
	g        *Gateway
	code     string
	mu       sync.Mutex
	sessions map[string]bool
}

// 返回的 session_id 是 cookie 的哈希，不能直接用 cookie 值：
// session_id 会写进 bus 的任务行和 artifact_key，worker 和日志都看得到。
func (w *Web) sid(r *http.Request) (string, bool) {
	c, err := r.Cookie("sid")
	if err != nil {
		return "", false
	}
	w.mu.Lock()
	ok := w.sessions[c.Value]
	w.mu.Unlock()
	if !ok {
		return "", false
	}
	h := sha256.Sum256([]byte(c.Value))
	return "web:" + hex.EncodeToString(h[:])[:16], true
}

func writeJSON(rw http.ResponseWriter, code int, v any) {
	rw.Header().Set("Content-Type", "application/json")
	rw.WriteHeader(code)
	json.NewEncoder(rw).Encode(v)
}

func (w *Web) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /{$}", func(rw http.ResponseWriter, r *http.Request) {
		rw.Header().Set("Content-Type", "text/html; charset=utf-8")
		fmt.Fprint(rw, page)
	})
	mux.HandleFunc("POST /api/login", func(rw http.ResponseWriter, r *http.Request) {
		var in struct{ Code string }
		json.NewDecoder(http.MaxBytesReader(rw, r.Body, 1<<10)).Decode(&in)
		if subtle.ConstantTimeCompare([]byte(in.Code), []byte(w.code)) != 1 {
			time.Sleep(time.Second) // 简单减速爆破；生产请加 IP 限流
			writeJSON(rw, 401, map[string]string{"error": "访问码错误"})
			return
		}
		id := rid() + rid()
		w.mu.Lock()
		w.sessions[id] = true
		w.mu.Unlock()
		http.SetCookie(rw, &http.Cookie{Name: "sid", Value: id, Path: "/", HttpOnly: true,
			Secure: true, SameSite: http.SameSiteLaxMode, MaxAge: 7 * 86400})
		writeJSON(rw, 200, map[string]string{"ok": "1"})
	})
	mux.HandleFunc("POST /api/chat", func(rw http.ResponseWriter, r *http.Request) {
		sid, ok := w.sid(r)
		if !ok {
			writeJSON(rw, 401, map[string]string{"error": "未登录"})
			return
		}
		var in struct{ Message string }
		json.NewDecoder(http.MaxBytesReader(rw, r.Body, 32<<10)).Decode(&in)
		id, err := w.g.Submit(sid, in.Message, nil)
		if err != nil {
			writeJSON(rw, 429, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(rw, 200, map[string]string{"job_id": id})
	})
	mux.HandleFunc("GET /api/jobs/{id}", func(rw http.ResponseWriter, r *http.Request) {
		sid, ok := w.sid(r)
		if !ok {
			writeJSON(rw, 401, map[string]string{"error": "未登录"})
			return
		}
		j := w.g.JobForSession(r.PathValue("id"), sid)
		if j == nil {
			writeJSON(rw, 404, map[string]string{"error": "not found"})
			return
		}
		writeJSON(rw, 200, map[string]string{"status": j.Status, "reply": j.Reply})
	})
	return mux
}

const page = `<!doctype html><meta charset=utf-8><meta name=viewport content="width=device-width,initial-scale=1">
<title>Chat</title><body style="font:16px sans-serif;max-width:720px;margin:2em auto;padding:0 1em">
<div id=log></div><p><input id=m style="width:80%" placeholder="输入消息，/claude /code /research 可指定角色">
<button onclick=send()>发送</button>
<script>
const log=document.getElementById('log'),m=document.getElementById('m');
function add(who,t){const d=document.createElement('p');d.textContent=who+': '+t;log.appendChild(d);return d}
async function api(p,b){const r=await fetch(p,b?{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(b)}:{});return [r.status,await r.json()]}
async function send(){
  const t=m.value.trim();if(!t)return;m.value='';add('我',t);
  let [s,d]=await api('/api/chat',{message:t});
  if(s==401){const c=prompt('访问码');let [s2]=await api('/api/login',{code:c});if(s2!=200)return add('系统','登录失败');[s,d]=await api('/api/chat',{message:t})}
  if(s!=200)return add('系统',d.error);
  const w=add('助手','思考中…');
  for(;;){await new Promise(r=>setTimeout(r,1500));const [,j]=await api('/api/jobs/'+d.job_id);
    if(j.status!='running'){w.textContent='助手: '+j.reply;break}}
}
m.addEventListener('keydown',e=>{if(e.key==='Enter')send()});
</script>`

/* ============ 7. main ============ */

func main() {
	busURL, busTok, webCode := os.Getenv("BUS_URL"), os.Getenv("BUS_TOKEN"), os.Getenv("WEB_CODE")
	if busURL == "" || busTok == "" {
		log.Fatal("BUS_URL 和 BUS_TOKEN 必须设置")
	}
	if webCode == "" {
		log.Fatal("WEB_CODE 必须设置（为空会导致任何人都能登录）")
	}

	bus := &Bus{url: busURL, tok: busTok}
	hub := NewHub(bus)
	g := &Gateway{bus: bus, hub: hub, hist: map[string][]Turn{}, jobs: map[string]*Job{}, running: map[string]int{}}
	go hub.Run(context.Background())

	// v1 web-only: no Telegram startup (see section 5 note).

	web := &Web{g: g, code: webCode, sessions: map[string]bool{}}
	addr := os.Getenv("LISTEN")
	if addr == "" {
		addr = "127.0.0.1:9000"
	}
	log.Println("gateway listening on", addr)
	log.Fatal(http.ListenAndServe(addr, web.routes()))
}
