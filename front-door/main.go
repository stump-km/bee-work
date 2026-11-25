package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	_ "embed"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"html"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

/* ================= Bus client (adapted from the working chat gateway) ================= */

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
}

// connect creates a FRESH MCP client session for a single call attempt.
// The gate previously kept one shared session for all clients and all
// calls: sess() only locked session creation, CallTool/ReadState then
// used the session concurrently outside the lock, and any error called
// reset() which closed the shared session — killing every other
// in-flight call (the fox's bus_state reads dropping mid-stream).
// The bus is on localhost and traffic is low, so the per-call handshake
// cost is fine and buys full isolation between calls.
func (b *Bus) connect() (*mcp.ClientSession, error) {
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.ResponseHeaderTimeout = 30 * time.Second
	c := mcp.NewClient(&mcp.Implementation{Name: "front-door-gate", Version: "1.0.0"}, nil)
	return c.Connect(context.Background(), &mcp.StreamableClientTransport{
		Endpoint:   b.url,
		HTTPClient: &http.Client{Transport: authRT{tok: b.tok, base: tr}},
	}, nil)
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

func resultJSON(res *mcp.CallToolResult) string {
	if res.StructuredContent != nil {
		if raw, err := json.Marshal(res.StructuredContent); err == nil {
			return string(raw)
		}
	}
	return resultText(res)
}

// CallTool forwards one tool call to the real bus and returns its
// structured result as JSON text.
func (b *Bus) CallTool(ctx context.Context, name string, args map[string]any) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	var last error
	for i := 0; i < 2; i++ {
		cs, err := b.connect()
		if err != nil {
			last = err
			continue
		}
		res, err := cs.CallTool(ctx, &mcp.CallToolParams{Name: name, Arguments: args})
		_ = cs.Close()
		if err != nil {
			last = err
			continue
		}
		if res.IsError {
			return "", fmt.Errorf("bus tool %s: %s", name, resultText(res))
		}
		return resultJSON(res), nil
	}
	return "", last
}

func (b *Bus) ReadState(ctx context.Context) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	var last error
	for i := 0; i < 2; i++ {
		cs, err := b.connect()
		if err != nil {
			last = err
			continue
		}
		r, err := cs.ReadResource(ctx, &mcp.ReadResourceParams{URI: "bus://state"})
		_ = cs.Close()
		if err != nil {
			last = err
			continue
		}
		if len(r.Contents) == 0 {
			return "", fmt.Errorf("empty bus://state")
		}
		return r.Contents[0].Text, nil
	}
	return "", last
}

// forward marshals a typed tool input, calls the bus, and unmarshals the
// bus's structured output into the typed output value.
func forward[In, Out any](ctx context.Context, b *Bus, name string, in In) (Out, error) {
	var out Out
	raw, err := json.Marshal(in)
	if err != nil {
		return out, err
	}
	var args map[string]any
	if err := json.Unmarshal(raw, &args); err != nil {
		return out, err
	}
	res, err := b.CallTool(ctx, name, args)
	if err != nil {
		return out, err
	}
	if err := json.Unmarshal([]byte(res), &out); err != nil {
		return out, fmt.Errorf("parse bus %s result: %w", name, err)
	}
	return out, nil
}

/* ================= Bus tool schemas (mirror of agent-bus-mcp) ================= */

type Task struct {
	ID           string `json:"id"`
	SessionID    string `json:"session_id,omitempty"`
	Title        string `json:"title"`
	TargetRole   string `json:"target_role"`
	Payload      string `json:"payload"`
	CreatedBy    string `json:"created_by"`
	ClaimedBy    string `json:"claimed_by,omitempty"`
	Status       string `json:"status"`
	Summary      string `json:"summary,omitempty"`
	ArtifactKey  string `json:"artifact_key,omitempty"`
	LeaseExpires string `json:"lease_expires,omitempty"`
	UpdatedAt    string `json:"updated_at"`
}

type PostTaskInput struct {
	SessionID   string `json:"session_id,omitempty" jsonschema:"可选：关联的业务/对话会话 ID"`
	Title       string `json:"title" jsonschema:"任务标题"`
	TargetRole  string `json:"target_role" jsonschema:"负责该任务的角色，例如 researcher、coder、reviewer"`
	Payload     string `json:"payload" jsonschema:"任务详细指令或上下文"`
	ArtifactKey string `json:"artifact_key,omitempty" jsonschema:"可选：任务完成后写入共享产物的键名"`
	CreatedBy   string `json:"created_by,omitempty" jsonschema:"派发者标识，默认 orchestrator"`
}

type PostTaskOutput struct {
	ID         string `json:"id"`
	Status     string `json:"status"`
	TargetRole string `json:"target_role"`
}

type ClaimTaskInput struct {
	Role    string `json:"role" jsonschema:"当前 Agent 承担的角色"`
	AgentID string `json:"agent_id" jsonschema:"当前 Agent 唯一实例 ID"`
}

type ClaimTaskOutput struct {
	Found   bool   `json:"found"`
	Task    *Task  `json:"task,omitempty"`
	Message string `json:"message,omitempty"`
}

type HeartbeatTaskInput struct {
	TaskID  string `json:"task_id" jsonschema:"正在执行的任务 ID"`
	AgentID string `json:"agent_id" jsonschema:"当前认领该任务的 Agent ID"`
}

type HeartbeatTaskOutput struct {
	TaskID       string `json:"task_id"`
	LeaseExpires string `json:"lease_expires"`
}

type CompleteTaskInput struct {
	TaskID      string `json:"task_id" jsonschema:"要更新的任务 ID"`
	AgentID     string `json:"agent_id" jsonschema:"执行该任务的 Agent ID"`
	Summary     string `json:"summary" jsonschema:"简短执行摘要（建议控制在几百字内，避免撑爆下游上下文）"`
	FullContent string `json:"full_content,omitempty" jsonschema:"可选：完整输出/报告/代码正文，存入共享产物供按需读取"`
	ArtifactKey string `json:"artifact_key,omitempty" jsonschema:"可选：存入共享产物的键名（若未填则沿用任务预设键名）"`
	Failed      bool   `json:"failed,omitempty" jsonschema:"若任务失败设为 true，否则视为 completed"`
}

type CompleteTaskOutput struct {
	ID          string `json:"id"`
	Status      string `json:"status"`
	ArtifactKey string `json:"artifact_key,omitempty"`
}

type GetArtifactInput struct {
	ArtifactKey string `json:"artifact_key" jsonschema:"要读取的共享产物键名"`
}

type GetArtifactOutput struct {
	Found       bool   `json:"found"`
	ArtifactKey string `json:"artifact_key"`
	Content     string `json:"content,omitempty"`
	UpdatedAt   string `json:"updated_at,omitempty"`
}

type PingPresenceInput struct {
	AgentID string `json:"agent_id" jsonschema:"当前 Agent 唯一实例 ID"`
	Role    string `json:"role,omitempty" jsonschema:"可选：当前 Agent 承担的角色；为空则保留已登记的角色"`
	Note    string `json:"note,omitempty" jsonschema:"可选：状态备注，如 idle、busy、offline；为空则保留已有备注"`
}

type PingPresenceOutput struct {
	AgentID  string `json:"agent_id"`
	LastSeen string `json:"last_seen"`
}

type AgentPresence struct {
	AgentID  string `json:"agent_id"`
	Role     string `json:"role,omitempty"`
	Note     string `json:"note,omitempty"`
	LastSeen string `json:"last_seen"`
	Alive    bool   `json:"alive"`
	// Tier and EmptyStrikes are computed by the bus core (revamp plan
	// S1 three-tier presence; gap-4 empty-completion strikes) and are
	// passed through verbatim so gate clients see the same board.
	Tier         string `json:"tier"`
	EmptyStrikes int    `json:"empty_strikes"`
}

type BusStateInput struct{}

type BusStateTask struct {
	ID          string `json:"id"`
	Title       string `json:"title"`
	TargetRole  string `json:"target_role"`
	Status      string `json:"status"`
	ClaimedBy   string `json:"claimed_by,omitempty"`
	ArtifactKey string `json:"artifact_key,omitempty"`
	UpdatedAt   string `json:"updated_at"`
}

type BusStateOutput struct {
	Total        int             `json:"total"`
	Counts       map[string]int  `json:"counts"`
	Recent       []BusStateTask  `json:"recent"`
	ArtifactKeys []string        `json:"artifact_keys"`
	Agents       []AgentPresence `json:"agents"`
}

/* ================= Client registry ================= */

type Client struct {
	Name             string   `json:"name"`
	TokenHash        string   `json:"token_hash"`
	PathSecretHash   string   `json:"path_secret_hash,omitempty"`
	Role             string   `json:"role"`
	AllowedTools     []string `json:"allowed_tools"`
	AllowedResources []string `json:"allowed_resources"`
	Enabled          *bool    `json:"enabled,omitempty"`
}

func (c Client) IsEnabled() bool { return c.Enabled == nil || *c.Enabled }

func (c Client) allowsTool(name string) bool {
	for _, t := range c.AllowedTools {
		if t == name {
			return true
		}
	}
	return false
}

func (c Client) allowsResource(uri string) bool {
	for _, r := range c.AllowedResources {
		if r == uri {
			return true
		}
	}
	return false
}

type Registry struct {
	Clients []Client `json:"clients"`
}

type RegistryStore struct {
	path string
}

func hashToken(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}

func (s *RegistryStore) Load() (Registry, error) {
	var reg Registry
	data, err := os.ReadFile(s.path)
	if errors.Is(err, os.ErrNotExist) {
		return reg, nil
	}
	if err != nil {
		return reg, err
	}
	if len(bytes.TrimSpace(data)) == 0 {
		return reg, nil
	}
	if err := json.Unmarshal(data, &reg); err != nil {
		return reg, fmt.Errorf("parse registry: %w", err)
	}
	return reg, nil
}

// Lookup resolves a plaintext client token to a registry client by hash.
// The plaintext is never stored or logged.
func (s *RegistryStore) Lookup(token string) (Client, bool) {
	if token == "" {
		return Client{}, false
	}
	reg, err := s.Load()
	if err != nil {
		log.Printf("registry load error: %v", err)
		return Client{}, false
	}
	want := hashToken(token)
	for _, c := range reg.Clients {
		if !c.IsEnabled() {
			continue
		}
		if subtle.ConstantTimeCompare([]byte(strings.ToLower(c.TokenHash)), []byte(want)) == 1 {
			return c, true
		}
	}
	return Client{}, false
}

// LookupPathSecret resolves an unguessable URL path secret to a registry
// client by hash. The plaintext secret is never stored or logged, and a
// path secret is NOT a bearer token: Lookup above never matches it.
func (s *RegistryStore) LookupPathSecret(secret string) (Client, bool) {
	if secret == "" {
		return Client{}, false
	}
	reg, err := s.Load()
	if err != nil {
		log.Printf("registry load error: %v", err)
		return Client{}, false
	}
	want := hashToken(secret)
	for _, c := range reg.Clients {
		if !c.IsEnabled() || strings.TrimSpace(c.PathSecretHash) == "" {
			continue
		}
		if subtle.ConstantTimeCompare([]byte(strings.ToLower(c.PathSecretHash)), []byte(want)) == 1 {
			return c, true
		}
	}
	return Client{}, false
}

func (s *RegistryStore) Get(name string) (Client, bool) {
	reg, err := s.Load()
	if err != nil {
		log.Printf("registry load error: %v", err)
		return Client{}, false
	}
	for _, c := range reg.Clients {
		if c.Name == name && c.IsEnabled() {
			return c, true
		}
	}
	return Client{}, false
}

// OAuthProfiles lists enabled registry identities that carry no static
// token; these are the identities an OAuth client can be bound to at
// owner consent time.
func (s *RegistryStore) OAuthProfiles() []Client {
	reg, err := s.Load()
	if err != nil {
		log.Printf("registry load error: %v", err)
		return nil
	}
	var out []Client
	for _, c := range reg.Clients {
		if c.IsEnabled() && strings.TrimSpace(c.TokenHash) == "" && strings.TrimSpace(c.PathSecretHash) == "" {
			out = append(out, c)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

func (s *RegistryStore) Upsert(c Client) error {
	reg, err := s.Load()
	if err != nil {
		return err
	}
	found := false
	for i := range reg.Clients {
		if reg.Clients[i].Name == c.Name {
			reg.Clients[i] = c
			found = true
			break
		}
	}
	if !found {
		reg.Clients = append(reg.Clients, c)
	}
	sort.Slice(reg.Clients, func(i, j int) bool { return reg.Clients[i].Name < reg.Clients[j].Name })
	data, err := json.MarshalIndent(reg, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	if dir := filepath.Dir(s.path); dir != "" {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return err
		}
	}
	tmp := s.path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, s.path)
}

/* ================= Audit log (never secrets) ================= */

type Audit struct {
	mu  sync.Mutex
	out io.Writer
}

func NewAudit(path string) (*Audit, error) {
	if path == "" {
		return &Audit{out: os.Stdout}, nil
	}
	if dir := filepath.Dir(path); dir != "" {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return nil, err
		}
	}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, err
	}
	return &Audit{out: io.MultiWriter(f, os.Stdout)}, nil
}

func (a *Audit) Log(client, method, tool, path string, status int) {
	a.mu.Lock()
	defer a.mu.Unlock()
	fmt.Fprintf(a.out, "%s client=%s method=%s tool=%s path=%s status=%d\n",
		time.Now().UTC().Format(time.RFC3339), client, method, tool, path, status)
}

/* ================= OAuth 2.1 authorization server =================
   The gate is both authorization server and resource server. All codes
   and tokens are opaque random strings; only SHA-256 hashes are stored.
   Dynamically registered clients start PENDING with zero privileges:
   they can only obtain tokens after the owner, authenticated by the
   owner passcode (whose hash alone is configured), binds the client to
   a registry identity on the consent page. */

func randomString(n int) (string, error) {
	raw := make([]byte, n)
	if _, err := rand.Read(raw); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(raw), nil
}

func pkceChallenge(verifier string) string {
	sum := sha256.Sum256([]byte(verifier))
	return base64.RawURLEncoding.EncodeToString(sum[:])
}

type OAuthClient struct {
	ID                      string    `json:"client_id"`
	SecretHash              string    `json:"client_secret_hash,omitempty"`
	ClientName              string    `json:"client_name,omitempty"`
	RedirectURIs            []string  `json:"redirect_uris"`
	GrantTypes              []string  `json:"grant_types"`
	ResponseTypes           []string  `json:"response_types"`
	TokenEndpointAuthMethod string    `json:"token_endpoint_auth_method"`
	Scope                   string    `json:"scope,omitempty"`
	Status                  string    `json:"status"` // pending | bound
	BoundClient             string    `json:"bound_client,omitempty"`
	CreatedAt               time.Time `json:"created_at"`
}

type AuthRequest struct {
	IDHash        string    `json:"id_hash"`
	OAuthClientID string    `json:"oauth_client_id"`
	RedirectURI   string    `json:"redirect_uri"`
	State         string    `json:"state,omitempty"`
	CodeChallenge string    `json:"code_challenge"`
	Scope         string    `json:"scope,omitempty"`
	Resource      string    `json:"resource,omitempty"`
	ExpiresAt     time.Time `json:"expires_at"`
	Used          bool      `json:"used"`
}

type AuthCode struct {
	Hash          string    `json:"hash"`
	OAuthClientID string    `json:"oauth_client_id"`
	BoundClient   string    `json:"bound_client"`
	RedirectURI   string    `json:"redirect_uri"`
	CodeChallenge string    `json:"code_challenge"`
	Scope         string    `json:"scope,omitempty"`
	Resource      string    `json:"resource,omitempty"`
	ExpiresAt     time.Time `json:"expires_at"`
	Used          bool      `json:"used"`
}

type TokenRecord struct {
	Family           string    `json:"family"`
	OAuthClientID    string    `json:"oauth_client_id"`
	BoundClient      string    `json:"bound_client"`
	AccessHash       string    `json:"access_hash"`
	RefreshHash      string    `json:"refresh_hash"`
	Scope            string    `json:"scope,omitempty"`
	Resource         string    `json:"resource,omitempty"`
	AccessExpiresAt  time.Time `json:"access_expires_at"`
	RefreshExpiresAt time.Time `json:"refresh_expires_at"`
	Revoked          bool      `json:"revoked"`
}

type OAuthData struct {
	Clients  []OAuthClient `json:"clients"`
	Requests []AuthRequest `json:"requests"`
	Codes    []AuthCode    `json:"codes"`
	Tokens   []TokenRecord `json:"tokens"`
}

type OAuthStore struct {
	path string
	mu   sync.Mutex
	data OAuthData
}

func NewOAuthStore(path string) (*OAuthStore, error) {
	s := &OAuthStore{path: path}
	raw, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return s, nil
	}
	if err != nil {
		return nil, err
	}
	if len(bytes.TrimSpace(raw)) > 0 {
		if err := json.Unmarshal(raw, &s.data); err != nil {
			return nil, fmt.Errorf("parse oauth store: %w", err)
		}
	}
	return s, nil
}

func (s *OAuthStore) saveLocked() error {
	now := time.Now()
	reqs := s.data.Requests[:0]
	for _, r := range s.data.Requests {
		if !r.Used && now.Before(r.ExpiresAt) {
			reqs = append(reqs, r)
		}
	}
	s.data.Requests = reqs
	codes := s.data.Codes[:0]
	for _, c := range s.data.Codes {
		if !c.Used && now.Before(c.ExpiresAt) {
			codes = append(codes, c)
		}
	}
	s.data.Codes = codes
	toks := s.data.Tokens[:0]
	for _, t := range s.data.Tokens {
		if now.Before(t.RefreshExpiresAt) {
			toks = append(toks, t)
		}
	}
	s.data.Tokens = toks

	raw, err := json.MarshalIndent(s.data, "", "  ")
	if err != nil {
		return err
	}
	raw = append(raw, '\n')
	if dir := filepath.Dir(s.path); dir != "" {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return err
		}
	}
	tmp := s.path + ".tmp"
	if err := os.WriteFile(tmp, raw, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, s.path)
}

func (s *OAuthStore) findClientLocked(id string) *OAuthClient {
	for i := range s.data.Clients {
		if s.data.Clients[i].ID == id {
			return &s.data.Clients[i]
		}
	}
	return nil
}

func (s *OAuthStore) GetClient(id string) (OAuthClient, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if c := s.findClientLocked(id); c != nil {
		return *c, true
	}
	return OAuthClient{}, false
}

type registerRequest struct {
	ClientName              string   `json:"client_name"`
	RedirectURIs            []string `json:"redirect_uris"`
	GrantTypes              []string `json:"grant_types"`
	ResponseTypes           []string `json:"response_types"`
	TokenEndpointAuthMethod string   `json:"token_endpoint_auth_method"`
	Scope                   string   `json:"scope"`
}

func (s *OAuthStore) Register(req registerRequest) (OAuthClient, string, error) {
	idRaw := make([]byte, 16)
	if _, err := rand.Read(idRaw); err != nil {
		return OAuthClient{}, "", err
	}
	c := OAuthClient{
		ID:                      "fd_" + hex.EncodeToString(idRaw),
		ClientName:              req.ClientName,
		RedirectURIs:            req.RedirectURIs,
		GrantTypes:              req.GrantTypes,
		ResponseTypes:           req.ResponseTypes,
		TokenEndpointAuthMethod: req.TokenEndpointAuthMethod,
		Scope:                   req.Scope,
		Status:                  "pending",
		CreatedAt:               time.Now().UTC(),
	}
	var secret string
	if req.TokenEndpointAuthMethod == "client_secret_basic" || req.TokenEndpointAuthMethod == "client_secret_post" {
		var err error
		secret, err = randomString(32)
		if err != nil {
			return OAuthClient{}, "", err
		}
		c.SecretHash = hashToken(secret)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data.Clients = append(s.data.Clients, c)
	if err := s.saveLocked(); err != nil {
		return OAuthClient{}, "", err
	}
	return c, secret, nil
}

func (s *OAuthStore) CreateRequest(clientID, redirectURI, state, challenge, scope, resource string) (string, error) {
	id, err := randomString(24)
	if err != nil {
		return "", err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data.Requests = append(s.data.Requests, AuthRequest{
		IDHash:        hashToken(id),
		OAuthClientID: clientID,
		RedirectURI:   redirectURI,
		State:         state,
		CodeChallenge: challenge,
		Scope:         scope,
		Resource:      resource,
		ExpiresAt:     time.Now().Add(10 * time.Minute),
	})
	if err := s.saveLocked(); err != nil {
		return "", err
	}
	return id, nil
}

func (s *OAuthStore) GetRequest(id string) (AuthRequest, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	h := hashToken(id)
	for _, r := range s.data.Requests {
		if r.IDHash == h && !r.Used && time.Now().Before(r.ExpiresAt) {
			return r, true
		}
	}
	return AuthRequest{}, false
}

func (s *OAuthStore) ConsumeRequest(id string) (AuthRequest, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	h := hashToken(id)
	for i := range s.data.Requests {
		r := &s.data.Requests[i]
		if r.IDHash == h && !r.Used && time.Now().Before(r.ExpiresAt) {
			r.Used = true
			out := *r
			_ = s.saveLocked()
			return out, true
		}
	}
	return AuthRequest{}, false
}

// BindAndCode consumes the authorization request, binds the OAuth client
// to the chosen registry identity, and issues a single-use code.
func (s *OAuthStore) BindAndCode(id, boundClient string) (string, AuthRequest, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	h := hashToken(id)
	var req *AuthRequest
	for i := range s.data.Requests {
		if s.data.Requests[i].IDHash == h && !s.data.Requests[i].Used && time.Now().Before(s.data.Requests[i].ExpiresAt) {
			req = &s.data.Requests[i]
			break
		}
	}
	if req == nil {
		return "", AuthRequest{}, errors.New("authorization request expired or already used")
	}
	client := s.findClientLocked(req.OAuthClientID)
	if client == nil {
		return "", AuthRequest{}, errors.New("oauth client not found")
	}
	code, err := randomString(32)
	if err != nil {
		return "", AuthRequest{}, err
	}
	client.Status = "bound"
	client.BoundClient = boundClient
	req.Used = true
	s.data.Codes = append(s.data.Codes, AuthCode{
		Hash:          hashToken(code),
		OAuthClientID: client.ID,
		BoundClient:   boundClient,
		RedirectURI:   req.RedirectURI,
		CodeChallenge: req.CodeChallenge,
		Scope:         req.Scope,
		Resource:      req.Resource,
		ExpiresAt:     time.Now().Add(10 * time.Minute),
	})
	out := *req
	if err := s.saveLocked(); err != nil {
		return "", AuthRequest{}, err
	}
	return code, out, nil
}

func (s *OAuthStore) checkClientSecretLocked(client *OAuthClient, secret string) bool {
	if client.SecretHash == "" {
		return true
	}
	if secret == "" {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(strings.ToLower(client.SecretHash)), []byte(hashToken(secret))) == 1
}

func (s *OAuthStore) issueLocked(client *OAuthClient, scope, resource, family string) (access, refresh string, rec TokenRecord, err error) {
	access, err = randomString(32)
	if err != nil {
		return "", "", TokenRecord{}, err
	}
	refresh, err = randomString(32)
	if err != nil {
		return "", "", TokenRecord{}, err
	}
	if family == "" {
		fam, ferr := randomString(12)
		if ferr != nil {
			return "", "", TokenRecord{}, ferr
		}
		family = fam
	}
	now := time.Now()
	rec = TokenRecord{
		Family:           family,
		OAuthClientID:    client.ID,
		BoundClient:      client.BoundClient,
		AccessHash:       hashToken(access),
		RefreshHash:      hashToken(refresh),
		Scope:            scope,
		Resource:         resource,
		AccessExpiresAt:  now.Add(time.Hour),
		RefreshExpiresAt: now.Add(30 * 24 * time.Hour),
	}
	s.data.Tokens = append(s.data.Tokens, rec)
	return access, refresh, rec, nil
}

func (s *OAuthStore) ExchangeCode(clientID, clientSecret, code, redirectURI, verifier, resource string) (access, refresh string, rec TokenRecord, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	client := s.findClientLocked(clientID)
	if client == nil || client.Status != "bound" || client.BoundClient == "" {
		return "", "", TokenRecord{}, errors.New("invalid_client")
	}
	if !s.checkClientSecretLocked(client, clientSecret) {
		return "", "", TokenRecord{}, errors.New("invalid_client")
	}
	h := hashToken(code)
	var ac *AuthCode
	for i := range s.data.Codes {
		if s.data.Codes[i].Hash == h {
			ac = &s.data.Codes[i]
			break
		}
	}
	if ac == nil || ac.Used || time.Now().After(ac.ExpiresAt) {
		return "", "", TokenRecord{}, errors.New("invalid_grant")
	}
	if ac.OAuthClientID != clientID || ac.RedirectURI != redirectURI || ac.BoundClient != client.BoundClient {
		return "", "", TokenRecord{}, errors.New("invalid_grant")
	}
	if verifier == "" || pkceChallenge(verifier) != ac.CodeChallenge {
		return "", "", TokenRecord{}, errors.New("invalid_grant")
	}
	if resource != "" && ac.Resource != "" && resource != ac.Resource {
		return "", "", TokenRecord{}, errors.New("invalid_target")
	}
	ac.Used = true
	res := ac.Resource
	if res == "" {
		res = resource
	}
	access, refresh, rec, err = s.issueLocked(client, ac.Scope, res, "")
	if err != nil {
		return "", "", TokenRecord{}, err
	}
	if err := s.saveLocked(); err != nil {
		return "", "", TokenRecord{}, err
	}
	return access, refresh, rec, nil
}

func (s *OAuthStore) Refresh(clientID, clientSecret, refreshToken string) (access, refresh string, rec TokenRecord, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	client := s.findClientLocked(clientID)
	if client == nil || client.Status != "bound" || client.BoundClient == "" {
		return "", "", TokenRecord{}, errors.New("invalid_client")
	}
	if !s.checkClientSecretLocked(client, clientSecret) {
		return "", "", TokenRecord{}, errors.New("invalid_client")
	}
	h := hashToken(refreshToken)
	var old *TokenRecord
	for i := range s.data.Tokens {
		if s.data.Tokens[i].RefreshHash == h {
			old = &s.data.Tokens[i]
			break
		}
	}
	if old == nil || old.Revoked || time.Now().After(old.RefreshExpiresAt) {
		return "", "", TokenRecord{}, errors.New("invalid_grant")
	}
	if old.OAuthClientID != clientID || old.BoundClient != client.BoundClient {
		return "", "", TokenRecord{}, errors.New("invalid_grant")
	}
	family, scope, resource := old.Family, old.Scope, old.Resource
	old.Revoked = true
	access, refresh, rec, err = s.issueLocked(client, scope, resource, family)
	if err != nil {
		return "", "", TokenRecord{}, err
	}
	if err := s.saveLocked(); err != nil {
		return "", "", TokenRecord{}, err
	}
	return access, refresh, rec, nil
}

// ResolveAccess maps an OAuth access token to its bound registry client
// name. Revoking the OAuth client binding or the token record kills it.
func (s *OAuthStore) ResolveAccess(token string) (string, bool) {
	if token == "" {
		return "", false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	h := hashToken(token)
	now := time.Now()
	for _, t := range s.data.Tokens {
		if t.AccessHash != h || t.Revoked || now.After(t.AccessExpiresAt) {
			continue
		}
		client := s.findClientLocked(t.OAuthClientID)
		if client == nil || client.Status != "bound" || client.BoundClient != t.BoundClient {
			return "", false
		}
		return t.BoundClient, true
	}
	return "", false
}

/* ---- OAuth HTTP handlers ---- */

func (g *Gate) publicBase() string {
	if g.publicBaseURL != "" {
		return g.publicBaseURL
	}
	return "http://127.0.0.1:8585"
}

func (g *Gate) publicResource() string { return g.publicBase() + "/mcp" }

func (g *Gate) wwwAuthenticate() string {
	return fmt.Sprintf(`Bearer resource_metadata="%s/.well-known/oauth-protected-resource", scope="bus"`, g.publicBase())
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func oauthError(w http.ResponseWriter, status int, code, desc string) {
	writeJSON(w, status, map[string]any{"error": code, "error_description": desc})
}

func (g *Gate) handleProtectedResourceMetadata(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		oauthError(w, http.StatusMethodNotAllowed, "invalid_request", "GET only")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"resource":                 g.publicResource(),
		"authorization_servers":    []string{g.publicBase()},
		"bearer_methods_supported": []string{"header"},
		"scopes_supported":         []string{"bus"},
		"resource_name":            "agent-bus front door",
	})
}

func (g *Gate) handleAuthServerMetadata(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		oauthError(w, http.StatusMethodNotAllowed, "invalid_request", "GET only")
		return
	}
	base := g.publicBase()
	writeJSON(w, http.StatusOK, map[string]any{
		"issuer":                                base,
		"authorization_endpoint":                base + "/authorize",
		"token_endpoint":                        base + "/token",
		"registration_endpoint":                 base + "/register",
		"response_types_supported":              []string{"code"},
		"grant_types_supported":                 []string{"authorization_code", "refresh_token"},
		"code_challenge_methods_supported":      []string{"S256"},
		"token_endpoint_auth_methods_supported": []string{"none", "client_secret_basic", "client_secret_post"},
		"scopes_supported":                      []string{"bus"},
	})
}

func validRedirectURI(raw string) bool {
	u, err := url.Parse(raw)
	if err != nil || u.Scheme == "" || u.Fragment != "" {
		return false
	}
	if u.Scheme == "http" || u.Scheme == "https" {
		return u.Host != ""
	}
	return true // native-app custom schemes etc.
}

func (g *Gate) handleRegister(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		oauthError(w, http.StatusMethodNotAllowed, "invalid_request", "POST only")
		return
	}
	var req registerRequest
	if err := json.NewDecoder(io.LimitReader(r.Body, 64<<10)).Decode(&req); err != nil {
		oauthError(w, http.StatusBadRequest, "invalid_client_metadata", "body must be JSON")
		return
	}
	if len(req.RedirectURIs) == 0 {
		oauthError(w, http.StatusBadRequest, "invalid_redirect_uri", "at least one redirect_uri is required")
		return
	}
	for _, u := range req.RedirectURIs {
		if !validRedirectURI(u) {
			oauthError(w, http.StatusBadRequest, "invalid_redirect_uri", "bad redirect_uri: "+u)
			return
		}
	}
	if len(req.GrantTypes) == 0 {
		req.GrantTypes = []string{"authorization_code", "refresh_token"}
	}
	for _, gt := range req.GrantTypes {
		if gt != "authorization_code" && gt != "refresh_token" {
			oauthError(w, http.StatusBadRequest, "invalid_client_metadata", "unsupported grant_type "+gt)
			return
		}
	}
	if len(req.ResponseTypes) == 0 {
		req.ResponseTypes = []string{"code"}
	}
	for _, rt := range req.ResponseTypes {
		if rt != "code" {
			oauthError(w, http.StatusBadRequest, "invalid_client_metadata", "only response_type=code is supported")
			return
		}
	}
	if req.TokenEndpointAuthMethod == "" {
		req.TokenEndpointAuthMethod = "none"
	}
	switch req.TokenEndpointAuthMethod {
	case "none", "client_secret_basic", "client_secret_post":
	default:
		oauthError(w, http.StatusBadRequest, "invalid_client_metadata", "unsupported token_endpoint_auth_method")
		return
	}
	client, secret, err := g.oauth.Register(req)
	if err != nil {
		log.Printf("oauth register error: %v", err)
		oauthError(w, http.StatusInternalServerError, "server_error", "registration failed")
		return
	}
	log.Printf("oauth client registered id=%s name=%q status=pending", client.ID, client.ClientName)
	resp := map[string]any{
		"client_id":                  client.ID,
		"client_id_issued_at":        client.CreatedAt.Unix(),
		"client_name":                client.ClientName,
		"redirect_uris":              client.RedirectURIs,
		"grant_types":                client.GrantTypes,
		"response_types":             client.ResponseTypes,
		"token_endpoint_auth_method": client.TokenEndpointAuthMethod,
		"scope":                      client.Scope,
		"status":                     "pending",
	}
	if secret != "" {
		resp["client_secret"] = secret
		resp["client_secret_expires_at"] = 0
	}
	writeJSON(w, http.StatusCreated, resp)
}

func redirectWithParams(w http.ResponseWriter, r *http.Request, redirectURI string, params url.Values) {
	u, err := url.Parse(redirectURI)
	if err != nil {
		oauthError(w, http.StatusBadRequest, "invalid_request", "bad redirect_uri")
		return
	}
	q := u.Query()
	for k, vs := range params {
		for _, v := range vs {
			q.Set(k, v)
		}
	}
	u.RawQuery = q.Encode()
	w.Header().Set("Cache-Control", "no-store")
	http.Redirect(w, r, u.String(), http.StatusFound)
}

func (g *Gate) handleAuthorize(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		q := r.URL.Query()
		clientID := q.Get("client_id")
		client, ok := g.oauth.GetClient(clientID)
		if !ok {
			oauthError(w, http.StatusBadRequest, "invalid_client", "unknown client_id")
			return
		}
		redirectURI := q.Get("redirect_uri")
		registered := false
		for _, u := range client.RedirectURIs {
			if u == redirectURI {
				registered = true
			}
		}
		if !registered {
			oauthError(w, http.StatusBadRequest, "invalid_redirect_uri", "redirect_uri is not registered for this client")
			return
		}
		if q.Get("response_type") != "code" {
			oauthError(w, http.StatusBadRequest, "unsupported_response_type", "only response_type=code")
			return
		}
		challenge := q.Get("code_challenge")
		if challenge == "" || q.Get("code_challenge_method") != "S256" {
			oauthError(w, http.StatusBadRequest, "invalid_request", "PKCE with S256 is required")
			return
		}
		if raw, err := base64.RawURLEncoding.DecodeString(challenge); err != nil || len(raw) != 32 {
			oauthError(w, http.StatusBadRequest, "invalid_request", "code_challenge must be a base64url SHA-256 digest")
			return
		}
		scope := q.Get("scope")
		if scope == "" {
			scope = "bus"
		}
		for _, s := range strings.Fields(scope) {
			if s != "bus" {
				oauthError(w, http.StatusBadRequest, "invalid_scope", "only scope 'bus' is available")
				return
			}
		}
		resource := q.Get("resource")
		if resource == "" {
			resource = g.publicResource()
		}
		if resource != g.publicResource() {
			oauthError(w, http.StatusBadRequest, "invalid_target", "resource must be the public /mcp URL")
			return
		}
		tx, err := g.oauth.CreateRequest(clientID, redirectURI, q.Get("state"), challenge, scope, resource)
		if err != nil {
			oauthError(w, http.StatusInternalServerError, "server_error", "could not create authorization request")
			return
		}
		profiles := g.registry.OAuthProfiles()
		var options strings.Builder
		for _, p := range profiles {
			fmt.Fprintf(&options, `<option value="%s">%s (role %s; tools: %s)</option>`,
				html.EscapeString(p.Name), html.EscapeString(p.Name), html.EscapeString(p.Role), html.EscapeString(strings.Join(p.AllowedTools, ", ")))
		}
		if len(profiles) == 0 {
			options.WriteString(`<option value="">(no OAuth profiles configured)</option>`)
		}
		appName := client.ClientName
		if appName == "" {
			appName = client.ID
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Header().Set("Cache-Control", "no-store")
		fmt.Fprintf(w, `<!doctype html><html><head><meta charset="utf-8"><title>Front door consent</title></head><body>
<h1>Connect an app to your agent-bus?</h1>
<p>App: <strong>%s</strong> (client %s)</p>
<p>This app is asking to act through your front door. Choose which identity it binds to, enter the owner passcode, and approve. Until you approve, it has <strong>no access at all</strong>.</p>
<form method="post" action="/authorize">
<input type="hidden" name="tx" value="%s">
<label>Bind to identity: <select name="bound_client">%s</select></label><br><br>
<label>Owner passcode: <input type="password" name="passcode" autocomplete="off" required></label><br><br>
<button type="submit" name="action" value="approve">Approve</button>
<button type="submit" name="action" value="deny" formnovalidate>Deny</button>
</form></body></html>`,
			html.EscapeString(appName), html.EscapeString(client.ID), html.EscapeString(tx), options.String())
	case http.MethodPost:
		if err := r.ParseForm(); err != nil {
			oauthError(w, http.StatusBadRequest, "invalid_request", "bad form")
			return
		}
		tx := r.Form.Get("tx")
		req, ok := g.oauth.GetRequest(tx)
		if !ok {
			oauthError(w, http.StatusBadRequest, "invalid_request", "authorization request expired or already used")
			return
		}
		if g.currentPassHash() == "" {
			oauthError(w, http.StatusServiceUnavailable, "server_error", "owner consent is not configured")
			return
		}
		pass := r.Form.Get("passcode")
		if !g.checkPasscode(pass) {
			w.Header().Set("Content-Type", "text/plain; charset=utf-8")
			w.WriteHeader(http.StatusForbidden)
			_, _ = w.Write([]byte("wrong owner passcode\n"))
			return
		}
		state := req.State
		if r.Form.Get("action") == "deny" {
			g.oauth.ConsumeRequest(tx)
			params := url.Values{"error": {"access_denied"}}
			if state != "" {
				params.Set("state", state)
			}
			redirectWithParams(w, r, req.RedirectURI, params)
			return
		}
		bound := r.Form.Get("bound_client")
		validBound := false
		for _, p := range g.registry.OAuthProfiles() {
			if p.Name == bound {
				validBound = true
			}
		}
		if !validBound {
			oauthError(w, http.StatusBadRequest, "invalid_request", "choose a configured OAuth identity")
			return
		}
		code, usedReq, err := g.oauth.BindAndCode(tx, bound)
		if err != nil {
			oauthError(w, http.StatusBadRequest, "invalid_request", err.Error())
			return
		}
		log.Printf("oauth client %s bound to registry client %s", usedReq.OAuthClientID, bound)
		params := url.Values{"code": {code}}
		if state != "" {
			params.Set("state", state)
		}
		redirectWithParams(w, r, usedReq.RedirectURI, params)
	default:
		oauthError(w, http.StatusMethodNotAllowed, "invalid_request", "GET or POST only")
	}
}

func (g *Gate) handleToken(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		oauthError(w, http.StatusMethodNotAllowed, "invalid_request", "POST only")
		return
	}
	if err := r.ParseForm(); err != nil {
		oauthError(w, http.StatusBadRequest, "invalid_request", "bad form")
		return
	}
	clientID := r.Form.Get("client_id")
	clientSecret := r.Form.Get("client_secret")
	if u, p, ok := r.BasicAuth(); ok {
		if clientID != "" && clientID != u {
			oauthError(w, http.StatusBadRequest, "invalid_client", "conflicting client_id")
			return
		}
		clientID = u
		clientSecret = p
	}
	if clientID == "" {
		oauthError(w, http.StatusBadRequest, "invalid_client", "client_id is required")
		return
	}
	var access, refresh string
	var rec TokenRecord
	var err error
	switch r.Form.Get("grant_type") {
	case "authorization_code":
		access, refresh, rec, err = g.oauth.ExchangeCode(clientID, clientSecret,
			r.Form.Get("code"), r.Form.Get("redirect_uri"), r.Form.Get("code_verifier"), r.Form.Get("resource"))
	case "refresh_token":
		access, refresh, rec, err = g.oauth.Refresh(clientID, clientSecret, r.Form.Get("refresh_token"))
	default:
		oauthError(w, http.StatusBadRequest, "unsupported_grant_type", "grant_type must be authorization_code or refresh_token")
		return
	}
	if err != nil {
		status := http.StatusBadRequest
		if err.Error() == "invalid_client" {
			status = http.StatusUnauthorized
		}
		oauthError(w, status, err.Error(), "token request failed")
		return
	}
	resp := map[string]any{
		"access_token": access,
		"token_type":   "Bearer",
		"expires_in":   int(time.Until(rec.AccessExpiresAt).Seconds()),
		"scope":        rec.Scope,
	}
	if refresh != "" {
		resp["refresh_token"] = refresh
	}
	writeJSON(w, http.StatusOK, resp)
}

// oauthAudit wraps the OAuth endpoints so registrations, authorization
// requests and token exchanges appear in the audit log alongside /mcp
// traffic. Only categorical details are logged (auth method, grant type,
// redirect host, client id) — never secrets, codes, verifiers or passcodes.
func (g *Gate) oauthAudit(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		detail := "metadata"
		switch r.URL.Path {
		case "/register":
			if r.Method == http.MethodPost {
				body, _ := io.ReadAll(io.LimitReader(r.Body, 64<<10))
				r.Body = io.NopCloser(bytes.NewReader(body))
				var probe struct {
					TokenEndpointAuthMethod string   `json:"token_endpoint_auth_method"`
					GrantTypes              []string `json:"grant_types"`
					RedirectURIs            []string `json:"redirect_uris"`
				}
				if json.Unmarshal(body, &probe) == nil {
					host := ""
					if len(probe.RedirectURIs) > 0 {
						if u, err := url.Parse(probe.RedirectURIs[0]); err == nil {
							host = u.Host
						}
					}
					detail = fmt.Sprintf("register auth=%s grants=%s redir_host=%s", probe.TokenEndpointAuthMethod, strings.Join(probe.GrantTypes, ","), host)
				}
			}
		case "/authorize":
			if r.Method == http.MethodGet {
				detail = "authorize client=" + r.URL.Query().Get("client_id")
			} else {
				detail = "authorize consent"
			}
		case "/token":
			if r.Method == http.MethodPost {
				body, _ := io.ReadAll(io.LimitReader(r.Body, 64<<10))
				r.Body = io.NopCloser(bytes.NewReader(body))
				if vals, err := url.ParseQuery(string(body)); err == nil {
					detail = "token grant=" + vals.Get("grant_type")
				}
			}
		}
		sw := &statusWriter{ResponseWriter: w, status: http.StatusOK}
		next(sw, r)
		g.audit.Log("-", "oauth", detail, r.URL.Path, sw.status)
	}
}

/* ================= HTTP auth + audit middleware ================= */

type clientCtxKey struct{}

func clientFromContext(ctx context.Context) (Client, bool) {
	c, ok := ctx.Value(clientCtxKey{}).(Client)
	return c, ok
}

type statusWriter struct {
	http.ResponseWriter
	status int
	wrote  bool
}

func (s *statusWriter) WriteHeader(code int) {
	if s.wrote {
		return
	}
	s.status = code
	s.wrote = true
	s.ResponseWriter.WriteHeader(code)
}

func (s *statusWriter) Write(b []byte) (int, error) {
	if !s.wrote {
		s.WriteHeader(http.StatusOK)
	}
	return s.ResponseWriter.Write(b)
}

func (s *statusWriter) Flush() {
	if f, ok := s.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

func (s *statusWriter) Unwrap() http.ResponseWriter { return s.ResponseWriter }

type rpcEnvelope struct {
	Method string `json:"method"`
	Params struct {
		Name string `json:"name"`
		URI  string `json:"uri"`
	} `json:"params"`
}

// rpcMeta extracts only the JSON-RPC method and (for tools/call) the tool
// name from a request body, then restores the body untouched. It never
// returns argument values, so it is safe for the audit log.
func rpcMeta(r *http.Request) (method, tool string) {
	method = r.Method
	if r.Method != http.MethodPost || r.Body == nil {
		return method, ""
	}
	data, err := io.ReadAll(io.LimitReader(r.Body, 8<<20))
	_ = r.Body.Close()
	if err != nil {
		return method, ""
	}
	r.Body = io.NopCloser(bytes.NewReader(data))
	r.ContentLength = int64(len(data))
	r.GetBody = func() (io.ReadCloser, error) { return io.NopCloser(bytes.NewReader(data)), nil }

	trimmed := bytes.TrimSpace(data)
	if len(trimmed) == 0 {
		return method, ""
	}
	var envs []rpcEnvelope
	if trimmed[0] == '[' {
		if err := json.Unmarshal(trimmed, &envs); err != nil || len(envs) == 0 {
			return method, ""
		}
	} else {
		var e rpcEnvelope
		if err := json.Unmarshal(trimmed, &e); err != nil {
			return method, ""
		}
		envs = []rpcEnvelope{e}
	}
	for _, e := range envs {
		if e.Method != "" {
			m := e.Method
			t := ""
			if m == "tools/call" {
				t = e.Params.Name
			} else if m == "resources/read" {
				t = e.Params.URI
			}
			return m, t
		}
	}
	return method, ""
}

func requestToken(r *http.Request) string {
	auth := strings.TrimSpace(r.Header.Get("Authorization"))
	if strings.HasPrefix(auth, "Bearer ") {
		return strings.TrimSpace(strings.TrimPrefix(auth, "Bearer "))
	}
	if k := strings.TrimSpace(r.Header.Get("x-api-key")); k != "" {
		return k
	}
	return ""
}

type Gate struct {
	bus           *Bus
	registry      *RegistryStore
	audit         *Audit
	oauth         *OAuthStore
	publicBaseURL string
	ownerPassHash string
	// passMu guards ownerPassHash: the one-time set-passcode page re-keys
	// it live (and persists it to passEnvFile) without a restart.
	passMu        sync.RWMutex
	passStatePath string
	passEnvFile   string
}

/* ================= Owner passcode re-key page (one-time) ================= */

// passPageState is the on-disk state for the one-time set-passcode page.
// The page is live only while a state file with used=false exists; the
// segment is generated out-of-band at deploy time, kept in an owner-only
// file, and never logged or audited.
type passPageState struct {
	Segment string `json:"segment"`
	Used    bool   `json:"used"`
}

func (g *Gate) loadPassPageState() (passPageState, bool) {
	var st passPageState
	if g.passStatePath == "" {
		return st, false
	}
	data, err := os.ReadFile(g.passStatePath)
	if err != nil {
		return st, false
	}
	if err := json.Unmarshal(data, &st); err != nil || st.Segment == "" {
		return st, false
	}
	return st, true
}

func (g *Gate) savePassPageState(st passPageState) error {
	data, err := json.Marshal(st)
	if err != nil {
		return err
	}
	tmp := g.passStatePath + ".tmp"
	if err := os.WriteFile(tmp, data, 0600); err != nil {
		return err
	}
	return os.Rename(tmp, g.passStatePath)
}

func (g *Gate) currentPassHash() string {
	g.passMu.RLock()
	defer g.passMu.RUnlock()
	return g.ownerPassHash
}

// checkPasscode mirrors the consent-page comparison exactly.
func (g *Gate) checkPasscode(pass string) bool {
	stored := g.currentPassHash()
	if stored == "" {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(strings.ToLower(stored)), []byte(hashToken(pass))) == 1
}

// writePasscodeHash persists a new owner passcode hash into the gate env
// file, replacing (or appending) the OWNER_PASSCODE_HASH line and leaving
// every other line untouched.
func writePasscodeHash(envPath, newHash string) error {
	data, err := os.ReadFile(envPath)
	if err != nil {
		return err
	}
	lines := strings.Split(string(data), "\n")
	found := false
	for i, ln := range lines {
		if strings.HasPrefix(ln, "OWNER_PASSCODE_HASH=") {
			lines[i] = "OWNER_PASSCODE_HASH=" + newHash
			found = true
		}
	}
	out := strings.Join(lines, "\n")
	if !found {
		if !strings.HasSuffix(out, "\n") {
			out += "\n"
		}
		out += "OWNER_PASSCODE_HASH=" + newHash + "\n"
	}
	tmp := envPath + ".tmp"
	if err := os.WriteFile(tmp, []byte(out), 0600); err != nil {
		return err
	}
	return os.Rename(tmp, envPath)
}

const setPasscodeForm = `<!doctype html><html><head><meta charset="utf-8"><title>Set gate passcode</title></head><body>
<h1>Set a new gate passcode</h1>
%s<form method="post">
<label>New passcode: <input type="password" name="new_passcode" autocomplete="off" required></label><br><br>
<label>Repeat new passcode: <input type="password" name="new_passcode_repeat" autocomplete="off" required></label><br><br>
<button type="submit">Set new passcode</button>
</form>
</body></html>`

// handleSetPasscode serves the one-time re-key page at
// /k/<segment>/set-passcode. Any state problem (no state file, already
// used, wrong segment, no passcode configured) is a plain 404, so the
// page is indistinguishable from any other dead secret path.
func (g *Gate) handleSetPasscode(w http.ResponseWriter, r *http.Request) {
	seg := r.PathValue("seg")
	st, ok := g.loadPassPageState()
	if !ok || st.Used || g.currentPassHash() == "" ||
		subtle.ConstantTimeCompare([]byte(st.Segment), []byte(seg)) != 1 {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	renderForm := func(errLine string) {
		errHTML := ""
		if errLine != "" {
			errHTML = "<p><strong>" + html.EscapeString(errLine) + "</strong></p>\n"
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		fmt.Fprintf(w, setPasscodeForm, errHTML)
	}
	if r.Method == http.MethodGet {
		renderForm("")
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	// Link-only mode: possession of the single-use secret link is the
	// authority for the reset (the owner lost the current passcode), so
	// there is no current-passcode check. A malformed body just yields
	// empty fields, which the length check below rejects.
	_ = r.ParseForm()
	newPass := r.Form.Get("new_passcode")
	repeat := r.Form.Get("new_passcode_repeat")
	if utf8.RuneCountInString(newPass) < 10 {
		renderForm("too short")
		return
	}
	if newPass != repeat {
		renderForm("new passcodes do not match")
		return
	}
	newHash := hashToken(newPass)
	g.passMu.Lock()
	defer g.passMu.Unlock()
	// Re-verify the page is still live under the write lock, then
	// persist, swap the live hash, and burn the page — all before the
	// success response is written.
	st2, ok2 := g.loadPassPageState()
	if !ok2 || st2.Used || subtle.ConstantTimeCompare([]byte(st2.Segment), []byte(seg)) != 1 {
		http.NotFound(w, r)
		return
	}
	st = st2
	if err := writePasscodeHash(g.passEnvFile, newHash); err != nil {
		log.Printf("set-passcode: env write failed: %v", err)
		renderForm("could not update the passcode, please try again")
		return
	}
	g.ownerPassHash = newHash
	st.Used = true
	if err := g.savePassPageState(st); err != nil {
		// The new passcode is already live and persisted; if the state
		// write failed the page would stay live for a further use.
		log.Printf("set-passcode: WARNING state write failed after re-key: %v", err)
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write([]byte("<!doctype html><html><head><meta charset=\"utf-8\"><title>Done</title></head><body><p>Done. Your new passcode is now the only key. You can close this page.</p></body></html>"))
}

// resolveClient accepts a static registry token first (unchanged
// behaviour), then an OAuth-issued access token.
func (g *Gate) resolveClient(token string) (Client, bool) {
	if c, ok := g.registry.Lookup(token); ok {
		return c, true
	}
	if g.oauth != nil {
		if bound, ok := g.oauth.ResolveAccess(token); ok {
			return g.registry.Get(bound)
		}
	}
	return Client{}, false
}

func (g *Gate) authAudit(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		method, tool := rpcMeta(r)
		sw := &statusWriter{ResponseWriter: w, status: http.StatusOK}
		client, ok := g.resolveClient(requestToken(r))
		if !ok {
			sw.Header().Set("WWW-Authenticate", g.wwwAuthenticate())
			http.Error(sw, "unauthorized", http.StatusUnauthorized)
			g.audit.Log("-", method, tool, "/mcp", sw.status)
			return
		}
		ctx := context.WithValue(r.Context(), clientCtxKey{}, client)
		next.ServeHTTP(sw, r.WithContext(ctx))
		g.audit.Log(client.Name, method, tool, "/mcp", sw.status)
	})
}

// pathAuthAudit serves the Grok-app fallback route /k/<secret>/mcp. The
// unguessable path segment is the credential: it is hashed and compared
// in constant time against the registry's path_secret_hash, and it is
// NEVER logged — the audit path is the constant /k/REDACTED/mcp. Any
// other /k/ shape, or a wrong secret, is a plain 404 that reveals
// nothing. Header credentials are not accepted on this route.
func (g *Gate) pathAuthAudit(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		const auditPath = "/k/REDACTED/mcp"
		method, tool := rpcMeta(r)
		sw := &statusWriter{ResponseWriter: w, status: http.StatusOK}
		rest := strings.TrimPrefix(r.URL.Path, "/k/")
		parts := strings.Split(rest, "/")
		if len(parts) != 2 || parts[0] == "" || parts[1] != "mcp" {
			http.NotFound(sw, r)
			g.audit.Log("-", method, tool, auditPath, sw.status)
			return
		}
		client, ok := g.registry.LookupPathSecret(parts[0])
		if !ok {
			http.NotFound(sw, r)
			g.audit.Log("-", method, tool, auditPath, sw.status)
			return
		}
		ctx := context.WithValue(r.Context(), clientCtxKey{}, client)
		next.ServeHTTP(sw, r.WithContext(ctx))
		g.audit.Log(client.Name, method, tool, auditPath, sw.status)
	})
}

/* ================= MCP facade (per-client filtered server) ================= */

func (g *Gate) buildServer(c Client) *mcp.Server {
	server := mcp.NewServer(&mcp.Implementation{
		Name:    "universal-front-door",
		Version: "1.0.0",
	}, nil)
	b := g.bus

	if c.allowsTool("post_task") {
		mcp.AddTool(server, &mcp.Tool{
			Name:        "post_task",
			Description: "向共享总线派发一个交给特定角色 Agent 处理的任务",
		}, func(ctx context.Context, req *mcp.CallToolRequest, in PostTaskInput) (*mcp.CallToolResult, PostTaskOutput, error) {
			out, err := forward[PostTaskInput, PostTaskOutput](ctx, b, "post_task", in)
			return nil, out, err
		})
	}
	if c.allowsTool("claim_next_task") {
		mcp.AddTool(server, &mcp.Tool{
			Name:        "claim_next_task",
			Description: "按角色原子认领最早一条 pending 任务（会自动回收超时未续约的卡死任务）",
		}, func(ctx context.Context, req *mcp.CallToolRequest, in ClaimTaskInput) (*mcp.CallToolResult, ClaimTaskOutput, error) {
			out, err := forward[ClaimTaskInput, ClaimTaskOutput](ctx, b, "claim_next_task", in)
			return nil, out, err
		})
	}
	if c.allowsTool("heartbeat_task") {
		mcp.AddTool(server, &mcp.Tool{
			Name:        "heartbeat_task",
			Description: "为长时间运行的 in_progress 任务续期租约，防止超时被其他 Agent 回收",
		}, func(ctx context.Context, req *mcp.CallToolRequest, in HeartbeatTaskInput) (*mcp.CallToolResult, HeartbeatTaskOutput, error) {
			out, err := forward[HeartbeatTaskInput, HeartbeatTaskOutput](ctx, b, "heartbeat_task", in)
			return nil, out, err
		})
	}
	if c.allowsTool("complete_task") {
		mcp.AddTool(server, &mcp.Tool{
			Name:        "complete_task",
			Description: "提交任务短摘要；如有长正文/大产物，请传入 full_content 与 artifact_key 供下游按需读取",
		}, func(ctx context.Context, req *mcp.CallToolRequest, in CompleteTaskInput) (*mcp.CallToolResult, CompleteTaskOutput, error) {
			out, err := forward[CompleteTaskInput, CompleteTaskOutput](ctx, b, "complete_task", in)
			return nil, out, err
		})
	}
	if c.allowsTool("get_artifact") {
		mcp.AddTool(server, &mcp.Tool{
			Name:        "get_artifact",
			Description: "按 artifact_key 读取上游 Agent 写入的完整共享产物内容",
		}, func(ctx context.Context, req *mcp.CallToolRequest, in GetArtifactInput) (*mcp.CallToolResult, GetArtifactOutput, error) {
			out, err := forward[GetArtifactInput, GetArtifactOutput](ctx, b, "get_artifact", in)
			return nil, out, err
		})
	}
	if c.allowsTool("ping_presence") {
		mcp.AddTool(server, &mcp.Tool{
			Name:        "ping_presence",
			Description: "登记或刷新当前 Agent 的在线状态（claim / heartbeat / complete 也会自动刷新；空闲时定期 ping 即可保持 alive）",
		}, func(ctx context.Context, req *mcp.CallToolRequest, in PingPresenceInput) (*mcp.CallToolResult, PingPresenceOutput, error) {
			out, err := forward[PingPresenceInput, PingPresenceOutput](ctx, b, "ping_presence", in)
			return nil, out, err
		})
	}
	if c.allowsTool("bus_state") {
		mcp.AddTool(server, &mcp.Tool{
			Name:        "bus_state",
			Description: "读取共享总线的任务概况：按状态计数、最近任务（ID/标题/目标角色/状态）与可用产物键列表",
			Annotations: &mcp.ToolAnnotations{ReadOnlyHint: true},
		}, func(ctx context.Context, req *mcp.CallToolRequest, in BusStateInput) (*mcp.CallToolResult, BusStateOutput, error) {
			text, err := b.ReadState(ctx)
			if err != nil {
				return nil, BusStateOutput{}, err
			}
			var snap struct {
				Tasks  []Task          `json:"tasks"`
				Keys   []string        `json:"artifact_keys"`
				Agents []AgentPresence `json:"agents"`
			}
			if err := json.Unmarshal([]byte(text), &snap); err != nil {
				return nil, BusStateOutput{}, fmt.Errorf("parse bus state: %w", err)
			}
			out := BusStateOutput{
				Total:        len(snap.Tasks),
				Counts:       map[string]int{},
				ArtifactKeys: snap.Keys,
				Agents:       snap.Agents,
			}
			if out.Agents == nil {
				out.Agents = []AgentPresence{}
			}
			for _, t := range snap.Tasks {
				out.Counts[t.Status]++
			}
			start := 0
			if len(snap.Tasks) > 10 {
				start = len(snap.Tasks) - 10
			}
			for _, t := range snap.Tasks[start:] {
				out.Recent = append([]BusStateTask{{
					ID:          t.ID,
					Title:       t.Title,
					TargetRole:  t.TargetRole,
					Status:      t.Status,
					ClaimedBy:   t.ClaimedBy,
					ArtifactKey: t.ArtifactKey,
					UpdatedAt:   t.UpdatedAt,
				}}, out.Recent...)
			}
			if out.Recent == nil {
				out.Recent = []BusStateTask{}
			}
			if out.ArtifactKeys == nil {
				out.ArtifactKeys = []string{}
			}
			return nil, out, nil
		})
	}
	if c.allowsResource("bus://state") {
		server.AddResource(&mcp.Resource{
			URI:         "bus://state",
			Name:        "agent-bus-state",
			Description: "当前任务队列与可用共享产物键列表（完整产物请调 get_artifact）",
			MIMEType:    "application/json",
		}, func(ctx context.Context, req *mcp.ReadResourceRequest) (*mcp.ReadResourceResult, error) {
			text, err := b.ReadState(ctx)
			if err != nil {
				return nil, err
			}
			return &mcp.ReadResourceResult{
				Contents: []*mcp.ResourceContents{
					{URI: req.Params.URI, MIMEType: "application/json", Text: text},
				},
			}, nil
		})
	}
	return server
}

/* ================= Config ================= */

func envOr(names []string, def string) string {
	for _, n := range names {
		if v := strings.TrimSpace(os.Getenv(n)); v != "" {
			return v
		}
	}
	return def
}

func defaultRegistryPath() string {
	return envOr([]string{"REGISTRY_PATH", "GATE_REGISTRY"}, "./clients.json")
}

func loadBusToken() (string, error) {
	if v := strings.TrimSpace(os.Getenv("BUS_TOKEN")); v != "" {
		return v, nil
	}
	path := strings.TrimSpace(os.Getenv("BUS_TOKEN_FILE"))
	if path == "" {
		return "", errors.New("neither BUS_TOKEN nor BUS_TOKEN_FILE is set")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("read bus token file: %w", err)
	}
	tok := strings.TrimSpace(string(data))
	if tok == "" {
		return "", errors.New("bus token file is empty")
	}
	return tok, nil
}

/* ================= gatectl admin helper ================= */

var clientNameRe = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]*$`)

func splitList(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func runGatectl(args []string) error {
	if len(args) < 1 {
		return errors.New("usage: gate gatectl <add|path-secret> <name> [flags]")
	}
	switch args[0] {
	case "add":
		return gatectlAdd(args[1:])
	case "path-secret":
		return gatectlPathSecret(args[1:])
	default:
		return errors.New("usage: gate gatectl <add|path-secret> <name> [flags]")
	}
}

func gatectlAdd(args []string) error {
	if len(args) < 1 {
		return errors.New("gatectl add: client name is required")
	}
	name := args[0]
	if !clientNameRe.MatchString(name) {
		return fmt.Errorf("gatectl add: invalid client name %q", name)
	}
	fs := flag.NewFlagSet("gatectl add", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	role := fs.String("role", "", "client role, e.g. commander")
	tools := fs.String("tools", "", "comma-separated allowed tools")
	resources := fs.String("resources", "", "comma-separated allowed resources")
	registryPath := fs.String("registry", defaultRegistryPath(), "registry JSON path")
	tokenDir := fs.String("token-dir", "", "directory for the one-time plaintext token file (default: registry directory)")
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		return err
	}
	token := hex.EncodeToString(raw)
	enabled := true
	client := Client{
		Name:             name,
		TokenHash:        hashToken(token),
		Role:             *role,
		AllowedTools:     splitList(*tools),
		AllowedResources: splitList(*resources),
		Enabled:          &enabled,
	}
	store := &RegistryStore{path: *registryPath}
	if err := store.Upsert(client); err != nil {
		return err
	}
	dir := *tokenDir
	if dir == "" {
		dir = filepath.Dir(*registryPath)
	}
	if dir == "" {
		dir = "."
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	tokenPath := filepath.Join(dir, name+".token")
	if err := os.WriteFile(tokenPath, []byte(token+"\n"), 0o600); err != nil {
		return err
	}
	if err := os.Chmod(tokenPath, 0o600); err != nil {
		return err
	}
	fmt.Printf("client %q added (role=%s, tools=%s, resources=%s); plaintext token written once to %s\n",
		name, *role, *tools, *resources, tokenPath)
	return nil
}

// gatectlPathSecret creates (or rotates) a path-secret client such as
// grok-path: it generates a fresh >=32-byte URL-safe secret, stores only
// its SHA-256 hash in the registry, and writes the full public URL —
// the only plaintext copy of the secret — to the given owner-only file.
// Neither the secret nor the URL is ever printed. Re-running it rotates
// the secret (the old URL stops working at the next registry reload,
// which is per-request). Deleting or disabling the registry entry
// revokes the path completely.
func gatectlPathSecret(args []string) error {
	if len(args) < 1 {
		return errors.New("gatectl path-secret: client name is required")
	}
	name := args[0]
	if !clientNameRe.MatchString(name) {
		return fmt.Errorf("gatectl path-secret: invalid client name %q", name)
	}
	fs := flag.NewFlagSet("gatectl path-secret", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	role := fs.String("role", "commander", "client role, e.g. commander")
	tools := fs.String("tools", "", "comma-separated allowed tools")
	resources := fs.String("resources", "", "comma-separated allowed resources")
	registryPath := fs.String("registry", defaultRegistryPath(), "registry JSON path")
	base := fs.String("base", "", "public base URL of the gate, no trailing slash (or set PUBLIC_BASE_URL)")
	urlFile := fs.String("url-file", "", "owner-only file to write the full secret URL to (default: <registry dir>/<name>.url)")
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}
	publicBase := strings.TrimRight(*base, "/")
	if publicBase == "" {
		publicBase = strings.TrimRight(os.Getenv("PUBLIC_BASE_URL"), "/")
	}
	if publicBase == "" {
		return errors.New("gatectl path-secret: --base or PUBLIC_BASE_URL is required to write the full URL")
	}
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		return err
	}
	secret := base64.RawURLEncoding.EncodeToString(raw)
	enabled := true
	client := Client{
		Name:             name,
		PathSecretHash:   hashToken(secret),
		Role:             *role,
		AllowedTools:     splitList(*tools),
		AllowedResources: splitList(*resources),
		Enabled:          &enabled,
	}
	store := &RegistryStore{path: *registryPath}
	if err := store.Upsert(client); err != nil {
		return err
	}
	out := *urlFile
	if out == "" {
		dir := filepath.Dir(*registryPath)
		if dir == "" {
			dir = "."
		}
		out = filepath.Join(dir, name+".url")
	}
	if dir := filepath.Dir(out); dir != "" {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return err
		}
	}
	fullURL := publicBase + "/k/" + secret + "/mcp"
	if err := os.WriteFile(out, []byte(fullURL+"\n"), 0o600); err != nil {
		return err
	}
	if err := os.Chmod(out, 0o600); err != nil {
		return err
	}
	fmt.Printf("path-secret client %q added (role=%s, tools=%s, resources=%s); full secret URL written once to %s (never printed)\n",
		name, *role, *tools, *resources, out)
	return nil
}

/* ================= main ================= */

// faviconPNG is the gate's icon, served at /favicon.ico and /logo.png.
// Added 2026-11-24 (iconfix): ChatGPT's plugin-creation validation names
// "icon" among the settings it checks, and this origin previously 404'd
// both / and /favicon.ico.
//
//go:embed favicon.png
var faviconPNG []byte

func main() {
	if len(os.Args) > 1 && os.Args[1] == "gatectl" {
		if err := runGatectl(os.Args[2:]); err != nil {
			fmt.Fprintln(os.Stderr, "gatectl:", err)
			os.Exit(2)
		}
		return
	}

	busURL := envOr([]string{"BUS_URL"}, "http://127.0.0.1:8484/mcp")
	busTok, err := loadBusToken()
	if err != nil {
		log.Fatalf("bus token: %v", err)
	}
	addr := envOr([]string{"GATE_ADDR", "LISTEN_ADDR", "LISTEN"}, "127.0.0.1:8585")
	audit, err := NewAudit(strings.TrimSpace(os.Getenv("AUDIT_LOG_PATH")))
	if err != nil {
		log.Fatalf("audit log: %v", err)
	}

	registryPath := defaultRegistryPath()
	oauthPath := strings.TrimSpace(os.Getenv("OAUTH_STORE_PATH"))
	if oauthPath == "" {
		dir := filepath.Dir(registryPath)
		if dir == "" {
			dir = "."
		}
		oauthPath = filepath.Join(dir, "oauth.json")
	}
	oauthStore, err := NewOAuthStore(oauthPath)
	if err != nil {
		log.Fatalf("oauth store: %v", err)
	}
	publicBase := strings.TrimRight(envOr([]string{"PUBLIC_BASE_URL", "GATE_PUBLIC_BASE_URL"}, ""), "/")
	ownerPassHash := strings.TrimSpace(os.Getenv("OWNER_PASSCODE_HASH"))
	if p := strings.TrimSpace(os.Getenv("OWNER_PASSCODE_HASH_FILE")); p != "" && ownerPassHash == "" {
		if data, rerr := os.ReadFile(p); rerr == nil {
			ownerPassHash = strings.TrimSpace(string(data))
		}
	}

	passStatePath := strings.TrimSpace(os.Getenv("PASSPAGE_STATE_PATH"))
	if passStatePath == "" {
		dir := filepath.Dir(registryPath)
		if dir == "" {
			dir = "."
		}
		passStatePath = filepath.Join(dir, "passpage.json")
	}
	passEnvFile := strings.TrimSpace(os.Getenv("OWNER_PASSCODE_ENV_FILE"))
	if passEnvFile == "" {
		passEnvFile = "gate.env"
	}

	gate := &Gate{
		bus:           &Bus{url: busURL, tok: busTok},
		registry:      &RegistryStore{path: registryPath},
		audit:         audit,
		oauth:         oauthStore,
		publicBaseURL: publicBase,
		ownerPassHash: ownerPassHash,
		passStatePath: passStatePath,
		passEnvFile:   passEnvFile,
	}

	mcpHandler := mcp.NewStreamableHTTPHandler(func(r *http.Request) *mcp.Server {
		client, ok := clientFromContext(r.Context())
		if !ok {
			// Auth middleware runs first and rejects unknown clients, so
			// this is only a defensive default-deny server.
			return gate.buildServer(Client{})
		}
		return gate.buildServer(client)
	}, nil)

	mux := http.NewServeMux()
	mux.Handle("/mcp", gate.authAudit(mcpHandler))
	mux.Handle("/k/", gate.pathAuthAudit(mcpHandler))
	mux.HandleFunc("/.well-known/oauth-protected-resource", gate.oauthAudit(gate.handleProtectedResourceMetadata))
	mux.HandleFunc("/.well-known/oauth-protected-resource/", gate.oauthAudit(gate.handleProtectedResourceMetadata))
	mux.HandleFunc("/.well-known/oauth-authorization-server", gate.oauthAudit(gate.handleAuthServerMetadata))
	mux.HandleFunc("/.well-known/openid-configuration", gate.oauthAudit(gate.handleAuthServerMetadata))
	mux.HandleFunc("/register", gate.oauthAudit(gate.handleRegister))
	mux.HandleFunc("/authorize", gate.oauthAudit(gate.handleAuthorize))
	mux.HandleFunc("/token", gate.oauthAudit(gate.handleToken))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = w.Write([]byte("ok\n"))
	})
	// Exact root only ({$}): unknown paths keep the mux's default 404.
	mux.HandleFunc("/{$}", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = w.Write([]byte("agent-bus front door\n"))
	})
	serveIcon := func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "image/png")
		w.Header().Set("Cache-Control", "public, max-age=86400")
		_, _ = w.Write(faviconPNG)
	}
	mux.HandleFunc("/favicon.ico", serveIcon)
	mux.HandleFunc("/logo.png", serveIcon)
	// One-time owner re-key page; more specific than the "/k/" subtree,
	// and the handler 404s unless a live state file names the segment.
	mux.HandleFunc("GET /k/{seg}/set-passcode", gate.handleSetPasscode)
	mux.HandleFunc("POST /k/{seg}/set-passcode", gate.handleSetPasscode)

	srv := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 15 * time.Second,
	}
	log.Printf("front-door gate listening on %s (bus=%s, registry=%s, oauth=%s, public=%s, owner_consent=%v)",
		addr, busURL, registryPath, oauthPath, gate.publicBase(), ownerPassHash != "")
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatalf("gate server error: %v", err)
	}
}
