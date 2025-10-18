package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/chromedp/cdproto/emulation"
	"github.com/chromedp/cdproto/network"
	"github.com/chromedp/chromedp"
	"github.com/go-co-op/gocron"
	"github.com/joho/godotenv"
	"github.com/redis/go-redis/v9"
)

var (
	ctx = context.Background()

	// Redis
	rdb          *redis.Client
	streamMaxLen int64
	oddsExpire   time.Duration

	// 调度与日志
	jitterMaxSec int
	appendOnChg  bool
	logLevel     = "info"

	// 浏览器发现/连接
	chromeDiscovery string // e.g. http://chrome:9222/json/version
	chromeWS        string // ws://chrome:9222/devtools/browser/<id>
	discoveryClient *http.Client
)

// ---------------- small utils ----------------
func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
func mustAtoiEnv(k, def string) int {
	if v := os.Getenv(k); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	n, _ := strconv.Atoi(def)
	return n
}
func mustAtobEnv(k, def string) bool {
	v := strings.TrimSpace(getenv(k, def))
	return strings.EqualFold(v, "1") || strings.EqualFold(v, "true") || strings.EqualFold(v, "yes")
}
func logAtLeast(level string) bool {
	order := map[string]int{"debug": 10, "info": 20, "warn": 30, "error": 40}
	cur := order[strings.ToLower(logLevel)]
	want := order[strings.ToLower(level)]
	if cur == 0 {
		cur = 20
	}
	if want == 0 {
		want = 20
	}
	return want >= cur
}
func L(level, format string, a ...any) {
	if logAtLeast(level) {
		pfx := map[string]string{"debug": "DBG", "info": "INF", "warn": "WRN", "error": "ERR"}[strings.ToLower(level)]
		log.Printf("[%s] %s", pfx, fmt.Sprintf(format, a...))
	}
}
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// ---------------- redis ----------------
func newRedis() *redis.Client {
	host := getenv("REDIS_HOST", "127.0.0.1")
	port := mustAtoiEnv("REDIS_PORT", "6379")
	user := getenv("REDIS_USER", "")
	pass := getenv("REDIS_PASS", "")

	useTLS := false
	if t := strings.TrimSpace(os.Getenv("REDIS_TLS")); t != "" {
		useTLS = strings.EqualFold(t, "true")
	} else if port == 6380 || strings.HasPrefix(host, "rediss://") {
		useTLS = true
	}

	addr := fmt.Sprintf("%s:%d", host, port)
	opts := &redis.Options{
		Addr:         addr,
		Username:     user,
		Password:     pass,
		DB:           0,
		DialTimeout:  5 * time.Second,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
	}
	if useTLS {
		opts.TLSConfig = &tls.Config{MinVersion: tls.VersionTLS12}
	}

	client := redis.NewClient(opts)
	if err := client.Ping(ctx).Err(); err != nil {
		log.Fatalf("Redis connect failed: %v (%s TLS=%v)", err, addr, useTLS)
	}
	L("info", "Redis connected: %s (TLS=%v)", addr, useTLS)
	return client
}

// ---------------- init ----------------
func init() {
	_ = godotenv.Load(".env")
	log.SetFlags(log.LstdFlags | log.Lmsgprefix)
	log.SetPrefix("[jczuodds] ")

	logLevel = strings.ToLower(getenv("LOG_LEVEL", "info"))
	appendOnChg = mustAtobEnv("APPEND_HISTORY_ON_CHANGE", "true")
	streamMaxLen = int64(mustAtoiEnv("ODDS_MAXLEN", "300"))
	oddsExpire = time.Duration(mustAtoiEnv("ODDSEXPIRE_H", "72")) * time.Hour
	jitterMaxSec = mustAtoiEnv("JITTER_SEC_MAX", "90")

	// 浏览器自动发现入口（可留空，默认 chrome 容器）
	chromeDiscovery = strings.TrimSpace(getenv("CHROME_DISCOVERY", "http://chrome:9222/json/version"))
	chromeWS = strings.TrimSpace(os.Getenv("CHROME_WS")) // 若提供则优先用，失效时会自动重发现

	// 发现专用 HTTP 客户端（仅用于 /json/version 与 /json），禁用环境代理，直连容器网络
	discoveryTransport := &http.Transport{
		Proxy: nil, // 关键：不走任何代理
		DialContext: (&net.Dialer{
			Timeout:   5 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12},
	}
	discoveryClient = &http.Client{
		Timeout:   8 * time.Second,
		Transport: discoveryTransport,
	}

	rand.Seed(time.Now().UnixNano())
	rdb = newRedis()

	L("info", "Config: LOG_LEVEL=%s, OnChange=%v, MaxLen=%d, Expire=%s, Jitter<=%ds",
		logLevel, appendOnChg, streamMaxLen, oddsExpire, jitterMaxSec)
	if chromeWS != "" {
		L("info", "Browser ONLY, ws_override=%q", chromeWS)
	} else {
		L("info", "Browser ONLY, discovery=%s ws_override=%q", chromeDiscovery, chromeWS)
	}
}

// ---------------- domain mapping ----------------
func toStr(v any) string {
	switch t := v.(type) {
	case string:
		return t
	case float64:
		return strconv.FormatFloat(t, 'f', -1, 64)
	default:
		return ""
	}
}
func buildMatchID(taxDateNo, matchNumStr string) string {
	s := matchNumStr
	if len(s) > 3 {
		s = s[len(s)-3:]
	}
	return taxDateNo + s
}

type matchOdds struct {
	id        string
	date      string
	odds      map[string]map[string]string // play -> fields
	playCount map[string]int
}

func cleanAndFormat(sub map[string]any) matchOdds {
	taxDateNo := toStr(sub["taxDateNo"])
	matchNumStr := toStr(sub["matchNumStr"])
	id := buildMatchID(taxDateNo, matchNumStr)

	date := toStr(sub["matchDate"])
	if len(date) >= 10 {
		date = date[:10]
	}

	out := matchOdds{
		id:        id,
		date:      date,
		odds:      map[string]map[string]string{},
		playCount: map[string]int{},
	}
	if had, ok := sub["had"].(map[string]any); ok {
		out.odds["mspf"] = map[string]string{"H": toStr(had["h"]), "D": toStr(had["d"]), "A": toStr(had["a"])}
		out.playCount["mspf"]++
	}
	if hhad, ok := sub["hhad"].(map[string]any); ok {
		out.odds["rqspf"] = map[string]string{
			"handicap": toStr(hhad["goalLineValue"]),
			"H":        toStr(hhad["h"]), "D": toStr(hhad["d"]), "A": toStr(hhad["a"]),
		}
		out.playCount["rqspf"]++
	}
	if ttg, ok := sub["ttg"].(map[string]any); ok {
		out.odds["jqs"] = map[string]string{
			"0": toStr(ttg["s0"]), "1": toStr(ttg["s1"]), "2": toStr(ttg["s2"]), "3": toStr(ttg["s3"]),
			"4": toStr(ttg["s4"]), "5": toStr(ttg["s5"]), "6": toStr(ttg["s6"]), "7+": toStr(ttg["s7"]),
		}
		out.playCount["jqs"]++
	}
	if hafu, ok := sub["hafu"].(map[string]any); ok {
		out.odds["bqc"] = map[string]string{
			"WW": toStr(hafu["hh"]), "WD": toStr(hafu["hd"]), "WL": toStr(hafu["ha"]),
			"DW": toStr(hafu["dh"]), "DD": toStr(hafu["dd"]), "DL": toStr(hafu["da"]),
			"LW": toStr(hafu["ah"]), "LD": toStr(hafu["ad"]), "LL": toStr(hafu["aa"]),
		}
		out.playCount["bqc"]++
	}
	if crs, ok := sub["crs"].(map[string]any); ok {
		mp := map[string]string{
			"s00s00": "0:0", "s00s01": "0:1", "s00s02": "0:2", "s00s03": "0:3", "s00s04": "0:4", "s00s05": "0:5",
			"s01s00": "1:0", "s01s01": "1:1", "s01s02": "1:2", "s01s03": "1:3", "s01s04": "1:4", "s01s05": "1:5",
			"s02s00": "2:0", "s02s01": "2:1", "s02s02": "2:2", "s02s03": "2:3", "s02s04": "2:4", "s02s05": "2:5",
			"s03s00": "3:0", "s03s01": "3:1", "s03s02": "3:2", "s03s03": "3:3", "s03s04": "3:4", "s03s05": "3:5",
			"s04s00": "4:0", "s04s01": "4:1", "s04s02": "4:2", "s04s03": "4:3", "s04s04": "4:4", "s04s05": "4:5",
			"s05s00": "5:0", "s05s01": "5:1", "s05s02": "5:2", "s05s03": "5:3", "s05s04": "5:4", "s05s05": "5:5",
			"s1sa": "WOther", "s1sd": "DOther", "s1sh": "LOther",
		}
		row := map[string]string{}
		for k, v := range crs {
			if name, ok2 := mp[k]; ok2 {
				row[name] = toStr(v)
			}
		}
		if len(row) > 0 {
			out.odds["bf"] = row
			out.playCount["bf"]++
		}
	}
	return out
}

// ---------------- browser discovery (0-ops) ----------------
func discoverWS() (string, error) {
	// 目标：返回 ws://chrome:9222/devtools/browser/<id>
	base := chromeDiscovery
	if base == "" {
		base = "http://chrome:9222/json/version"
	}

	// 解析 chrome 的容器 IP（供 IP 直连兜底）
	ip := ""
	if addrs, err := net.LookupHost("chrome"); err == nil && len(addrs) > 0 {
		ip = addrs[0]
	}

	type probe struct {
		URL       string // 完整 URL
		HostHdr   string // 若非空，则设置 req.Host 与 Header("Host")
		ParseMode string // "version" 或 "list"
		Label     string // 仅用于日志排错
	}

	var probes []probe

	// /json/version @ chrome（正常）
	probes = append(probes, probe{
		URL:       strings.Replace(base, "127.0.0.1:9222", "chrome:9222", 1),
		ParseMode: "version",
		Label:     "version@chrome",
	})
	// /json/version @ chrome + Host:127.0.0.1:9222（你环境实测可行）
	probes = append(probes, probe{
		URL:       strings.Replace(base, "127.0.0.1:9222", "chrome:9222", 1),
		HostHdr:   "127.0.0.1:9222",
		ParseMode: "version",
		Label:     "version@chrome host=127.0.0.1",
	})
	// /json 列表兜底 @ chrome
	jsonList := strings.Replace(base, "/json/version", "/json", 1)
	if jsonList == base {
		jsonList = "http://chrome:9222/json"
	}
	probes = append(probes, probe{
		URL:       jsonList,
		ParseMode: "list",
		Label:     "list@chrome",
	})
	// /json 列表 @ chrome + Host:127.0.0.1:9222
	probes = append(probes, probe{
		URL:       jsonList,
		HostHdr:   "127.0.0.1:9222",
		ParseMode: "list",
		Label:     "list@chrome host=127.0.0.1",
	})

	// 用 IP 再全套兜底（某些镜像对 Host 更敏感）
	if ip != "" {
		ipVer := strings.Replace(base, "chrome:9222", ip+":9222", 1)
		ipList := strings.Replace(jsonList, "chrome:9222", ip+":9222", 1)

		probes = append(probes, probe{
			URL:       ipVer,
			ParseMode: "version",
			Label:     "version@ip",
		})
		probes = append(probes, probe{
			URL:       ipVer,
			HostHdr:   "127.0.0.1:9222",
			ParseMode: "version",
			Label:     "version@ip host=127.0.0.1",
		})
		probes = append(probes, probe{
			URL:       ipList,
			ParseMode: "list",
			Label:     "list@ip",
		})
		probes = append(probes, probe{
			URL:       ipList,
			HostHdr:   "127.0.0.1:9222",
			ParseMode: "list",
			Label:     "list@ip host=127.0.0.1",
		})
	}

	// 发送请求
	do := func(p probe) ([]byte, int, error) {
		req, _ := http.NewRequest("GET", p.URL, nil)
		req.Header.Set("Accept", "application/json")
		if p.HostHdr != "" {
			req.Host = p.HostHdr
			req.Header.Set("Host", p.HostHdr)
		}
		resp, err := discoveryClient.Do(req)
		if err != nil {
			return nil, 0, err
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		return b, resp.StatusCode, nil
	}

	var lastErr error
	for round := 0; round < 3; round++ {
		for _, p := range probes {
			b, code, err := do(p)
			if err != nil {
				lastErr = fmt.Errorf("%s -> req err: %v", p.Label, err)
				continue
			}
			if code < 200 || code >= 300 {
				lastErr = fmt.Errorf("%s -> http %d body=%q", p.Label, code, string(b[:min(120, len(b))]))
				continue
			}

			switch p.ParseMode {
			case "version":
				var vr map[string]any
				if err := json.Unmarshal(b, &vr); err != nil {
					lastErr = fmt.Errorf("%s decode json: %v", p.Label, err)
					continue
				}
				if ws, _ := vr["webSocketDebuggerUrl"].(string); ws != "" {
					if ip != "" {
						ws = strings.ReplaceAll(ws, ip+":9222", "chrome:9222")
					}
					ws = strings.ReplaceAll(ws, "127.0.0.1:9222", "chrome:9222")
					return ws, nil
				}
				lastErr = fmt.Errorf("%s no webSocketDebuggerUrl", p.Label)

			case "list":
				var arr []map[string]any
				if err := json.Unmarshal(b, &arr); err != nil {
					lastErr = fmt.Errorf("%s decode json: %v", p.Label, err)
					continue
				}
				for _, it := range arr {
					if typ, _ := it["type"].(string); typ == "browser" {
						if ws, _ := it["webSocketDebuggerUrl"].(string); ws != "" {
							if ip != "" {
								ws = strings.ReplaceAll(ws, ip+":9222", "chrome:9222")
							}
							ws = strings.ReplaceAll(ws, "127.0.0.1:9222", "chrome:9222")
							return ws, nil
						}
					}
				}
				lastErr = fmt.Errorf("%s list has no browser entry", p.Label)
			}
		}
		// 指数退避
		backoff := time.Duration(400*(1<<round)) * time.Millisecond
		time.Sleep(backoff)
	}
	if lastErr == nil {
		lastErr = errors.New("discovery exhausted without any attempt error")
	}
	return "", lastErr
}

// ---------------- chromedp fetch (browser-only) ----------------
func chromeFetchJSON(c context.Context, url string, wsURL string, timeout time.Duration) ([]byte, error) {
	if wsURL == "" {
		return nil, errors.New("chromedp: empty wsURL")
	}

	// 连接远程浏览器（复用已有 session；更像“人类浏览器”）
	allocCtx, cancelAlloc := chromedp.NewRemoteAllocator(c, wsURL)
	defer cancelAlloc()
	taskCtx, cancelTask := chromedp.NewContext(allocCtx)
	defer cancelTask()

	if timeout <= 0 {
		timeout = 35 * time.Second
	}
	cctx, cancel := context.WithTimeout(taskCtx, timeout)
	defer cancel()

	// 统一伪装：移动端 UA + 合理的 Origin/Referer（贴近你之前的直连头部）
	const ua = "Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Mobile Safari/537.36 Edg/141.0.0.0"
	extraHeaders := network.Headers{
		"Accept":          "application/json, text/javascript, */*; q=0.01",
		"Accept-Language": "zh-CN,zh;q=0.9",
		"Cache-Control":   "no-cache",
		"Pragma":          "no-cache",
		"Origin":          "https://m.sporttery.cn",
		"Referer":         "https://m.sporttery.cn/",
	}

	if err := chromedp.Run(cctx,
		network.Enable(),
		emulation.SetUserAgentOverride(ua),
		network.SetExtraHTTPHeaders(extraHeaders),
	); err != nil {
		return nil, err
	}

	bodyCh := make(chan []byte, 1)
	errCh := make(chan error, 1)
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	var targetReq network.RequestID
	chromedp.ListenTarget(taskCtx, func(ev interface{}) {
		switch ev := ev.(type) {
		case *network.EventResponseReceived:
			if targetReq == "" && strings.HasPrefix(ev.Response.URL, url) {
				targetReq = ev.RequestID
			}
		case *network.EventLoadingFinished:
			if targetReq != "" && ev.RequestID == targetReq {
				body, err := network.GetResponseBody(ev.RequestID).Do(taskCtx)
				if err != nil {
					select {
					case errCh <- err:
					default:
					}
					return
				}
				select {
				case bodyCh <- body:
				default:
				}
			}
		case *network.EventLoadingFailed:
			if targetReq != "" && ev.RequestID == targetReq {
				select {
				case errCh <- errors.New(ev.ErrorText):
				default:
				}
			}
		}
	})

	if err := chromedp.Run(cctx,
		chromedp.Navigate("https://m.sporttery.cn/"),
		chromedp.WaitReady("body", chromedp.ByQuery),
	); err != nil {
		return nil, fmt.Errorf("warmup navigate failed: %w", err)
	}

	if err := chromedp.Run(cctx,
		chromedp.Navigate(url),
	); err != nil {
		return nil, fmt.Errorf("navigate api failed: %w", err)
	}

	select {
	case body := <-bodyCh:
		if len(body) > 0 {
			return body, nil
		}
	case err := <-errCh:
		return nil, fmt.Errorf("network body capture failed: %w", err)
	case <-timer.C:
	case <-cctx.Done():
		return nil, fmt.Errorf("wait response timeout: %w", cctx.Err())
	}

	// 兜底：直接从 <pre> 读取文本
	var preText string
	fallbackCtx, cancelFallback := context.WithTimeout(taskCtx, 10*time.Second)
	defer cancelFallback()

	if err := chromedp.Run(fallbackCtx,
		chromedp.WaitVisible("pre", chromedp.ByQuery),
		chromedp.Text("pre", &preText, chromedp.ByQuery),
	); err != nil {
		return nil, fmt.Errorf("fetch via JS err or empty; navigate+read<pre> err=%v; network_body_empty", err)
	}

	return []byte(preText), nil
}

// ---------------- fetch business ----------------
func fetchOfficial() ([]matchOdds, error) {
	const url = "https://webapi.sporttery.cn/gateway/uniform/football/getMatchCalculatorV1.qry?poolCode=&channel=c"

	// 确保有可用的 WS（优先用 CHROME_WS，失效则自动发现）
	if chromeWS == "" {
		ws, err := discoverWS()
		if err != nil {
			return nil, fmt.Errorf("browser discovery failed: %w", err)
		}
		chromeWS = ws
		L("info", "chromedp connected: %s", chromeWS)
	}

	start := time.Now()
	raw, err := chromeFetchJSON(ctx, url, chromeWS, 35*time.Second)
	if err != nil {
		// WS 可能失效 → 重新发现一次再试
		L("warn", "browser fetch failed: %v; rediscover…", err)
		chromeWS = ""
		if ws, derr := discoverWS(); derr == nil {
			chromeWS = ws
			L("info", "chromedp reconnected: %s", chromeWS)
			raw, err = chromeFetchJSON(ctx, url, chromeWS, 35*time.Second)
		}
	}
	if err != nil {
		return nil, fmt.Errorf("browser fetch failed: %w", err)
	}
	L("info", "200 OK(JSON) via chromedp, cost=%s len=%d", time.Since(start), len(raw))

	var root map[string]any
	if uerr := json.Unmarshal(raw, &root); uerr != nil {
		// 不是 JSON？打印前 160 字符辅助定位
		snip := string(raw[:min(160, len(raw))])
		return nil, fmt.Errorf("unmarshal JSON failed: %v body=%q", uerr, snip)
	}

	val, ok := root["value"].(map[string]any)
	if !ok {
		L("warn", "response has no value (maybe off-sale/idle)")
		return nil, nil
	}
	list, ok := val["matchInfoList"].([]any)
	if !ok {
		L("warn", "response has no matchInfoList (maybe no fixtures today)")
		return nil, nil
	}

	out := make([]matchOdds, 0, 256)
	for _, m := range list {
		if subList, ok2 := m.(map[string]any)["subMatchList"].([]any); ok2 {
			for _, s := range subList {
				out = append(out, cleanAndFormat(s.(map[string]any)))
			}
		}
	}
	return out, nil
}

// ---------------- redis write ----------------
func fieldsChanged(old map[string]string, new map[string]string) (bool, []string) {
	var diffs []string
	changed := false
	for k, v := range new {
		if k == "ts" {
			continue
		}
		ov := old[k]
		if ov != v {
			changed = true
			diffs = append(diffs, fmt.Sprintf("%s:%s->%s", k, ov, v))
		}
	}
	return changed, diffs
}
func hgetAllString(rdb *redis.Client, key string) (map[string]string, error) {
	res, err := rdb.HGetAll(ctx, key).Result()
	if err != nil {
		return nil, err
	}
	return res, nil
}
func saveOddsToRedis(items []matchOdds) error {
	ts := time.Now().UnixMilli()
	pipe := rdb.Pipeline()

	totalPlays := map[string]int{"mspf": 0, "rqspf": 0, "jqs": 0, "bqc": 0, "bf": 0}
	appendCnt := 0
	updateCnt := 0
	notifyCnt := 0

	var sampleID string

	for _, it := range items {
		if it.id == "" {
			continue
		}
		if sampleID == "" {
			sampleID = it.id
		}
		date := it.date
		if date == "" {
			date = time.Now().Format("2006-01-02")
		}

		pipe.SAdd(ctx, "jczq:ids:"+date, it.id)
		pipe.Expire(ctx, "jczq:ids:"+date, 7*24*time.Hour)

		for play, fields := range it.odds {
			totalPlays[play]++

			key := "jczq:odds:" + it.id + ":" + play
			streamKey := "jczq:odds:hist:" + it.id + ":" + play

			newMap := map[string]interface{}{}
			for k, v := range fields {
				newMap[k] = v
			}
			newMap["ts"] = strconv.FormatInt(ts, 10)

			doAppend := true
			var diffs []string
			if appendOnChg {
				old, err := hgetAllString(rdb, key)
				if err != nil && !errors.Is(err, redis.Nil) {
					L("warn", "HGETALL fail key=%s err=%v", key, err)
				}
				changed, ds := fieldsChanged(old, fields)
				diffs = ds
				doAppend = changed
			}

			if err := pipe.HSet(ctx, key, newMap).Err(); err != nil {
				L("error", "HSET fail key=%s err=%v", key, err)
			} else {
				updateCnt++
			}
			_ = pipe.PExpire(ctx, key, oddsExpire)

			if doAppend {
				if err := pipe.XAdd(ctx, &redis.XAddArgs{
					Stream: streamKey, MaxLen: streamMaxLen, Approx: true, Values: newMap,
				}).Err(); err != nil {
					L("warn", "XADD fail stream=%s err=%v", streamKey, err)
				} else {
					appendCnt++
				}
				_ = pipe.Expire(ctx, streamKey, oddsExpire)
			}
			if doAppend {
				if err := pipe.Publish(ctx, "jczq:odds:updated", it.id).Err(); err != nil {
					L("warn", "PUBLISH fail mid=%s err=%v", it.id, err)
				} else {
					notifyCnt++
				}
			}

			if logAtLeast("debug") {
				if appendOnChg {
					L("debug", "mid=%s play=%s snapshot=ok append=%v diffs=%v", it.id, play, doAppend, diffs)
				} else {
					L("debug", "mid=%s play=%s snapshot=ok append=true (on-change disabled)", it.id, play)
				}
			}
		}
	}

	_, err := pipe.Exec(ctx)
	if err != nil {
		return err
	}

	L("info", "write done: matches=%d sample_id=%s | plays(mspf=%d rqspf=%d jqs=%d bqc=%d bf=%d) | updated=%d appended=%d notified=%d",
		len(items), sampleID, totalPlays["mspf"], totalPlays["rqspf"], totalPlays["jqs"], totalPlays["bqc"], totalPlays["bf"],
		updateCnt, appendCnt, notifyCnt)
	return nil
}

// ---------------- schedule ----------------
func runOnceWithJitter() {
	delay := 0
	if jitterMaxSec > 0 {
		delay = rand.Intn(jitterMaxSec + 1)
	}
	L("info", "start job: jitter=%ds …", delay)
	if delay > 0 {
		time.Sleep(time.Duration(delay) * time.Second)
	}

	start := time.Now()
	items, err := fetchOfficial()
	if err != nil {
		L("error", "fetch failed (browser-only): %v", err)
		return
	}
	if len(items) == 0 {
		L("warn", "0 matches (maybe off-sale or no fixtures now)")
		return
	}
	L("info", "parsed: matches=%d (first id=%s), cost=%s", len(items), items[0].id, time.Since(start))

	if err := saveOddsToRedis(items); err != nil {
		L("error", "save to redis failed: %v", err)
		return
	}
	L("info", "done, total=%s", time.Since(start))
}
func scheduleJobs(s *gocron.Scheduler) {
	spec := getenv("ODDS_CRON", "*/3 11-21 * * *")
	_, _ = s.Cron(spec).Do(runOnceWithJitter)
	L("info", "cron scheduled: %s", spec)
}

// ---------------- main ----------------
func main() {
	if err := rdb.Set(ctx, "odds_connection_test", "ok", time.Minute).Err(); err != nil {
		log.Fatalf("Redis write failed: %v", err)
	}
	if v, _ := rdb.Get(ctx, "odds_connection_test").Result(); v != "ok" {
		log.Fatalf("Redis RW check failed, got=%s", v)
	}
	L("info", "Redis RW OK")

	L("info", "run once on start (with jitter)…")
	runOnceWithJitter()

	s := gocron.NewScheduler(time.FixedZone("Asia/Shanghai", 8*3600))
	scheduleJobs(s)
	L("info", "scheduler started (StreamMaxLen=%d, Expire=%s, Jitter<=%ds, OnChange=%v)",
		streamMaxLen, oddsExpire, jitterMaxSec, appendOnChg)
	s.StartBlocking()
}
