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

	// 统一 TTL 与 Stream 水位
	ttl54h       = 54 * time.Hour
	streamMaxLen = int64(120)

	redisClient *redis.Client

	// 浏览器发现/连接
	chromeDiscovery string
	chromeWS        string
	discoveryClient *http.Client
)

/* ======================= 环境/初始化 ======================= */

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func mustAtoiEnv(envKey, def string) int {
	if v := os.Getenv(envKey); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	n, _ := strconv.Atoi(def)
	return n
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func newRedisClient() *redis.Client {
	host := getenv("REDIS_HOST", "redis-15892.c273.us-east-1-2.ec2.redns.redis-cloud.com")
	port := mustAtoiEnv("REDIS_PORT", "15892")
	user := getenv("REDIS_USER", "default")
	pass := getenv("REDIS_PASS", "")

	// 显式配置优先；否则按常识推断
	tlsEnv := strings.TrimSpace(os.Getenv("REDIS_TLS"))
	useTLS := false
	if tlsEnv != "" {
		useTLS = strings.EqualFold(tlsEnv, "true")
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

	rdb := redis.NewClient(opts)
	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Fatalf("❌ Redis 连接失败: %v (addr=%s tls=%v)", err, addr, useTLS)
	}
	log.Printf("✅ 已连接 Redis: %s (TLS=%v)\n", addr, useTLS)
	return rdb
}

func init() {
	_ = godotenv.Load(".env") // 可选：读取当前目录的 .env

	rand.Seed(time.Now().UnixNano())
	log.SetFlags(log.LstdFlags | log.Lmsgprefix)
	log.SetPrefix("[fetcher] ")

	redisClient = newRedisClient()

	chromeDiscovery = strings.TrimSpace(getenv("CHROME_DISCOVERY", "http://chrome:9222/json/version"))
	chromeWS = strings.TrimSpace(os.Getenv("CHROME_WS"))

	// 发现 DevTools 的专用 HTTP 客户端（禁用代理，短超时）
	discoveryTransport := &http.Transport{
		Proxy: nil, // 禁用环境代理，避免 9222 traffic 走系统代理
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

	log.Printf("🌐 Chrome discovery=%s ws_override=%q\n", chromeDiscovery, chromeWS)
}

/* ======================= 数据结构/工具 ======================= */

type OutputData struct {
	LotteryNumber struct {
		MatchNumStr string `json:"matchNumStr"`
		SellStatus  string `json:"sellStatus"`
		TaxDateNo   string `json:"taxDateNo"`
		MatchID     string `json:"taxDateNo+matchNumStr_lastnm"`
	} `json:"Lottery_Number"`
	MatchInfo struct {
		League    string `json:"league"`
		HomeTeam  string `json:"home_team"`
		AwayTeam  string `json:"away_team"`
		DateTime  string `json:"date_time"`
		MatchTime string `json:"matchTime"`
	} `json:"match_info"`
	GoalLine string `json:"goal_line,omitempty"`
}

func toString(v any) string {
	switch t := v.(type) {
	case string:
		return t
	case float64:
		// 比赛号等整数字段
		if t == float64(int64(t)) {
			return fmt.Sprintf("%.0f", t)
		}
		return strconv.FormatFloat(t, 'f', -1, 64)
	default:
		return ""
	}
}

func safeMatchID(taxDateNo, matchNumStr string) string {
	s := matchNumStr
	if len(s) > 3 {
		s = s[len(s)-3:]
	}
	return taxDateNo + s
}

func parseCNTime(loc *time.Location, date, tm string) (time.Time, error) {
	layouts := []string{"2006-01-02 15:04:05", "2006-01-02 15:04"}
	var last error
	for _, l := range layouts {
		if ts, err := time.ParseInLocation(l, date+" "+tm, loc); err == nil {
			return ts, nil
		} else {
			last = err
		}
	}
	return time.Time{}, last
}

/* ======================= DevTools 自动发现 ======================= */

func discoverChromeWS() (string, error) {
	base := chromeDiscovery
	if base == "" {
		base = "http://chrome:9222/json/version"
	}

	// 解析 chrome 容器的 IP（兜底直连）
	ip := ""
	if addrs, err := net.LookupHost("chrome"); err == nil && len(addrs) > 0 {
		ip = addrs[0]
	}

	type probe struct {
		URL       string
		HostHdr   string
		ParseMode string
		Label     string
	}

	var probes []probe

	// /json/version 常规探测
	versionURL := strings.Replace(base, "127.0.0.1:9222", "chrome:9222", 1)
	probes = append(probes, probe{
		URL:       versionURL,
		ParseMode: "version",
		Label:     "version@chrome",
	})
	probes = append(probes, probe{
		URL:       versionURL,
		HostHdr:   "127.0.0.1:9222",
		ParseMode: "version",
		Label:     "version@chrome host=127.0.0.1",
	})

	// /json 列表兜底
	jsonList := strings.Replace(versionURL, "/json/version", "/json", 1)
	if jsonList == versionURL {
		jsonList = "http://chrome:9222/json"
	}
	probes = append(probes, probe{
		URL:       jsonList,
		ParseMode: "list",
		Label:     "list@chrome",
	})
	probes = append(probes, probe{
		URL:       jsonList,
		HostHdr:   "127.0.0.1:9222",
		ParseMode: "list",
		Label:     "list@chrome host=127.0.0.1",
	})

	// 若解析到 IP，再尝试用 IP 直连
	if ip != "" {
		ipVersion := strings.Replace(versionURL, "chrome:9222", ip+":9222", 1)
		ipList := strings.Replace(jsonList, "chrome:9222", ip+":9222", 1)

		probes = append(probes, probe{
			URL:       ipVersion,
			ParseMode: "version",
			Label:     "version@ip",
		})
		probes = append(probes, probe{
			URL:       ipVersion,
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
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		return body, resp.StatusCode, nil
	}

	var lastErr error
	for round := 0; round < 3; round++ {
		for _, p := range probes {
			body, code, err := do(p)
			if err != nil {
				lastErr = fmt.Errorf("%s -> request error: %w", p.Label, err)
				continue
			}
			if code < 200 || code >= 300 {
				lastErr = fmt.Errorf("%s -> http %d body=%q", p.Label, code, string(body[:min(120, len(body))]))
				continue
			}

			switch p.ParseMode {
			case "version":
				var vr map[string]any
				if err := json.Unmarshal(body, &vr); err != nil {
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
				if err := json.Unmarshal(body, &arr); err != nil {
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
		backoff := time.Duration(400*(1<<round)) * time.Millisecond
		time.Sleep(backoff)
	}

	if lastErr == nil {
		lastErr = errors.New("discovery exhausted without specific error")
	}
	return "", lastErr
}

/* ======================= 通过浏览器取 JSON ======================= */
func chromeFetchJSON(c context.Context, wsURL, apiURL string, timeout time.Duration) ([]byte, error) {
	if wsURL == "" {
		return nil, errors.New("chromeFetchJSON: empty wsURL")
	}
	if apiURL == "" {
		return nil, errors.New("chromeFetchJSON: empty apiURL")
	}

	// 连接到远程 Chrome（容器里的 headless-shell）
	allocCtx, cancelAlloc := chromedp.NewRemoteAllocator(c, wsURL)
	defer cancelAlloc()
	taskCtx, cancelTask := chromedp.NewContext(allocCtx)
	defer cancelTask()

	if timeout <= 0 {
		timeout = 35 * time.Second
	}
	ctx, cancel := context.WithTimeout(taskCtx, timeout)
	defer cancel()

	// 统一 UA（移动端）
	const ua = "Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Mobile Safari/537.36 Edg/141.0.0.0"
	extraHeaders := network.Headers{
		"Accept":          "application/json, text/javascript, */*; q=0.01",
		"Accept-Language": "zh-CN,zh;q=0.9",
		"Cache-Control":   "no-cache",
		"Pragma":          "no-cache",
		"Origin":          "https://m.sporttery.cn",
		"Referer":         "https://m.sporttery.cn/",
	}

	if err := chromedp.Run(ctx,
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
			if targetReq == "" && strings.HasPrefix(ev.Response.URL, apiURL) {
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

	// 预热到空白页，避免站点脚本发起无关 XHR/CORS 噪声
	if err := chromedp.Run(ctx,
		chromedp.Navigate("about:blank"),
	); err != nil {
		return nil, fmt.Errorf("warmup navigate failed: %w", err)
	}

	if err := chromedp.Run(ctx,
		chromedp.Navigate(apiURL),
	); err != nil {
		return nil, fmt.Errorf("navigate api failed: %w", err)
	}

	var (
		body    []byte
		waitErr error
	)

	select {
	case body = <-bodyCh:
	case waitErr = <-errCh:
	case <-timer.C:
	case <-ctx.Done():
		waitErr = ctx.Err()
	}

	if len(body) > 0 {
		return body, nil
	}
	if waitErr != nil {
		msg := waitErr.Error()
		if strings.Contains(msg, "invalid context") || strings.Contains(msg, "target closed") {
			log.Printf("ℹ️ network body capture fallback: %v", waitErr)
		} else {
			return nil, fmt.Errorf("network body capture failed: %w", waitErr)
		}
	}
	if err := ctx.Err(); err != nil && !errors.Is(err, context.DeadlineExceeded) {
		return nil, fmt.Errorf("wait response failed: %w", err)
	}

	var pre string
	fallbackCtx, cancelNav := context.WithTimeout(taskCtx, 10*time.Second)
	defer cancelNav()

	if err := chromedp.Run(fallbackCtx,
		chromedp.WaitVisible("pre", chromedp.ByQuery),
		chromedp.Text("pre", &pre, chromedp.ByQuery),
	); err != nil {
		return nil, fmt.Errorf("fetch via JS failed and no <pre> fallback: %w | network_body_empty", err)
	}

	return []byte(pre), nil
}


/* ======================= 业务抓取（全走浏览器容器） ======================= */

func fetchMatches() ([]OutputData, error) {
	target := "https://webapi.sporttery.cn/gateway/uniform/football/getMatchCalculatorV1.qry?poolCode=&channel=c"

	// 1) 获取 DevTools WS（支持由环境指定，失败自动发现）
	if chromeWS == "" {
		ws, err := discoverChromeWS()
		if err != nil {
			return nil, fmt.Errorf("devtools discover failed: %w", err)
		}
		chromeWS = ws
		log.Printf("🌐 DevTools WS = %s", chromeWS)
	}

	// 2) 通过浏览器抓
	start := time.Now()
	raw, err := chromeFetchJSON(ctx, chromeWS, target, 35*time.Second)
	if err != nil {
		log.Printf("⚠️ browser fetch failed: %v; rediscovering DevTools…", err)
		chromeWS = ""
		ws, derr := discoverChromeWS()
		if derr != nil {
			return nil, fmt.Errorf("browser fetch failed: %v (rediscover err=%v)", err, derr)
		}
		chromeWS = ws
		log.Printf("🌐 DevTools WS refreshed: %s", chromeWS)
		start = time.Now()
		raw, err = chromeFetchJSON(ctx, chromeWS, target, 35*time.Second)
	}
	if err != nil {
		return nil, fmt.Errorf("browser fetch failed after rediscover: %w", err)
	}
	log.Printf("✅ 200 OK via browser, cost=%s len=%d", time.Since(start), len(raw))

	// 3) 解析 JSON
	var root map[string]any
	if err := json.Unmarshal(raw, &root); err != nil {
		// 如遇非 JSON，把前 200 字符打印出来辅助定位
		s := string(raw)
		if len(s) > 200 {
			s = s[:200]
		}
		return nil, fmt.Errorf("JSON 解析失败: %w | head=%q", err, s)
	}

	value, ok := root["value"].(map[string]any)
	if !ok {
		log.Println("⚠️ 无 value 字段，可能停售/空档期")
		return []OutputData{}, nil
	}
	rawList, exists := value["matchInfoList"]
	if !exists {
		log.Println("⚠️ 无 matchInfoList")
		return []OutputData{}, nil
	}
	arr, ok := rawList.([]any)
	if !ok {
		log.Println("⚠️ matchInfoList 类型异常")
		return []OutputData{}, nil
	}

	res := make([]OutputData, 0)
	for _, m := range arr {
		subs, ok := m.(map[string]any)["subMatchList"].([]any)
		if !ok {
			continue
		}
		for _, s := range subs {
			sm := s.(map[string]any)
			var out OutputData
			out.LotteryNumber.MatchNumStr = toString(sm["matchNumStr"])
			out.LotteryNumber.SellStatus = toString(sm["sellStatus"])
			out.LotteryNumber.TaxDateNo = toString(sm["taxDateNo"])
			out.LotteryNumber.MatchID = safeMatchID(out.LotteryNumber.TaxDateNo, out.LotteryNumber.MatchNumStr)

			out.MatchInfo.League = toString(sm["leagueAllName"])
			out.MatchInfo.HomeTeam = toString(sm["homeTeamAllName"])
			out.MatchInfo.AwayTeam = toString(sm["awayTeamAllName"])
			out.MatchInfo.DateTime = toString(sm["matchDate"])
			out.MatchInfo.MatchTime = toString(sm["matchTime"])

			if hhad, ok := sm["hhad"].(map[string]any); ok {
				out.GoalLine = toString(hhad["goalLineValue"])
			}
			res = append(res, out)
		}
	}
	return res, nil
}

/* ======================= Redis 写入/监控 ======================= */

func pushToStream(m OutputData, ts int64) error {
	id, err := redisClient.XAdd(ctx, &redis.XAddArgs{
		Stream: "new_matches_stream",
		MaxLen: streamMaxLen,
		Approx: true,
		Values: map[string]any{
			"match_id":    m.LotteryNumber.MatchID,
			"timestamp":   ts,
			"home_team":   m.MatchInfo.HomeTeam,
			"away_team":   m.MatchInfo.AwayTeam,
			"league":      m.MatchInfo.League,
			"datetime":    m.MatchInfo.DateTime,
			"match_time":  m.MatchInfo.MatchTime,
			"goal_line":   m.GoalLine,
			"sell_status": m.LotteryNumber.SellStatus,
		},
	}).Result()
	if err != nil {
		return fmt.Errorf("XADD 失败: %w", err)
	}
	fmt.Printf("🚀 Stream 推送: %s -> %s\n", m.LotteryNumber.MatchID, id)
	return nil
}

func storeReadableIndex(m OutputData) error {
	norm := func(s string) string { return strings.TrimSpace(s) }
	key := fmt.Sprintf("match_index:%s:%s:%s",
		norm(m.MatchInfo.HomeTeam), norm(m.MatchInfo.AwayTeam), m.LotteryNumber.MatchNumStr)
	val := map[string]any{
		"match_id":  m.LotteryNumber.MatchID,
		"date":      m.MatchInfo.DateTime,
		"time":      m.MatchInfo.MatchTime,
		"goal_line": m.GoalLine,
		"home":      m.MatchInfo.HomeTeam,
		"away":      m.MatchInfo.AwayTeam,
	}
	if err := redisClient.HSet(ctx, key, val).Err(); err != nil {
		return fmt.Errorf("HSET %s 失败: %w", key, err)
	}
	_ = redisClient.Expire(ctx, key, ttl54h).Err()
	fmt.Printf("📌 可读索引: %s -> %+v\n", key, val)
	return nil
}

func storeToRedis(matches []OutputData) {
	loc, _ := time.LoadLocation("Asia/Shanghai")

	for _, m := range matches {
		id := m.LotteryNumber.MatchID
		if id == "" {
			continue
		}
		t, err := parseCNTime(loc, m.MatchInfo.DateTime, m.MatchInfo.MatchTime)
		if err != nil {
			log.Printf("❌ 时间解析失败：%s %s (%v)", m.MatchInfo.DateTime, m.MatchInfo.MatchTime, err)
			continue
		}
		ts := t.Unix()

		ok, err := redisClient.SetNX(ctx, "match_id:"+id, "1", ttl54h).Result()
		if err != nil {
			log.Printf("❌ SetNX 失败 match_id:%s: %v", id, err)
			continue
		}
		if !ok {
			continue // 不是新增，跳过
		}

		if err := redisClient.ZAdd(ctx, "match_schedule", redis.Z{
			Score:  float64(ts),
			Member: id,
		}).Err(); err != nil {
			log.Printf("❌ ZADD 失败 %s: %v", id, err)
		}

		if err := pushToStream(m, ts); err != nil {
			log.Printf("❌ Stream 推送失败 %s: %v", id, err)
		}

		if err := storeReadableIndex(m); err != nil {
			log.Printf("⚠️ 可读索引写入失败 %s: %v", id, err)
		}

		fmt.Printf("✅ 新增比赛：%s  时间戳：%d\n", id, ts)
	}

	// 清理 72h 前的时间索引；并确保流不超过 120
	cutoff := float64(time.Now().Add(-72 * time.Hour).Unix())
	_ = redisClient.ZRemRangeByScore(ctx, "match_schedule", "0", strconv.FormatFloat(cutoff, 'f', -1, 64)).Err()
	_ = redisClient.XTrimMaxLen(ctx, "new_matches_stream", streamMaxLen).Err()
}

func initStreamConsumerGroup() {
	if err := redisClient.XGroupCreateMkStream(ctx, "new_matches_stream", "match_processors", "0").Err(); err != nil &&
		!strings.Contains(err.Error(), "BUSYGROUP") {
		log.Printf("⚠️ 创建消费组失败: %v", err)
		return
	}
	log.Println("✅ 消费组 'match_processors' 就绪")
}

func printStreamStats() {
	info, err := redisClient.XInfoStream(ctx, "new_matches_stream").Result()
	if err != nil {
		if strings.Contains(err.Error(), "No such key") {
			log.Println("ℹ️ Stream 尚无数据")
			return
		}
		log.Printf("⚠️ 获取 Stream 信息失败: %v", err)
		return
	}
	fmt.Printf("📊 Stream: len=%d groups=%d last-id=%s\n", info.Length, info.Groups, info.LastGeneratedID)
}

func debugRedisData() {
	it := redisClient.Scan(ctx, 0, "match_index:*", 3).Iterator()
	fmt.Println("🔎 采样 match_index:*")
	for it.Next(ctx) {
		k := it.Val()
		vals, _ := redisClient.HGetAll(ctx, k).Result()
		fmt.Printf(" - %s => %+v\n", k, vals)
	}
	if err := it.Err(); err != nil {
		log.Printf("⚠️ SCAN 出错: %v", err)
	}
	zs, _ := redisClient.ZRangeWithScores(ctx, "match_schedule", 0, 2).Result()
	fmt.Printf("⏱  match_schedule 前3: %+v\n", zs)
	printStreamStats()
}

/* ======================= 调度 ======================= */

func scheduledTask(fast bool) {
	// 在 3 分钟 Cron 基础上，额外增加 60–90 秒随机延迟
	delay := time.Duration(60+rand.Intn(31)) * time.Second
	fmt.Printf("[%s] [%s] 额外延迟 %s 后开始抓取...\n",
		time.Now().Format("15:04:05"),
		map[bool]string{true: "高频", false: "常规"}[fast],
		delay)
	time.Sleep(delay)

	res, err := fetchMatches()
	if err != nil {
		log.Printf("抓取失败: %v\n", err)
		return
	}
	if len(res) > 0 {
		storeToRedis(res)
	}

	if !fast {
		printStreamStats()
	}
}

func scheduleJobs(s *gocron.Scheduler) {
	// 高频：11:00–13:59 每 3 分钟
	_, _ = s.Cron("*/3 11-13 * * *").Do(func() { scheduledTask(true) })
	// 高频：14:00–14:30 每 3 分钟
	_, _ = s.Cron("0-30/3 14 * * *").Do(func() { scheduledTask(true) })
	// 常规：15:15–22:15 每小时
	_, _ = s.Cron("15 15-22 * * *").Do(func() { scheduledTask(false) })
}

/* ======================= main ======================= */

func main() {
	// 健康检查
	if err := redisClient.Set(ctx, "connection_test", "ok", time.Minute).Err(); err != nil {
		log.Fatalf("❌ Redis 写入失败: %v", err)
	}
	if v, _ := redisClient.Get(ctx, "connection_test").Result(); v != "ok" {
		log.Fatalf("❌ Redis 读写校验失败, got=%s", v)
	}
	log.Println("✅ Redis 读写 OK")

	initStreamConsumerGroup()

	s := gocron.NewScheduler(time.FixedZone("Asia/Shanghai", 8*3600))
	scheduleJobs(s)

	fmt.Println("容器启动，立即执行抓取任务（常规）...")
	scheduledTask(false)
	fmt.Println("抓取后打印采样：")
	debugRedisData()

	fmt.Println("调度器启动，等待任务...")
	fmt.Println("💡 Redis 命令：XLEN new_matches_stream | XRANGE new_matches_stream - + COUNT 5 | XINFO GROUPS new_matches_stream")
	s.StartBlocking()
}
