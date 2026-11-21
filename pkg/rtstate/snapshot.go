package rtstate

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// Snapshot is the rolling real-time view over the last WindowSeconds.
type Snapshot struct {
	AsOf               time.Time        `json:"as_of"`
	WindowSeconds      int              `json:"window_seconds"`
	Events             int64            `json:"events"`
	Purchases          int64            `json:"purchases"`
	Revenue            string           `json:"revenue"`
	PaymentSuccess     int64            `json:"payment_success"`
	PaymentFailure     int64            `json:"payment_failure"`
	PaymentSuccessRate float64          `json:"payment_success_rate"`
	RateLimited        int64            `json:"rate_limited"`
	EventsPerSecond    float64          `json:"events_per_second"`
	ByType             map[string]int64 `json:"by_type"`
	ByRegion           map[string]int64 `json:"by_region"`
	ActiveUsers        int64            `json:"active_users"`
	Series             []SecondPoint    `json:"series"`
}

// SecondPoint is one event-time second of the rolling series.
type SecondPoint struct {
	Second int64 `json:"second"`
	Events int64 `json:"events"`
}

// ReadOptions bounds a snapshot read.
type ReadOptions struct {
	ActiveWindow time.Duration // users seen within this horizon count as active
	MaxWindow    time.Duration // clamp (bucket retention); 0 = 15m
}

// ReadSnapshot sums the second buckets in (now-window, now] in one pipeline and
// counts active users. It is read-only and safe to call from any service.
func ReadSnapshot(ctx context.Context, rdb redis.Cmdable, now time.Time, window time.Duration, opt ReadOptions) (Snapshot, error) {
	if opt.MaxWindow <= 0 {
		opt.MaxWindow = 15 * time.Minute
	}
	if opt.ActiveWindow <= 0 {
		opt.ActiveWindow = 5 * time.Minute
	}
	if window < time.Second {
		window = time.Second
	}
	if window > opt.MaxWindow {
		window = opt.MaxWindow
	}
	n := int(window / time.Second)
	end := now.Unix()
	start := end - int64(n) + 1

	pipe := rdb.Pipeline()
	cmds := make([]*redis.MapStringStringCmd, n)
	for i := 0; i < n; i++ {
		cmds[i] = pipe.HGetAll(ctx, SecondBucketKey(start+int64(i)))
	}
	activeCmd := pipe.ZCount(ctx, ActiveUsersKey(),
		"("+strconv.FormatInt(now.Add(-opt.ActiveWindow).UnixMilli(), 10), "+inf")
	if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
		return Snapshot{}, err
	}

	snap := Snapshot{
		AsOf:          now,
		WindowSeconds: n,
		ByType:        map[string]int64{},
		ByRegion:      map[string]int64{},
		Series:        make([]SecondPoint, n),
	}
	var cents int64
	for i, c := range cmds {
		pt := SecondPoint{Second: start + int64(i)}
		for k, v := range c.Val() {
			x, err := strconv.ParseInt(v, 10, 64)
			if err != nil {
				continue
			}
			switch {
			case k == "events":
				snap.Events += x
				pt.Events = x
			case k == "purchases":
				snap.Purchases += x
			case k == "revenue_cents":
				cents += x
			case k == "pay_ok":
				snap.PaymentSuccess += x
			case k == "pay_fail":
				snap.PaymentFailure += x
			case k == "rate_limited":
				snap.RateLimited += x
			case strings.HasPrefix(k, "type:"):
				snap.ByType[strings.TrimPrefix(k, "type:")] += x
			case strings.HasPrefix(k, "region:"):
				snap.ByRegion[strings.TrimPrefix(k, "region:")] += x
			}
		}
		snap.Series[i] = pt
	}
	snap.Revenue = CentsString(cents)
	snap.EventsPerSecond = float64(snap.Events) / float64(n)
	if p := snap.PaymentSuccess + snap.PaymentFailure; p > 0 {
		snap.PaymentSuccessRate = float64(snap.PaymentSuccess) / float64(p)
	}
	snap.ActiveUsers = activeCmd.Val()
	return snap, nil
}

// CentsString formats integer cents as a decimal string ("1234.56").
func CentsString(c int64) string {
	sign := ""
	if c < 0 {
		sign, c = "-", -c
	}
	return fmt.Sprintf("%s%d.%02d", sign, c/100, c%100)
}
