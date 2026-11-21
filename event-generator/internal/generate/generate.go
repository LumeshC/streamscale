package generate

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/google/uuid"
	"github.com/streamscale/streamscale/pkg/event"
)

var regions = []string{"us-east-1", "us-west-2", "eu-west-1", "ap-south-1"}

var products = []string{"sku_watch", "sku_headphones", "sku_keyboard", "sku_hoodie", "sku_mug"}

var paths = []string{"/", "/pricing", "/docs", "/search", "/checkout", "/account"}

var queries = []string{"kafka", "golang", "analytics", "postgres", "streaming"}

type weighted struct {
	t      event.Type
	weight int
}

var mix = []weighted{
	{event.TypePageView, 40},
	{event.TypeSearch, 20},
	{event.TypePurchase, 12},
	{event.TypePaymentSuccess, 10},
	{event.TypePaymentFailure, 5},
	{event.TypeLogin, 8},
	{event.TypeLogout, 5},
}

type Generator struct {
	rng   *rand.Rand
	users []string
}

func New(seed int64, userPool int) *Generator {
	if userPool < 1 {
		userPool = 250
	}
	rng := rand.New(rand.NewSource(seed))
	users := make([]string, userPool)
	for i := range users {
		users[i] = uuid.NewString()
	}
	return &Generator{rng: rng, users: users}
}

func (g *Generator) Next() event.Event {
	t := g.pickType()
	user := g.users[g.rng.Intn(len(g.users))]
	e := event.Event{
		EventID:   uuid.NewString(),
		EventType: t,
		UserID:    user,
		Timestamp: time.Now().UTC(),
		Region:    regions[g.rng.Intn(len(regions))],
		Metadata: map[string]string{
			"source":     "event-generator",
			"session_id": fmt.Sprintf("ses_%d", g.rng.Intn(10_000)),
			"path":       paths[g.rng.Intn(len(paths))],
		},
	}
	if t == event.TypeSearch {
		e.Metadata["query"] = queries[g.rng.Intn(len(queries))]
	}
	if t.RequiresCommerce() {
		e.ProductID = products[g.rng.Intn(len(products))]
		cents := 199 + g.rng.Intn(19800)
		e.Amount = fmt.Sprintf("%d.%02d", cents/100, cents%100)
		e.Currency = "USD"
	}
	return e
}

func (g *Generator) pickType() event.Type {
	total := 0
	for _, w := range mix {
		total += w.weight
	}
	n := g.rng.Intn(total)
	acc := 0
	for _, w := range mix {
		acc += w.weight
		if n < acc {
			return w.t
		}
	}
	return event.TypePageView
}
