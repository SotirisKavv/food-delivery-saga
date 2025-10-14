package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

type Item struct {
	SKU      string `json:"sku"`
	Quantity int64  `json:"quantity"`
}

type OrderRequest struct {
	CustomerId      string          `json:"customer_id"`
	RestaurantId    string          `json:"restaurant_id"`
	Items           map[string]Item `json:"items"`
	PaymentMethodId string          `json:"pm_id"`
	Amount          int64           `json:"amount_cents"`
	Currency        string          `json:"currency"`
	DeliveryAddress string          `json:"delivery_address"`
}

type OrderResponse struct {
	OrderId       string `json:"order_id"`
	Status        string `json:"status"`
	Message       string `json:"message"`
	CorrelationID string `json:"correlation_id"`
}

// APIResponse mirrors the gateway's utils.Response shape
type APIResponse struct {
	Success bool            `json:"success"`
	Message string          `json:"message"`
	Data    json.RawMessage `json:"data"`
}

type scenario string

const (
	ScHappy         scenario = "happy"
	ScNotFound      scenario = "reservation_item_not_found"
	ScNotEnough     scenario = "reservation_not_enough_qty"
	ScPayFail       scenario = "payment_failure_random"
	ScRestoCapacity scenario = "restaurant_capacity"
)

func main() {
	rand.New(rand.NewSource(time.Now().UnixNano()))

	baseURL := flag.String("base", envOr("GATEWAY_BASE", "http://localhost:8080"), "API Gateway base URL (no trailing slash)")
	total := flag.Int("total", 10, "total number of synthetic orders to send in burst phase")
	conc := flag.Int("concurrency", 5, "concurrency for burst phase")
	pollTimeout := flag.Duration("timeout", 60*time.Second, "max time to wait for an order to finish (per order)")
	jitterMax := flag.Duration("jitter", 800*time.Millisecond, "max random jitter between requests in spike test")
	flag.Parse()

	client := &http.Client{Timeout: 10 * time.Second}

	log.Printf("Base URL: %s", *baseURL)

	// 1) Deterministic scenarios
	runScenario(client, *baseURL, ScHappy, *pollTimeout)
	runScenario(client, *baseURL, ScNotFound, *pollTimeout)
	runScenario(client, *baseURL, ScNotEnough, *pollTimeout)
	runScenario(client, *baseURL, ScRestoCapacity, *pollTimeout)
	// Payment failure is random; run a few to likely hit the failure path
	for i := 0; i < 5; i++ {
		runScenario(client, *baseURL, ScPayFail, *pollTimeout)
	}

	// 2) Burst & spikes
	log.Printf("Starting burst test: total=%d concurrency=%d", *total, *conc)
	burst(client, *baseURL, *total, *conc, *pollTimeout, *jitterMax)
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func runScenario(client *http.Client, base string, sc scenario, timeout time.Duration) {
	req := buildOrder(sc)
	id, err := createOrder(client, base, req)
	if err != nil {
		log.Printf("[%s] create failed: %v", sc, err)
		return
	}
	st, err := waitForStatus(client, base, id, timeout)
	if err != nil {
		log.Printf("[%s] wait failed for %s: %v", sc, id, err)
		return
	}
	log.Printf("[%s] result: order_id=%s status=%s", sc, id, st)
}

func burst(client *http.Client, base string, total, conc int, timeout, jitterMax time.Duration) {
	var wg sync.WaitGroup
	jobs := make(chan int)
	scenarios := []scenario{ScHappy, ScNotFound, ScNotEnough, ScPayFail, ScRestoCapacity}

	worker := func() {
		defer wg.Done()
		for range jobs {
			// random scenario
			sc := scenarios[rand.Intn(len(scenarios))]
			// random jitter to create spikes
			time.Sleep(time.Duration(rand.Int63n(int64(jitterMax))))
			runScenario(client, base, sc, timeout)
		}
	}

	for i := 0; i < conc; i++ {
		wg.Add(1)
		go worker()
	}
	for i := 0; i < total; i++ {
		jobs <- i
	}
	close(jobs)
	wg.Wait()
}

func buildOrder(sc scenario) OrderRequest {
	// Defaults
	req := OrderRequest{
		CustomerId:      "cust-" + randID(),
		RestaurantId:    "resto-1",
		Items:           map[string]Item{"burger": {SKU: "burger", Quantity: 1}, "fries": {SKU: "fries", Quantity: 1}},
		PaymentMethodId: "pm-111",
		Amount:          1500,
		Currency:        "USD",
		DeliveryAddress: "Main street 1",
	}

	switch sc {
	case ScHappy:
		// leave defaults
	case ScNotFound:
		req.Items = map[string]Item{"unknown": {SKU: "unknown", Quantity: 1}}
	case ScNotEnough:
		// inventory has ~50 burgers initially -> request 500
		req.Items = map[string]Item{"burger": {SKU: "burger", Quantity: 500}}
	case ScPayFail:
		// normal order; payment processor has ~10% failure probability
	case ScRestoCapacity:
		// try to exceed restaurant capacity with many burgers but within inventory
		// resto-1: PrepTime burger=20, ParallelizationFactor=2, CapacityMax=120
		// quantity 20 -> load ~200 > 120 after division? (20*20)/2 = 200 -> exceeds
		req.Items = map[string]Item{"burger": {SKU: "burger", Quantity: 20}}
		// ensure amount somewhat larger
		req.Amount = 30000
	}
	return req
}

func createOrder(client *http.Client, base string, req OrderRequest) (string, error) {
	b, _ := json.Marshal(req)
	url := strings.TrimRight(base, "/") + "/api/v1/orders"
	httpReq, _ := http.NewRequest("POST", url, bytes.NewReader(b))
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(httpReq)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return "", fmt.Errorf("unexpected status: %s", resp.Status)
	}
	var api APIResponse
	if err := json.NewDecoder(resp.Body).Decode(&api); err != nil {
		return "", err
	}
	if !api.Success {
		return "", fmt.Errorf("api returned success=false: %s", api.Message)
	}
	var or OrderResponse
	if err := json.Unmarshal(api.Data, &or); err != nil {
		return "", err
	}
	return or.OrderId, nil
}

func waitForStatus(client *http.Client, base, id string, timeout time.Duration) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	url := strings.TrimRight(base, "/") + "/api/v1/orders/" + id
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return "", fmt.Errorf("timeout waiting for order %s", id)
		case <-ticker.C:
			req, _ := http.NewRequestWithContext(ctx, "GET", url, nil)
			resp, err := client.Do(req)
			if err != nil {
				continue
			}
			if resp.StatusCode >= 300 {
				resp.Body.Close()
				continue
			}
			var api APIResponse
			decErr := json.NewDecoder(resp.Body).Decode(&api)
			resp.Body.Close()
			if decErr != nil {
				continue
			}
			var or OrderResponse
			if err := json.Unmarshal(api.Data, &or); err != nil {
				continue
			}
			if or.Status == "COMPLETED" || or.Status == "CANCELED" || or.Status == "AUTHORIZED" || or.Status == "ACCEPTED" || or.Status == "RESERVED" {
				return or.Status, nil
			}
		}
	}
}

func randID() string { return fmt.Sprintf("%04x", rand.Intn(65536)) }
