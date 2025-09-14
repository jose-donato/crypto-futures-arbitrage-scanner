package exchanges

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/websocket"
)

type BingXBookTickerData struct {
	Symbol   string `json:"s"`
	BestBid  string `json:"b"`
	BestAsk  string `json:"a"`
	BidQty   string `json:"B"`
	AskQty   string `json:"A"`
	Time     int64  `json:"T"`
}

type BingXWebSocketMessage struct {
	ID      string          `json:"id"`
	Code    int             `json:"code"`
	Msg     string          `json:"msg"`
	Data    json.RawMessage `json:"data"`
	DataType string         `json:"dataType"`
}

func ConnectBingXFutures(symbols []string, priceChan chan<- PriceData, orderbookChan chan<- OrderbookData, tradeChan chan<- TradeData) {
	wsURL := "wss://open-api-swap.bingx.com/swap-market"

	for {
		header := http.Header{}
		header.Add("Accept-Encoding", "gzip")

		conn, _, err := websocket.DefaultDialer.Dial(wsURL, header)
		if err != nil {
			log.Printf("BingX futures connection error: %v", err)
			time.Sleep(5 * time.Second)
			continue
		}

		log.Printf("Connected to BingX futures WebSocket")

		// Subscribe to book ticker for each symbol
		for _, symbol := range symbols {
			// Convert BTCUSDT to BTC-USDT format for BingX
			bingxSymbol := convertToBingXSymbol(symbol)
			subscribeMessage := map[string]any{
				"id":       fmt.Sprintf("bingx-%s-bookticker", symbol),
				"reqType":  "sub",
				"dataType": fmt.Sprintf("%s@bookTicker", bingxSymbol),
			}

			if err := conn.WriteJSON(subscribeMessage); err != nil {
				log.Printf("BingX subscription error for %s: %v", symbol, err)
			}
		}

		// Set up ping/pong mechanism
		go func(conn *websocket.Conn) {
			ticker := time.NewTicker(30 * time.Second)
			defer ticker.Stop()

			for {
				select {
				case <-ticker.C:
					if err := conn.WriteMessage(websocket.TextMessage, []byte("Ping")); err != nil {
						log.Printf("BingX ping error: %v", err)
						return
					}
				}
			}
		}(conn)

		for {
			messageType, message, err := conn.ReadMessage()
			if err != nil {
				log.Printf("BingX futures read error: %v", err)
				conn.Close()
				break
			}

			if err := handleBingXMessage(messageType, message, orderbookChan); err != nil {
				log.Printf("BingX message handling error: %v", err)
			}
		}

		time.Sleep(2 * time.Second)
	}
}

func handleBingXMessage(messageType int, message []byte, orderbookChan chan<- OrderbookData) error {
	var decodedMsg string
	var err error

	if messageType == websocket.TextMessage {
		decodedMsg = string(message)
	} else if messageType == websocket.BinaryMessage {
		decodedMsg, err = decodeGzip(message)
		if err != nil {
			return fmt.Errorf("gzip decode error: %v", err)
		}
	}

	// Handle ping/pong
	if strings.Contains(decodedMsg, "Ping") {
		return nil
	}

	// Skip empty messages
	if decodedMsg == "" {
		return nil
	}

	// Parse WebSocket message
	var wsMessage BingXWebSocketMessage
	if err := json.Unmarshal([]byte(decodedMsg), &wsMessage); err != nil {
		return nil
	}

	// Handle subscription confirmations
	if wsMessage.Code == 0 && wsMessage.Data == nil {
		return nil
	}

	// Handle book ticker data
	if strings.Contains(wsMessage.DataType, "@bookTicker") && wsMessage.Data != nil {
		var bookTicker BingXBookTickerData
		if err := json.Unmarshal(wsMessage.Data, &bookTicker); err != nil {
			return fmt.Errorf("failed to parse book ticker: %v", err)
		}


		bidPrice, err1 := strconv.ParseFloat(bookTicker.BestBid, 64)
		askPrice, err2 := strconv.ParseFloat(bookTicker.BestAsk, 64)
		if err1 != nil || err2 != nil {
			return fmt.Errorf("failed to parse prices: bid=%v, ask=%v", err1, err2)
		}

		// Convert BTC-USDT back to BTCUSDT
		normalizedSymbol := convertFromBingXSymbol(bookTicker.Symbol)

		orderbookData := OrderbookData{
			Symbol:    normalizedSymbol,
			Source:    "bingx_futures",
			BestBid:   bidPrice,
			BestAsk:   askPrice,
			Timestamp: time.Now().UnixMilli(),
		}

		orderbookChan <- orderbookData
	}

	return nil
}

func decodeGzip(data []byte) (string, error) {
	reader, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return "", err
	}
	defer reader.Close()

	decodedMsg, err := io.ReadAll(reader)
	if err != nil {
		return "", err
	}

	return string(decodedMsg), nil
}

// Convert BTCUSDT to BTC-USDT format for BingX
func convertToBingXSymbol(symbol string) string {
	if strings.HasSuffix(symbol, "USDT") {
		base := symbol[:len(symbol)-4]
		return base + "-USDT"
	}
	return symbol
}

// Convert BTC-USDT back to BTCUSDT format
func convertFromBingXSymbol(symbol string) string {
	if strings.Contains(symbol, "-") {
		return strings.ReplaceAll(symbol, "-", "")
	}
	return symbol
}

func ConnectBingXSpot(symbols []string, priceChan chan<- PriceData, orderbookChan chan<- OrderbookData, tradeChan chan<- TradeData) {
	wsURL := "wss://open-api-ws.bingx.com/market"

	for {
		header := http.Header{}
		header.Add("Accept-Encoding", "gzip")

		conn, _, err := websocket.DefaultDialer.Dial(wsURL, header)
		if err != nil {
			log.Printf("BingX spot connection error: %v", err)
			time.Sleep(5 * time.Second)
			continue
		}

		log.Printf("Connected to BingX spot WebSocket")

		// Subscribe to book ticker for each symbol
		for _, symbol := range symbols {
			// Convert BTCUSDT to BTC-USDT format for BingX
			bingxSymbol := convertToBingXSymbol(symbol)
			subscribeMessage := map[string]any{
				"id":       fmt.Sprintf("bingx-spot-%s-bookticker", symbol),
				"reqType":  "sub",
				"dataType": fmt.Sprintf("%s@bookTicker", bingxSymbol),
			}

			if err := conn.WriteJSON(subscribeMessage); err != nil {
				log.Printf("BingX spot subscription error for %s: %v", symbol, err)
			}
		}

		// Set up ping/pong mechanism
		go func(conn *websocket.Conn) {
			ticker := time.NewTicker(30 * time.Second)
			defer ticker.Stop()

			for {
				select {
				case <-ticker.C:
					if err := conn.WriteMessage(websocket.TextMessage, []byte("ping")); err != nil {
						log.Printf("BingX spot ping error: %v", err)
						return
					}
				}
			}
		}(conn)

		for {
			messageType, message, err := conn.ReadMessage()
			if err != nil {
				log.Printf("BingX spot read error: %v", err)
				conn.Close()
				break
			}

			if err := handleBingXSpotMessage(messageType, message, conn, orderbookChan); err != nil {
				log.Printf("BingX spot message handling error: %v", err)
			}
		}

		time.Sleep(2 * time.Second)
	}
}

func handleBingXSpotMessage(messageType int, message []byte, conn *websocket.Conn, orderbookChan chan<- OrderbookData) error {
	var decodedMsg string
	var err error

	switch messageType {
	case websocket.TextMessage:
		decodedMsg = string(message)
	case websocket.BinaryMessage:
		decodedMsg, err = decodeGzip(message)
		if err != nil {
			return fmt.Errorf("gzip decode error: %v", err)
		}
	default:
		return nil
	}

	// Handle ping/pong
	if strings.Contains(strings.ToLower(decodedMsg), "ping") {
		if err := conn.WriteMessage(websocket.TextMessage, []byte("pong")); err != nil {
			return fmt.Errorf("pong response error: %v", err)
		}
		return nil
	}

	// Skip empty messages
	if decodedMsg == "" {
		return nil
	}

	// Parse WebSocket message
	var wsMessage BingXWebSocketMessage
	if err := json.Unmarshal([]byte(decodedMsg), &wsMessage); err != nil {
		return nil
	}

	// Handle subscription confirmations
	if wsMessage.Code == 0 && wsMessage.Data == nil {
		return nil
	}

	// Handle book ticker data
	if strings.Contains(wsMessage.DataType, "@bookTicker") && wsMessage.Data != nil {
		var bookTicker BingXBookTickerData
		if err := json.Unmarshal(wsMessage.Data, &bookTicker); err != nil {
			return fmt.Errorf("failed to parse book ticker: %v", err)
		}

		bidPrice, err1 := strconv.ParseFloat(bookTicker.BestBid, 64)
		askPrice, err2 := strconv.ParseFloat(bookTicker.BestAsk, 64)
		if err1 != nil || err2 != nil {
			return fmt.Errorf("failed to parse prices: bid=%v, ask=%v", err1, err2)
		}

		// Convert BTC-USDT back to BTCUSDT
		normalizedSymbol := convertFromBingXSymbol(bookTicker.Symbol)

		orderbookData := OrderbookData{
			Symbol:    normalizedSymbol,
			Source:    "bingx_spot",
			BestBid:   bidPrice,
			BestAsk:   askPrice,
			Timestamp: time.Now().UnixMilli(),
		}

		orderbookChan <- orderbookData
	}

	return nil
}