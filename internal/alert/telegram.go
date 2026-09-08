// Package alert sends notifications when a check's consensus status changes.
package alert

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
)

// Telegram sends a plain text message to a chat via a bot token. Token and
// chat ID are passed in per call (rather than read from the environment
// once at startup) so they can be changed live from the dashboard without
// restarting the server.
type Telegram struct {
	token  string
	chatID string
}

func NewTelegram(token, chatID string) *Telegram {
	return &Telegram{token: token, chatID: chatID}
}

func (t *Telegram) Enabled() bool {
	return t.token != "" && t.chatID != ""
}

func (t *Telegram) Send(text string) error {
	if !t.Enabled() {
		return fmt.Errorf("telegram alerts are not configured")
	}
	url := fmt.Sprintf("https://api.telegram.org/bot%s/sendMessage", t.token)
	body, _ := json.Marshal(map[string]string{
		"chat_id":    t.chatID,
		"text":       text,
		"parse_mode": "HTML",
	})
	resp, err := http.Post(url, "application/json", bytes.NewReader(body))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 {
		var apiErr struct {
			Description string `json:"description"`
		}
		json.NewDecoder(resp.Body).Decode(&apiErr)
		if apiErr.Description != "" {
			return fmt.Errorf("telegram: %s", apiErr.Description)
		}
		return fmt.Errorf("telegram: unexpected status %d", resp.StatusCode)
	}
	return nil
}
