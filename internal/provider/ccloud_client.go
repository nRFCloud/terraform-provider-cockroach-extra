package provider

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	"github.com/hashicorp/terraform-plugin-log/tflog"
	"github.com/jackc/pgx"
	"io"
	"net/http"
	"strings"
)

type CcloudClient struct {
	ApiKey     string
	Host       string
	HttpClient *http.Client
}

// NewCcloudClient returns a new CcloudClient.
func NewCcloudClient(apiKey string) *CcloudClient {
	return &CcloudClient{
		ApiKey:     apiKey,
		Host:       "https://cockroachlabs.cloud",
		HttpClient: http.DefaultClient,
	}
}

type TempUser struct {
	Username string `json:"name"`
	Password string `json:"password"`
}

func GenerateAuthHeader(apiKey string) string {
	return fmt.Sprintf("Bearer %s", apiKey)
}

func (c *CcloudClient) CreateTempUser(ctx context.Context, clusterId string) (*TempUser, error) {
	path := fmt.Sprintf("/api/v1/clusters/%s/sql-users", clusterId)
	request := TempUser{
		Username: fmt.Sprintf("temp-%s", uuid.New().String()),
		Password: uuid.New().String(),
	}

	requestBytes, err := json.Marshal(request)
	if err != nil {

		return nil, err
	}

	tflog.Debug(ctx, fmt.Sprintf("Create temp user request: %s, clusterId: %s", string(requestBytes), clusterId))

	body := bytes.NewReader(requestBytes)

	// Create a temp sql user using the ccloud api
	tflog.Debug(ctx, fmt.Sprintf("Making POST request to: %s", c.Host+path))
	req, err := http.NewRequest("POST", c.Host+path, body)
	if err != nil {
		return nil, err
	}

	req.Header.Add("Authorization", GenerateAuthHeader(c.ApiKey))
	req.Header.Add("Content-Type", "application/json")

	resp, err := c.HttpClient.Do(req)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != 200 {
		content := new(strings.Builder)
		io.Copy(content, resp.Body)
		return nil, fmt.Errorf("received non-200 status code: %d, %s", resp.StatusCode, content.String())
	}

	defer resp.Body.Close()

	return &request, nil
}

func (c *CcloudClient) DeleteTempUser(ctx context.Context, clusterId string, username string) error {
	path := fmt.Sprintf("/api/v1/clusters/%s/sql-users/%s", clusterId, username)

	req, err := http.NewRequest("DELETE", c.Host+path, nil)
	if err != nil {
		return err
	}

	req.Header.Add("Authorization", GenerateAuthHeader(c.ApiKey))

	resp, err := c.HttpClient.Do(req)
	if resp.StatusCode != 200 {
		return fmt.Errorf("received non-200 status code: %d", resp.StatusCode)
	}

	defer resp.Body.Close()

	return nil
}

type ConnectionStringResponseParams struct {
	Host     string `json:"Host"`
	Port     string `json:"Port"`
	Database string `json:"Database"`
}

type ConnectionStringResponse struct {
	ConnectionString string                         `json:"connection_string"`
	Params           ConnectionStringResponseParams `json:"params"`
}

func (c *CcloudClient) getConnectionOptions(ctx context.Context, clusterId string, user *TempUser) (*pgx.ConnConfig, error) {
	path := fmt.Sprintf("/api/v1/clusters/%s/connection-string?sql_user=%s", clusterId, user.Username)
	req, err := http.NewRequest("GET", c.Host+path, nil)
	if err != nil {

		return nil, err
	}

	req.Header.Add("Authorization", GenerateAuthHeader(c.ApiKey))

	resp, err := c.HttpClient.Do(req)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != 200 {
		content := new(strings.Builder)
		io.Copy(content, resp.Body)
		return nil, fmt.Errorf("received non-200 status code: %d, %s", resp.StatusCode, content.String())
	}

	defer resp.Body.Close()
	// read json data
	responseData := ConnectionStringResponse{}
	err = json.NewDecoder(resp.Body).Decode(&responseData)
	if err != nil {

		return nil, err
	}

	tflog.Debug(ctx, fmt.Sprintf("Connection string response: %v", responseData))

	opts, err := pgx.ParseConnectionString(responseData.ConnectionString)

	if err != nil {
		return nil, err
	}

	opts.Password = user.Password
	opts.User = user.Username

	opts.Logger = PgxLogger{ctx: ctx}
	opts.LogLevel = pgx.LogLevelTrace

	return &opts, nil
}

type PgxLogger struct {
	ctx context.Context
}

func (l PgxLogger) Log(level pgx.LogLevel, msg string, data map[string]interface{}) {
	tflog.Debug(l.ctx, fmt.Sprintf("PGX: %s, %v", msg, data))
}

type queryLoggerHook struct{}

func SqlConWithTempUser[Handler func(db *pgx.Conn) (*R, error), R any](ctx context.Context, client *CcloudClient, clusterId string, handler Handler) (res *R, err error) {
	user, err := client.CreateTempUser(ctx, clusterId)
	if err != nil {
		return nil, err
	}

	defer func(client *CcloudClient, ctx context.Context, clusterId string, username string) {
		r := client.DeleteTempUser(ctx, clusterId, username)
		if r != nil {
			err = r
		}
	}(client, ctx, clusterId, user.Username)

	opts, err := client.getConnectionOptions(ctx, clusterId, user)
	if err != nil {
		return nil, err
	}

	tflog.Debug(ctx, fmt.Sprintf("Connecting to cluster with options: %v", opts))

	db, err := pgx.Connect(*opts)
	if err != nil {
		return nil, err
	}

	defer db.Close()

	return handler(db)
}
