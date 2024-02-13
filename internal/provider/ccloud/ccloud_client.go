package ccloud

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
	"time"
)

type CcloudClient struct {
	ApiKey     string
	Host       string
	httpClient *http.Client
	sqlConMap  map[string]map[string]*pgx.ConnPool
}

const clusterUserName = "terraform-provider-cockroach-extra"

var userCredMapResource = NewSyncResourceHolder(&UserCredMap{})

type CockroachCloudErrorResponse struct {
	Code    int      `json:"code"`
	Message string   `json:"message"`
	Details []string `json:"details"`
}

type CockroachCloudClusterNotFoundError struct{}

func (e CockroachCloudClusterNotFoundError) Error() string {
	return "cluster not found"
}

type CockroachCloudClusterNotReadyError struct {
}

func (e CockroachCloudClusterNotReadyError) Error() string {
	return "cluster not ready"
}

func processCloudResponse(resp *http.Response, outputStruct *interface{}) (err error) {
	if resp.StatusCode != 200 {
		// read body content as string
		errorBody := CockroachCloudErrorResponse{}
		err = json.NewDecoder(resp.Body).Decode(&errorBody)
		if err != nil {
			return err
		}
		if errorBody.Code == 9 {
			return CockroachCloudClusterNotReadyError{}
		}
		if errorBody.Code == 5 {
			return CockroachCloudClusterNotFoundError{}
		}

		return fmt.Errorf("received non-200 status code: %d, body: %v", resp.StatusCode, errorBody)
	}

	if outputStruct == nil {
		return nil
	}

	// read json data
	err = json.NewDecoder(resp.Body).Decode(outputStruct)
	if err != nil {
		return err
	}

	return nil

}

// NewCcloudClient returns a new CcloudClient.
func NewCcloudClient(ctx context.Context, apiKey string) *CcloudClient {
	tflog.Debug(ctx, "Creating ccloud client with api key")

	client := &CcloudClient{
		ApiKey:     apiKey,
		Host:       "https://cockroachlabs.cloud",
		httpClient: http.DefaultClient,
		sqlConMap:  make(map[string]map[string]*pgx.ConnPool),
	}

	return client
}

type tempUser struct {
	Username string `json:"name"`
	Password string `json:"password"`
}

type UserCredMap = map[string]*tempUser

func generateAuthHeader(apiKey string) string {
	return fmt.Sprintf("Bearer %s", apiKey)
}

func (c *CcloudClient) createTempUser(ctx context.Context, clusterId string) (user *tempUser, err error) {
	err = c.deleteTempUser(ctx, clusterId, clusterUserName)
	if err != nil {
		return nil, err
	}

	path := fmt.Sprintf("/api/v1/clusters/%s/sql-users", clusterId)
	request := tempUser{
		Username: clusterUserName,
		Password: uuid.New().String(),
	}

	requestBytes, err := json.Marshal(request)
	if err != nil {
		return nil, err
	}

	body := bytes.NewReader(requestBytes)

	// Create a temp sql user using the ccloud api
	tflog.Debug(ctx, fmt.Sprintf("Making POST request to: %s", c.Host+path))
	req, err := http.NewRequest("POST", c.Host+path, body)
	if err != nil {
		return nil, err
	}

	req.Header.Add("Authorization", generateAuthHeader(c.ApiKey))
	req.Header.Add("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}

	err = processCloudResponse(resp, nil)

	if err != nil {
		return nil, err
	}

	err = c.updateUserExpiration(ctx, clusterId, &request)

	if err != nil {
		return nil, err
	}

	defer func(Body io.ReadCloser) {
		err := Body.Close()
		if err != nil {
			return
		}
	}(resp.Body)

	return &request, nil
}

func (c *CcloudClient) updateUserExpiration(ctx context.Context, clusterId string, user *tempUser) error {
	pool, err := c.getOrCreateConPool(ctx, clusterId, user, "defaultdb")

	if err != nil {
		return err
	}
	expTime := time.Now().Add(4 * time.Minute)
	_, err = pool.Exec(fmt.Sprintf("ALTER USER %s WITH VALID UNTIL $1", pgx.Identifier{user.Username}.Sanitize()), expTime.Format(time.RFC3339))
	return err
}

func (c *CcloudClient) getOrCreateTempUser(ctx context.Context, userCredMap *UserCredMap, clusterId string) (*tempUser, error) {
	credMap := *userCredMap
	if credMap[clusterId] == nil {
		tflog.Debug(ctx, fmt.Sprintf("Creating temp user for cluster %s", clusterId))
		user, err := c.createTempUser(ctx, clusterId)
		if err != nil {
			return nil, err
		}
		credMap[clusterId] = user
	} else {
		tflog.Debug(ctx, fmt.Sprintf("Using existing temp user for cluster %s", clusterId))
	}
	return credMap[clusterId], nil
}

func (c *CcloudClient) deleteTempUser(ctx context.Context, clusterId string, username string) (err error) {
	path := fmt.Sprintf("/api/v1/clusters/%s/sql-users/%s", clusterId, username)

	req, err := http.NewRequest("DELETE", c.Host+path, nil)
	if err != nil {
		return err
	}

	req.Header.Add("Authorization", generateAuthHeader(c.ApiKey))

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}

	defer func(Body io.ReadCloser) {
		err := Body.Close()
		if err != nil {
			return
		}
	}(resp.Body)

	return processCloudResponse(resp, nil)
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

func (c *CcloudClient) getConnectionOptions(ctx context.Context, clusterId string, user *tempUser, database string) (con *pgx.ConnConfig, err error) {
	path := fmt.Sprintf("/api/v1/clusters/%s/connection-string?sql_user=%s", clusterId, user.Username)
	req, err := http.NewRequest("GET", c.Host+path, nil)
	if err != nil {

		return nil, err
	}

	req.Header.Add("Authorization", generateAuthHeader(c.ApiKey))

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("received non-200 status code: %d", resp.StatusCode)
	}

	defer func(Body io.ReadCloser) {
		err := Body.Close()
		if err != nil {
			return
		}
	}(resp.Body)

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
	opts.Database = database
	opts.Logger = pgxLogger{ctx: ctx}
	opts.LogLevel = pgx.LogLevelTrace

	return &opts, nil
}

func (c *CcloudClient) getOrCreateConPool(ctx context.Context, clusterId string, user *tempUser, database string) (*pgx.ConnPool, error) {
	if c.sqlConMap[clusterId][database] == nil {
		tflog.Debug(ctx, fmt.Sprintf("Creating connection pool for cluster %s", clusterId))
		connConfig, err := c.getConnectionOptions(ctx, clusterId, user, database)

		if err != nil {
			return nil, err
		}

		config := pgx.ConnPoolConfig{
			ConnConfig:     *connConfig,
			MaxConnections: 5,
			AfterConnect: func(conn *pgx.Conn) error {
				_, err := conn.Exec("SET role admin")
				return err
			},
		}
		if c.sqlConMap[clusterId] == nil {
			c.sqlConMap[clusterId] = make(map[string]*pgx.ConnPool)
		}

		c.sqlConMap[clusterId][database], err = pgx.NewConnPool(config)

		if err != nil {
			return nil, err
		}
	} else {
		tflog.Debug(ctx, fmt.Sprintf("Using existing connection pool for cluster %s", clusterId))
	}
	return c.sqlConMap[clusterId][database], nil
}

type pgxLogger struct {
	ctx context.Context
}

func (l pgxLogger) Log(_ pgx.LogLevel, msg string, data map[string]interface{}) {
	tflog.Debug(l.ctx, fmt.Sprintf("PGX: %s, %v", msg, data))
}

func SqlConWithTempUser[Handler func(db *pgx.ConnPool) (*R, error), R any](ctx context.Context, client *CcloudClient, clusterId string, database string, handler Handler) (res *R, err error) {
	userCredMap, unlock := userCredMapResource.Get()
	defer unlock()

	user, err := client.getOrCreateTempUser(ctx, userCredMap, clusterId)
	if err != nil {
		return nil, err
	}

	err = client.updateUserExpiration(ctx, clusterId, user)
	if err != nil {
		return nil, err
	}
	pool, err := client.getOrCreateConPool(ctx, clusterId, user, database)

	if err != nil {
		return nil, err
	}

	defer func(pool *pgx.ConnPool, sql string) {
		_, err := pool.Exec(sql)
		if err != nil {
			return
		}
	}(pool, fmt.Sprintf("REASSIGN OWNED BY %s TO admin", pgx.Identifier{user.Username}.Sanitize()))

	return handler(pool)
}
