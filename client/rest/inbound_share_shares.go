package rest

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
)

// ListSharesParams contains parameters for listing shares
type ListSharesParams struct {
	Status       string // Filter by operational status (Pending, Creating, Active, Inactive, Error, Deleting)
	HealthStatus string // Filter by health status (Healthy, Unhealthy, Unknown)
	ProviderType string // Filter by provider type (Snowflake)
	Limit        int    // Maximum number of results (default: server-side default)
	Offset       int    // Number of results to skip
	OrderBy      string // Comma-separated list of fields to order by (e.g., "createdAt,-id")
}

// ListShares lists all external shares imported for the customer
func (c *Client) ListShares(ctx context.Context, params *ListSharesParams) (*ShareListResponse, error) {
	path := "/v1/shares/inbound"
	
	// Build query parameters
	if params != nil {
		query := url.Values{}
		if params.Status != "" {
			query.Add("status", params.Status)
		}
		if params.HealthStatus != "" {
			query.Add("healthStatus", params.HealthStatus)
		}
		if params.ProviderType != "" {
			query.Add("providerType", params.ProviderType)
		}
		if params.Limit > 0 {
			query.Add("limit", fmt.Sprintf("%d", params.Limit))
		}
		if params.Offset > 0 {
			query.Add("offset", fmt.Sprintf("%d", params.Offset))
		}
		if params.OrderBy != "" {
			query.Add("orderBy", params.OrderBy)
		}
		
		if len(query) > 0 {
			path = path + "?" + query.Encode()
		}
	}

	resp, err := c.Get(path)
	if err != nil {
		return nil, fmt.Errorf("failed to list shares: %w", err)
	}
	defer resp.Body.Close()

	var result ShareListResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to decode share list response: %w", err)
	}

	return &result, nil
}

// GetShare retrieves details for a specific external share
func (c *Client) GetShare(ctx context.Context, shareId string) (*Share, error) {
	path := fmt.Sprintf("/v1/shares/inbound/%s", url.PathEscape(shareId))

	resp, err := c.Get(path)
	if err != nil {
		return nil, fmt.Errorf("failed to get share: %w", err)
	}
	defer resp.Body.Close()

	var share Share
	if err := json.NewDecoder(resp.Body).Decode(&share); err != nil {
		return nil, fmt.Errorf("failed to decode share response: %w", err)
	}

	return &share, nil
}

// LookupShare finds a share by exact shareName and providerAccount match
// This is a convenience method that lists shares and filters by exact match on both fields
// Both shareName and providerAccount are required for uniqueness
func (c *Client) LookupShare(ctx context.Context, shareName, providerAccount string) (*Share, error) {
	// List all shares
	result, err := c.ListShares(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to lookup share: %w", err)
	}

	// Find exact match on both shareName and providerAccount
	for _, share := range result.Shares {
		if share.ShareName == shareName &&
		   share.SnowflakeConfig != nil &&
		   share.SnowflakeConfig.ProviderAccount == providerAccount {
			return &share, nil
		}
	}

	return nil, ErrorWithStatusCode{
		StatusCode: http.StatusNotFound,
		Err:        fmt.Errorf("share with name %q and provider account %q not found", shareName, providerAccount),
	}
}

