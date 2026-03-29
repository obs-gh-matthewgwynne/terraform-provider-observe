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

	fmt.Printf("[DEBUG] LookupShare: Looking for shareName=%q providerAccount=%q\n", shareName, providerAccount)
	fmt.Printf("[DEBUG] LookupShare: Got %d shares from API\n", len(result.Shares))

	// Find shares by shareName first
	// NOTE: The list endpoint returns ShareListItem which doesn't include SnowflakeConfig,
	// so we need to GET each matching share to verify the provider account.
	var candidates []Share
	for i, share := range result.Shares {
		fmt.Printf("[DEBUG] LookupShare: Share %d: ID=%s ShareName=%q ProviderType=%q\n",
			i, share.Id, share.ShareName, share.ProviderType)

		// Check that this is a Snowflake share
		if share.ProviderType != "Snowflake" {
			fmt.Printf("[DEBUG] LookupShare: Share %d SKIP: ProviderType=%q not Snowflake\n", i, share.ProviderType)
			continue
		}

		// Match on shareName (top-level field is the Snowflake share name)
		if share.ShareName != shareName {
			fmt.Printf("[DEBUG] LookupShare: Share %d SKIP: ShareName mismatch (want %q got %q)\n",
				i, shareName, share.ShareName)
			continue
		}

		fmt.Printf("[DEBUG] LookupShare: Share %d is a candidate (name matches)\n", i)
		candidates = append(candidates, share)
	}

	fmt.Printf("[DEBUG] LookupShare: Found %d candidate shares by name\n", len(candidates))

	// Now get full details for each candidate to check provider account
	var matches []Share
	for i, candidate := range candidates {
		fmt.Printf("[DEBUG] LookupShare: Getting full details for candidate %d (ID=%s)\n", i, candidate.Id)

		fullShare, err := c.GetShare(ctx, candidate.Id)
		if err != nil {
			fmt.Printf("[DEBUG] LookupShare: Failed to get share %s: %v\n", candidate.Id, err)
			continue
		}

		fmt.Printf("[DEBUG] LookupShare: Got full share %s\n", fullShare.Id)
		if fullShare.SnowflakeConfig != nil {
			fmt.Printf("[DEBUG] LookupShare: SnowflakeConfig: ShareName=%q ProviderAccount=%q\n",
				fullShare.SnowflakeConfig.ShareName, fullShare.SnowflakeConfig.ProviderAccount)
		} else {
			fmt.Printf("[DEBUG] LookupShare: SnowflakeConfig is nil\n")
		}

		// Verify provider account matches
		if fullShare.SnowflakeConfig == nil {
			fmt.Printf("[DEBUG] LookupShare: SKIP: SnowflakeConfig is nil\n")
			continue
		}

		if fullShare.SnowflakeConfig.ProviderAccount != providerAccount {
			fmt.Printf("[DEBUG] LookupShare: SKIP: ProviderAccount mismatch (want %q got %q)\n",
				providerAccount, fullShare.SnowflakeConfig.ProviderAccount)
			continue
		}

		fmt.Printf("[DEBUG] LookupShare: MATCH! Share %s matches both name and provider\n", fullShare.Id)
		matches = append(matches, *fullShare)
	}

	fmt.Printf("[DEBUG] LookupShare: Found %d matching shares after verifying provider account\n", len(matches))

	// Validate exactly one match
	if len(matches) == 0 {
		return nil, ErrorWithStatusCode{
			StatusCode: http.StatusNotFound,
			Err:        fmt.Errorf("share with name %q and provider account %q not found", shareName, providerAccount),
		}
	}
	if len(matches) > 1 {
		// Build helpful error message listing the conflicting share IDs
		shareIDs := make([]string, len(matches))
		for i, share := range matches {
			shareIDs[i] = share.Id
		}
		return nil, ErrorWithStatusCode{
			StatusCode: http.StatusConflict,
			Err: fmt.Errorf(
				"multiple shares found with name %q and provider %q. "+
					"Share names may not be unique. Found %d shares with IDs: %v. "+
					"Use the share ID directly instead of name+provider lookup",
				shareName, providerAccount, len(matches), shareIDs),
		}
	}

	return &matches[0], nil
}

