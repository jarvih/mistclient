package mistclient

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
)

// GetSiteStats fetches a site's operational statistics
func (c *APIClient) GetSiteStats(siteID string) (SiteStat, error) {
	var siteStat SiteStat

	resp, err := c.Get(c.baseURL.JoinPath(fmt.Sprintf("/api/v1/sites/%s/stats", siteID)))
	if err != nil {
		return siteStat, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return siteStat, extractError(resp)
	}

	err = json.NewDecoder(resp.Body).Decode(&siteStat)

	return siteStat, err
}

// GetSiteDevices fetches and returns a list of all devices configured at a site
func (c *APIClient) GetSiteDevices(siteID string) ([]Device, error) {
	resp, err := c.Get(c.baseURL.JoinPath(fmt.Sprintf("/api/v1/sites/%s/devices", siteID)))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, extractError(resp)
	}

	var devices []Device
	if err := json.NewDecoder(resp.Body).Decode(&devices); err != nil {
		return nil, err
	}

	return devices, nil
}

// StreamSiteDevices opens a websocket connection and subscribes to the site devices stream
func (c *APIClient) StreamSiteDevices(ctx context.Context, siteID string) (<-chan Device, error) {
	return streamStats[Device](ctx, c, fmt.Sprintf("/sites/%s/devices", siteID))
}

// GetSiteDeviceStats fetches and returns a list of all devices configured at a site, supplemented with operational statistics
func (c *APIClient) GetSiteDeviceStats(siteID string) ([]DeviceStat, error) {
	resp, err := c.Get(c.baseURL.JoinPath(fmt.Sprintf("/api/v1/sites/%s/stats/devices", siteID)))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, extractError(resp)
	}

	var devices []DeviceStat
	if err := json.NewDecoder(resp.Body).Decode(&devices); err != nil {
		return nil, err
	}

	return devices, nil
}

// StreamSiteDeviceStats opens a websocket connection and subscribes to the device statistics stream
func (c *APIClient) StreamSiteDeviceStats(ctx context.Context, siteID string) (<-chan StreamedDeviceStat, error) {
	return streamStats[StreamedDeviceStat](ctx, c, fmt.Sprintf("/sites/%s/stats/devices", siteID))

}

// GetSiteClientStats fetches and returns a list of all clients configured at a site
func (c *APIClient) GetSiteClientStats(siteID string) ([]Client, error) {
	resp, err := c.Get(c.baseURL.JoinPath(fmt.Sprintf("/api/v1/sites/%s/stats/clients", siteID)))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, extractError(resp)
	}

	var clients []Client
	if err := json.NewDecoder(resp.Body).Decode(&clients); err != nil {
		return nil, err
	}

	return clients, nil
}

// StreamSiteClientStats opens a websocket connection and subscribes to the client statistics stream
func (c *APIClient) StreamSiteClientStats(ctx context.Context, siteID string) (<-chan StreamedClientStat, error) {
	return streamStats[StreamedClientStat](ctx, c, fmt.Sprintf("/sites/%s/stats/clients", siteID))
}

// GetSiteRRMEvents retrieves RRM events for a site within the specified time range for a specific radio band.
// Parameters start and end are epoch timestamps. Use limit to control page size (default 100).
// The band parameter specifies which radio band to query (Band24, Band5, or Band6).
func (c *APIClient) GetSiteRRMEvents(siteID string, band Radio, start int64, end int64, limit int) (RRMEventsResponse, error) {
	var response RRMEventsResponse

	u := c.baseURL.JoinPath(fmt.Sprintf("/api/v1/sites/%s/rrm/events", siteID))

	q := u.Query()
	q.Add("band", band.APIString())
	q.Add("start", strconv.FormatInt(start, 10))
	q.Add("end", strconv.FormatInt(end, 10))
	if limit > 0 {
		q.Add("limit", strconv.Itoa(limit))
	}
	u.RawQuery = q.Encode()

	resp, err := c.Get(u)
	if err != nil {
		return response, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return response, extractError(resp)
	}

	err = json.NewDecoder(resp.Body).Decode(&response)
	return response, err
}
