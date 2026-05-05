package main

import "net/http"

type closeableHTTPResponse struct{ *http.Response }

func closeableHTTPDo(client *http.Client, req *http.Request) (*closeableHTTPResponse, error) {
	return newCloseableHTTPResponse(client.Do(req))
}

func newCloseableHTTPResponse(resp *http.Response, err error) (*closeableHTTPResponse, error) {
	if err != nil {
		return nil, err
	}
	return &closeableHTTPResponse{Response: resp}, nil
}

func (r *closeableHTTPResponse) Close() error {
	if r == nil || r.Body == nil {
		return nil
	}
	return r.Body.Close()
}
