package mlflow

import "context"

// fakeClient builds a Client whose transport is fn, so API methods can be unit
// tested without a server. fn receives the call's (method, path, in) and writes
// any canned response into out.
func fakeClient(fn func(method, path string, in, out any) error) *Client {
	return &Client{do: func(_ context.Context, method, path string, in, out any) error {
		return fn(method, path, in, out)
	}}
}
