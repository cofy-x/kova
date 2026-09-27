package service

import (
	"net/http"

	"k8s.io/client-go/rest"
)

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

// singleAttemptWrites prevents client-go's Retry-After handling from issuing
// a second wire POST/PUT/PATCH/DELETE for one high-level mutation. A delayed
// first Create must not arrive after its intent or Pod nonce was released.
// Our callers handle conflicts and unknown outcomes explicitly.
func singleAttemptWrites(config *rest.Config) *rest.Config {
	copy := rest.CopyConfig(config)
	previous := copy.WrapTransport
	copy.WrapTransport = func(transport http.RoundTripper) http.RoundTripper {
		if previous != nil {
			transport = previous(transport)
		}
		return roundTripperFunc(func(request *http.Request) (*http.Response, error) {
			response, err := transport.RoundTrip(request)
			if response != nil && request.Method != http.MethodGet && request.Method != http.MethodHead && request.Method != http.MethodOptions {
				response.Header.Del("Retry-After")
			}
			return response, err
		})
	}
	return copy
}
