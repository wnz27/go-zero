package trace

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/metadata"
)

func TestHttpPropagator_Extract(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "http://localhost", nil)
	req.Header.Set(TraceIdKey, "trace")
	req.Header.Set(spanIdKey, "span")
	carrier, err := Extract(HttpFormat, req.Header)
	assert.Nil(t, err)
	assert.Equal(t, "trace", carrier.Get(TraceIdKey))
	assert.Equal(t, "span", carrier.Get(spanIdKey))

	_, err = Extract(HttpFormat, req)
	assert.Equal(t, ErrInvalidCarrier, err)
}

func TestHttpPropagator_Inject(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "http://localhost", nil)
	req.Header.Set(TraceIdKey, "trace")
	req.Header.Set(spanIdKey, "span")
	carrier, err := Inject(HttpFormat, req.Header)
	assert.Nil(t, err)
	assert.Equal(t, "trace", carrier.Get(TraceIdKey))
	assert.Equal(t, "span", carrier.Get(spanIdKey))

	_, err = Inject(HttpFormat, req)
	assert.Equal(t, ErrInvalidCarrier, err)
}

func TestGrpcPropagator_Extract(t *testing.T) {
	md := metadata.New(map[string]string{
		TraceIdKey: "trace",
		spanIdKey:  "span",
	})
	carrier, err := Extract(GrpcFormat, md)
	assert.Nil(t, err)
	assert.Equal(t, "trace", carrier.Get(TraceIdKey))
	assert.Equal(t, "span", carrier.Get(spanIdKey))

	_, err = Extract(GrpcFormat, 1)
	assert.Equal(t, ErrInvalidCarrier, err)
	_, err = Extract(nil, 1)
	assert.Equal(t, ErrInvalidCarrier, err)
}

func TestGrpcPropagator_Inject(t *testing.T) {
	md := metadata.New(map[string]string{
		TraceIdKey: "trace",
		spanIdKey:  "span",
	})
	carrier, err := Inject(GrpcFormat, md)
	assert.Nil(t, err)
	assert.Equal(t, "trace", carrier.Get(TraceIdKey))
	assert.Equal(t, "span", carrier.Get(spanIdKey))

	_, err = Inject(GrpcFormat, 1)
	assert.Equal(t, ErrInvalidCarrier, err)
	_, err = Inject(nil, 1)
	assert.Equal(t, ErrInvalidCarrier, err)
}
