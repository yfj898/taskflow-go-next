package handler

import (
	"crypto/rand"
	"encoding/hex"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	appmetrics "github.com/yfj898/taskflow-go/internal/metrics"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	otelcodes "go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/time/rate"
)

func requestID() gin.HandlerFunc {
	return func(c *gin.Context) {
		id := c.GetHeader("X-Request-ID")
		if id == "" {
			var buf [12]byte
			if _, err := rand.Read(buf[:]); err == nil {
				id = hex.EncodeToString(buf[:])
			} else {
				id = strconv.FormatInt(time.Now().UnixNano(), 10)
			}
		}
		c.Header("X-Request-ID", id)
		c.Set("request_id", id)
		c.Next()
	}
}

func rateLimit(rps, burst int) gin.HandlerFunc {
	limiter := rate.NewLimiter(rate.Limit(rps), burst)
	return func(c *gin.Context) {
		if !limiter.Allow() {
			c.AbortWithStatusJSON(http.StatusTooManyRequests, gin.H{"error": "rate limit exceeded"})
			return
		}
		c.Next()
	}
}

func observeHTTP() gin.HandlerFunc {
	return func(c *gin.Context) {
		started := time.Now()
		c.Next()
		path := c.FullPath()
		if path == "" {
			path = "unmatched"
		}
		appmetrics.HTTPRequests.WithLabelValues(c.Request.Method, path, strconv.Itoa(c.Writer.Status())).Inc()
		appmetrics.HTTPDuration.WithLabelValues(c.Request.Method, path).Observe(time.Since(started).Seconds())
	}
}

func traceHTTP() gin.HandlerFunc {
	tracer := otel.Tracer("taskflow/http")
	return func(c *gin.Context) {
		ctx := otel.GetTextMapPropagator().Extract(
			c.Request.Context(),
			propagation.HeaderCarrier(c.Request.Header),
		)
		ctx, span := tracer.Start(
			ctx,
			c.Request.Method+" "+c.Request.URL.Path,
			trace.WithSpanKind(trace.SpanKindServer),
		)
		defer span.End()

		if requestID, ok := c.Get("request_id"); ok {
			if id, ok := requestID.(string); ok {
				span.SetAttributes(attribute.String("request.id", id))
			}
		}
		if span.SpanContext().IsValid() {
			c.Header("X-Trace-ID", span.SpanContext().TraceID().String())
		}
		c.Request = c.Request.WithContext(ctx)
		c.Next()

		route := c.FullPath()
		if route != "" {
			span.SetName(c.Request.Method + " " + route)
		}
		span.SetAttributes(
			attribute.Int("http.response.status_code", c.Writer.Status()),
			attribute.String("http.route", route),
		)
		if c.Writer.Status() >= http.StatusInternalServerError {
			span.SetStatus(otelcodes.Error, http.StatusText(c.Writer.Status()))
		}
	}
}
