package middleware

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/go-kit/kit/metrics"
	"github.com/zhiqiangxu/redisbed/pkg/logger"
)

// MetricLogger middleware
func MetricLogger(latencyMetric metrics.Histogram, counterMetric metrics.Counter) func(*gin.Context) {

	return func(c *gin.Context) {

		var body string

		contentType := c.ContentType()
		if contentType == "application/json" {
			body, _ = copyRequestBody(c.Request)
		}

		begin := time.Now()

		c.Next()

		end := time.Now()
		latency := end.Sub(begin)

		path := c.Request.URL.Path
		raw := c.Request.URL.RawQuery

		clientIP := c.ClientIP()
		method := c.Request.Method
		statusCode := c.Writer.Status()
		comment := c.Errors.ByType(gin.ErrorTypePrivate).String()
		if comment != "" {
			comment = " [" + comment + "]"
		}

		// metrics
		if statusCode != 404 {
			err := strconv.Itoa(statusCode)
			latencyMetric.With("method", path, "error", err).Observe(latency.Seconds())
			counterMetric.With("method", path, "error", err).Add(1)
		}

		// log
		{
			if raw != "" {
				path = path + "?" + raw
			}

			logger.Instance().Info(
				fmt.Sprintf("[redisbed] %3d | %13v | %15s | %-7s %s %s%s %s",
					statusCode,
					latency,
					clientIP,
					method,
					contentType,
					path,
					comment,
					body))

			if (statusCode != 200 && statusCode != 204) || latency.Seconds() > 1 {
				var slowLog string
				if latency.Seconds() > 1 {
					slowLog = "slowlog"
				}
				logger.Instance().Error(
					fmt.Sprintf("[redisbed] %3d | %13v | %15s | %-7s %s  %s%s %s %s",
						statusCode,
						latency,
						clientIP,
						method,
						contentType,
						path,
						comment,
						body,
						slowLog))
			}

		}

	}

}

type readerCloser struct {
	io.Reader
	io.Closer
}

func copyRequestBody(req *http.Request) (body string, err error) {
	bodyBytes, err := ioutil.ReadAll(req.Body)
	if err != nil {
		return
	}

	body = string(bodyBytes)

	req.Body = readerCloser{Reader: bytes.NewBuffer(bodyBytes), Closer: req.Body}
	return
}
