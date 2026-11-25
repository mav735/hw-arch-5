package aggregator

import (
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
)

func Router(s *Service) *gin.Engine {
	r := gin.New()
	r.Use(gin.Recovery())

	r.GET("/health", func(c *gin.Context) { c.JSON(http.StatusOK, gin.H{"status": "ok"}) })

	r.POST("/recompute", func(c *gin.Context) {
		day, ok := parseDate(c)
		if !ok {
			return
		}
		res, err := s.Recompute(c.Request.Context(), day)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusOK, res)
	})

	r.POST("/export", func(c *gin.Context) {
		day, ok := parseDate(c)
		if !ok {
			return
		}
		key, n, err := s.Export(c.Request.Context(), day)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusOK, gin.H{"date": day.Format(time.DateOnly), "key": key, "metrics": n})
	})
	return r
}

func parseDate(c *gin.Context) (time.Time, bool) {
	q := c.Query("date")
	if q == "" {
		return Today(), true
	}
	day, err := time.Parse(time.DateOnly, q)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "date must be YYYY-MM-DD"})
		return time.Time{}, false
	}
	return day, true
}
