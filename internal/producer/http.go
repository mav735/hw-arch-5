package producer

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"github.com/mav735/hw-arch-5/internal/event"
)

func Router(pub *Publisher) *gin.Engine {
	r := gin.New()
	r.Use(gin.Recovery())

	r.GET("/health", func(c *gin.Context) { c.JSON(http.StatusOK, gin.H{"status": "ok"}) })

	r.POST("/events", func(c *gin.Context) {
		var e event.Event
		if err := c.ShouldBindJSON(&e); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		if e.EventID == "" {
			e.EventID = uuid.NewString()
		}
		if e.Timestamp.IsZero() {
			e.Timestamp = time.Now().UTC()
		}
		if err := e.Validate(); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}

		ctx, cancel := context.WithTimeout(c.Request.Context(), 30*time.Second)
		defer cancel()
		if err := pub.Publish(ctx, &e); err != nil {
			status := http.StatusServiceUnavailable
			if errors.Is(err, event.ErrInvalid) {
				status = http.StatusBadRequest
			}
			c.JSON(status, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusCreated, gin.H{"event_id": e.EventID})
	})
	return r
}
