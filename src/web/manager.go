package web

import (
	// GIN
	"net/http"
	"tomqserver/src/mq"

	"github.com/gin-gonic/gin"
)

var qc mq.QueuesControl

func TaskQueue(c *gin.Context) {
	res := qc.ServerInfo()
	c.IndentedJSON(http.StatusOK, res)
}

func RegisterAll(router gin.Engine) {
	// watch queue
	router.GET("/qc", TaskQueue)
}

func Serve(control *mq.QueuesControl) {
	qc = *control
	// default router
	router := gin.Default()
	// api blueprint
	RegisterAll(*router)
	// vrum vrum
	router.Run("localhost:15896")
}
