package rdapp

import (
	"context"
	"go.uber.org/zap"
)

type RdappContext struct {
	context.Context
	logger *zap.Logger
}
