package drpc

import (
	"context"
	"reflect"
)

type ServerHandler func(ctx context.Context, serviceMethod string, argv, replyv reflect.Value) error

type ServerMiddleware func(
	ctx context.Context,
	serviceMethod string,
	argv, replyv reflect.Value,
	handler ServerHandler,
) error
