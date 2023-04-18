package ste

import (
	"context"
	"time"

	"github.com/Azure/azure-pipeline-go/pipeline"
)

// NewRequestLogPolicyFactory creates a RequestLogPolicyFactory object configured using the specified options.
func NewInjectErrorPolicyFactory() pipeline.Factory {
	blocked := true
	go func() {
		for {
			<-time.After(30 * time.Second)
			blocked = !blocked
			<-time.After(2 * time.Minute)
			blocked = !blocked
		}
	}()
	return pipeline.FactoryFunc(func(next pipeline.Policy, po *pipeline.PolicyOptions) pipeline.PolicyFunc {
		// These variables are per-policy; shared by multiple calls to Do
		var try int32
		return func(ctx context.Context, request pipeline.Request) (response pipeline.Response, err error) {
			try++ // The first try is #1 (not #0)

			response, err = next.Do(ctx, request) // Make the request\

			if blocked && try == 1 && request.Method == "PUT" {
				response.Response().StatusCode = 500
				response.Response().Status = "500 Operation could not be completed within the specified time."
			}

			
			return response, err
		}
	})
}