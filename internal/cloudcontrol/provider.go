package cloudcontrol

import (
	"context"

	"github.com/yfj898/taskflow-go/internal/domain"
)

type ProviderSnapshot struct {
	ProviderResourceID string
	State              domain.CloudResourceActualState
	Spec               string
}

type ResourceProvider interface {
	Apply(context.Context, domain.CloudResource) (ProviderSnapshot, error)
	Delete(context.Context, domain.CloudResource) (ProviderSnapshot, error)
	Observe(context.Context, domain.CloudResource) (ProviderSnapshot, error)
}
