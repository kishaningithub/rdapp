package rdapp

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/service/redshift"
	"github.com/aws/aws-sdk-go-v2/service/redshift/types"
	"github.com/aws/aws-sdk-go-v2/service/redshiftserverless"
	redshiftserverlesstypes "github.com/aws/aws-sdk-go-v2/service/redshiftserverless/types"
)

type RedshiftService interface {
	FetchServerlessWorkGroups(ctx context.Context) ([]redshiftserverlesstypes.Workgroup, error)
	FetchServerlessNamespaces(ctx context.Context) ([]redshiftserverlesstypes.Namespace, error)
	FetchProvisionedClusters(ctx context.Context) ([]types.Cluster, error)
}

type redshiftService struct {
	redshiftClient           RedshiftClient
	redshiftServerlessClient RedshiftServerlessClient
}

func NewRedshiftService(redshiftClient RedshiftClient, redshiftServerlessClient RedshiftServerlessClient) RedshiftService {
	return &redshiftService{
		redshiftClient:           redshiftClient,
		redshiftServerlessClient: redshiftServerlessClient,
	}
}

func (service *redshiftService) FetchServerlessWorkGroups(ctx context.Context) ([]redshiftserverlesstypes.Workgroup, error) {
	var serverlessWorkgroups []redshiftserverlesstypes.Workgroup
	listWorkgroupsPaginator := redshiftserverless.NewListWorkgroupsPaginator(service.redshiftServerlessClient, nil)
	for listWorkgroupsPaginator.HasMorePages() {
		page, err := listWorkgroupsPaginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("error while loading serverless workgroups: %w", err)
		}
		serverlessWorkgroups = append(serverlessWorkgroups, page.Workgroups...)
	}
	return serverlessWorkgroups, nil
}

func (service *redshiftService) FetchServerlessNamespaces(ctx context.Context) ([]redshiftserverlesstypes.Namespace, error) {
	var serverlessNamespaces []redshiftserverlesstypes.Namespace
	listNamespacesPaginator := redshiftserverless.NewListNamespacesPaginator(service.redshiftServerlessClient, nil)
	for listNamespacesPaginator.HasMorePages() {
		page, err := listNamespacesPaginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("error while loading redshift serverless namespaces: %w", err)
		}
		serverlessNamespaces = append(serverlessNamespaces, page.Namespaces...)
	}
	return serverlessNamespaces, nil
}

func (service *redshiftService) FetchProvisionedClusters(ctx context.Context) ([]types.Cluster, error) {
	var clusters []types.Cluster
	describeClustersPaginator := redshift.NewDescribeClustersPaginator(service.redshiftClient, nil)
	for describeClustersPaginator.HasMorePages() {
		page, err := describeClustersPaginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("error while loading provisioned clusters: %w", err)
		}
		clusters = append(clusters, page.Clusters...)
	}
	return clusters, nil
}
