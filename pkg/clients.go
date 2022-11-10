package rdapp

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/service/redshift"
	"github.com/aws/aws-sdk-go-v2/service/redshiftdata"
	"github.com/aws/aws-sdk-go-v2/service/redshiftserverless"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
)

//go:generate mockgen -destination mocks/mock_redshift_data_api_client.go -package mocks . RedshiftDataApiClient
type RedshiftDataApiClient interface {
	ExecuteStatement(ctx context.Context, params *redshiftdata.ExecuteStatementInput, optFns ...func(*redshiftdata.Options)) (*redshiftdata.ExecuteStatementOutput, error)
	DescribeStatement(ctx context.Context, params *redshiftdata.DescribeStatementInput, optFns ...func(*redshiftdata.Options)) (*redshiftdata.DescribeStatementOutput, error)
	GetStatementResult(ctx context.Context, params *redshiftdata.GetStatementResultInput, optFns ...func(*redshiftdata.Options)) (*redshiftdata.GetStatementResultOutput, error)
}

//go:generate mockgen -destination mocks/mock_redshift_serverless_client.go -package mocks . RedshiftServerlessClient
type RedshiftServerlessClient interface {
	redshiftserverless.ListWorkgroupsAPIClient
	redshiftserverless.ListNamespacesAPIClient
}

//go:generate mockgen -destination mocks/mock_redshift_client.go -package mocks . RedshiftClient
type RedshiftClient interface {
	redshift.DescribeClustersAPIClient
}

//go:generate mockgen -destination mocks/mock_secrets_manager_client.go -package mocks . SecretsManagerClient
type SecretsManagerClient interface {
	secretsmanager.ListSecretsAPIClient
}
