package rdapp

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/service/redshift"
	"github.com/aws/aws-sdk-go-v2/service/redshiftdata"
	"github.com/aws/aws-sdk-go-v2/service/redshiftserverless"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
)

type RedshiftDataApiClient interface {
	ExecuteStatement(ctx context.Context, params *redshiftdata.ExecuteStatementInput, optFns ...func(*redshiftdata.Options)) (*redshiftdata.ExecuteStatementOutput, error)
	DescribeStatement(ctx context.Context, params *redshiftdata.DescribeStatementInput, optFns ...func(*redshiftdata.Options)) (*redshiftdata.DescribeStatementOutput, error)
	GetStatementResult(ctx context.Context, params *redshiftdata.GetStatementResultInput, optFns ...func(*redshiftdata.Options)) (*redshiftdata.GetStatementResultOutput, error)
}

type RedshiftServerlessClient interface {
	redshiftserverless.ListWorkgroupsAPIClient
	redshiftserverless.ListNamespacesAPIClient
}

type RedshiftClient interface {
	redshift.DescribeClustersAPIClient
}

type SecretsManagerClient interface {
	secretsmanager.ListSecretsAPIClient
}
