package main

import (
	"context"
	"fmt"
	"github.com/AlecAivazis/survey/v2"
	"github.com/aws/aws-sdk-go-v2/service/redshift"
	"github.com/aws/aws-sdk-go-v2/service/redshift/types"
	"github.com/aws/aws-sdk-go-v2/service/redshiftserverless"
	redshiftserverlesstypes "github.com/aws/aws-sdk-go-v2/service/redshiftserverless/types"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	secretmanagertypes "github.com/aws/aws-sdk-go-v2/service/secretsmanager/types"
	"github.com/kishaningithub/rdapp/pkg"
	"go.uber.org/zap"
)

type ConfigInstance struct {
	instanceName    string
	instanceDetails rdapp.RedshiftDataAPIConfig
}

type configInstances []ConfigInstance

func (instances configInstances) getInstanceNames() []string {
	var instanceNames []string
	for _, instance := range instances {
		instanceNames = append(instanceNames, instance.instanceName)
	}
	return instanceNames
}

func (instances configInstances) getInstanceByName(instanceName string) (ConfigInstance, error) {
	for _, instance := range instances {
		if instance.instanceName == instanceName {
			return instance, nil
		}
	}
	return ConfigInstance{}, fmt.Errorf("configuration instance is not found instanceName=%s", instanceName)
}

type Secrets []secretmanagertypes.SecretListEntry

func (secrets Secrets) getSecretArns() []string {
	var secretArns []string
	for _, secret := range secrets {
		secretArns = append(secretArns, *secret.ARN)
	}
	return secretArns
}

type InteractionService interface {
	Interact(ctx context.Context) (rdapp.RedshiftDataAPIConfig, error)
}

type interactionService struct {
	redshiftClient           rdapp.RedshiftClient
	redshiftServerlessClient rdapp.RedshiftServerlessClient
	secretsManagerClient     rdapp.SecretsManagerClient
	logger                   *zap.Logger
}

func NewInteractionService(redshiftClient rdapp.RedshiftClient, redshiftServerlessClient rdapp.RedshiftServerlessClient,
	secretsManagerClient rdapp.SecretsManagerClient, logger *zap.Logger) InteractionService {
	return &interactionService{
		redshiftClient:           redshiftClient,
		redshiftServerlessClient: redshiftServerlessClient,
		secretsManagerClient:     secretsManagerClient,
		logger:                   logger,
	}
}

func (service *interactionService) Interact(ctx context.Context) (rdapp.RedshiftDataAPIConfig, error) {
	instances, err := service.loadConfigInstances(ctx)
	if err != nil {
		return rdapp.RedshiftDataAPIConfig{}, err
	}
	var selectedInstanceName string
	err = survey.AskOne(&survey.Select{
		Message: "Which instance you want to connect to?",
		Options: instances.getInstanceNames(),
	}, &selectedInstanceName)
	if err != nil {
		return rdapp.RedshiftDataAPIConfig{}, err
	}
	selectedInstance, err := instances.getInstanceByName(selectedInstanceName)
	if err != nil {
		return rdapp.RedshiftDataAPIConfig{}, err
	}
	var useSecretManager bool
	err = survey.AskOne(&survey.Confirm{
		Message: "Would you like to choose a secret for connecting?",
	}, &useSecretManager)
	if err != nil {
		return rdapp.RedshiftDataAPIConfig{}, err
	}
	if useSecretManager {
		secrets, err := service.fetchSecrets(ctx)
		if err != nil {
			return rdapp.RedshiftDataAPIConfig{}, err
		}
		var selectedSecretArn string
		err = survey.AskOne(&survey.Select{
			Message: "Choose the secret",
			Options: secrets.getSecretArns(),
		}, &selectedSecretArn)
		if err != nil {
			return rdapp.RedshiftDataAPIConfig{}, err
		}
		selectedInstance.instanceDetails.SecretArn = &selectedSecretArn
		selectedInstance.instanceDetails.DbUser = nil
	}
	return selectedInstance.instanceDetails, nil
}

func (service *interactionService) loadConfigInstances(ctx context.Context) (configInstances, error) {
	provisionedClusters, err := service.fetchProvisionedClusters(ctx)
	if err != nil {
		return nil, err
	}
	serverlessNamespaces, err := service.fetchServerlessNamespaces(ctx)
	if err != nil {
		return nil, err
	}
	workgroups, err := service.fetchServerlessWorkGroups(ctx)
	if err != nil {
		return nil, err
	}
	instances, err := service.computeConfigInstances(provisionedClusters, workgroups, serverlessNamespaces)
	if err != nil {
		return nil, err
	}
	return instances, nil
}

func (service *interactionService) fetchSecrets(ctx context.Context) (Secrets, error) {
	var secrets Secrets
	secretsPaginator := secretsmanager.NewListSecretsPaginator(service.secretsManagerClient, nil)
	for secretsPaginator.HasMorePages() {
		listSecretsOutput, err := secretsPaginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("error occurred while fetching secrets from secrets manager: %w", err)
		}
		secrets = append(secrets, listSecretsOutput.SecretList...)
	}
	service.logger.Debug("completed fetching secrets from secrets manager", zap.Int("noOfSecrets", len(secrets)))
	return secrets, nil
}

func (service *interactionService) fetchServerlessWorkGroups(ctx context.Context) ([]redshiftserverlesstypes.Workgroup, error) {
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

func (service *interactionService) fetchServerlessNamespaces(ctx context.Context) ([]redshiftserverlesstypes.Namespace, error) {
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

func (service *interactionService) fetchProvisionedClusters(ctx context.Context) ([]types.Cluster, error) {
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

func (service *interactionService) computeConfigInstances(provisionedClusters []types.Cluster, serverlessWorkgroups []redshiftserverlesstypes.Workgroup, serverlessNamespaces []redshiftserverlesstypes.Namespace) (configInstances, error) {
	var instances configInstances
	for _, cluster := range provisionedClusters {
		if *cluster.ClusterStatus != "available" {
			continue
		}
		instances = append(instances, ConfigInstance{
			instanceName: *cluster.ClusterIdentifier,
			instanceDetails: rdapp.RedshiftDataAPIConfig{
				Database:          cluster.DBName,
				ClusterIdentifier: cluster.ClusterIdentifier,
				DbUser:            cluster.MasterUsername,
			},
		})
	}
	for _, workgroup := range serverlessWorkgroups {
		if workgroup.Status != redshiftserverlesstypes.WorkgroupStatusAvailable {
			continue
		}
		namespace, err := service.findNameSpaceForWorkGroup(workgroup, serverlessNamespaces)
		if err != nil {
			return nil, err
		}
		instances = append(instances, ConfigInstance{
			instanceName: *workgroup.WorkgroupName,
			instanceDetails: rdapp.RedshiftDataAPIConfig{
				Database:      namespace.DbName,
				WorkgroupName: workgroup.WorkgroupName,
			},
		})
	}
	return instances, nil
}

func (service *interactionService) findNameSpaceForWorkGroup(workgroup redshiftserverlesstypes.Workgroup, serverlessNamespaces []redshiftserverlesstypes.Namespace) (redshiftserverlesstypes.Namespace, error) {
	var namespaces []string
	for _, namespace := range serverlessNamespaces {
		if *namespace.NamespaceName == *workgroup.NamespaceName {
			return namespace, nil
		}
		namespaces = append(namespaces, *namespace.NamespaceName)
	}
	return redshiftserverlesstypes.Namespace{}, fmt.Errorf("namespace not found for workgroup workgroup=%s requiredNamespace=%s availableNamespaces=%v", *workgroup.WorkgroupName, *workgroup.NamespaceName, namespaces)
}
